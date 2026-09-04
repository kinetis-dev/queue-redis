<?php

declare(strict_types=1);

/**
 * Permanent regression coverage for the crash-loses-a-job bug in
 * release() and promoteDelayedJobs(): both used to be a remove-from-source
 * command followed by a separate push-to-destination command, so a
 * process crash between the two left the job in neither list. Both are
 * now a single Lua script (RedisQueue's own eval() calls), which Redis
 * always executes as one indivisible unit — there is no "between" left
 * for a crash to land in.
 *
 * Proven two ways for each transition, both against a real Redis, not a
 * mock:
 *
 * 1. A deliberate reproduction of the OLD two-command shape (raw ZREM/
 *    LREM then a real sleep() then a raw LPUSH, bypassing RedisQueue
 *    entirely) with a concurrent observer polling throughout — this
 *    confirms the observer genuinely can see a job vanish from both
 *    lists when a real gap exists, so the second half isn't trusted
 *    blindly.
 * 2. The same observer polling while the real, current release()/
 *    promoteDelayedJobs() run repeatedly — across every run, the job
 *    must never once be observed absent from every list at once.
 *
 * The observer and the transition under test run as two genuinely
 * interleaved Fibers via Kinetis\Async\concurrently() on one shared Redis
 * connection — each poll is itself a real network round trip that
 * suspends its Fiber, which is what gives the scheduler a real chance to
 * run the transition's own Fiber in between polls, the same "against a
 * real backend, not a synchronized mock" discipline this package already
 * uses for redis_delayed_race.php.
 */

require __DIR__ . '/../vendor/autoload.php';

use Amp\Redis\RedisClient;
use Kinetis\Async\Timer;
use Kinetis\Queue\Exception\StaleJobHandleException;
use Kinetis\Queue\Job;
use Kinetis\QueueRedis\RedisQueue;

use function Amp\Redis\createRedisClient;
use function Kinetis\Async\concurrently;

final readonly class AtomicTransitionJob implements Job
{
    public function __construct(
        public string $message,
    ) {}

    public function handle(): void
    {
    }
}

function check(string $label, bool $condition): void
{
    echo ($condition ? "OK   " : "FAIL ") . $label . "\n";

    if (!$condition) {
        exit(1);
    }
}

$redisHost = getenv('REDIS_HOST') ?: '127.0.0.1';
$redis = createRedisClient("redis://{$redisHost}:6379");

/**
 * Polls $sourceKey (a list) and $destinationKey (a list) for $payload
 * until $done is flipped, flagging $sawGap the moment $payload is absent
 * from both at once — the exact state a crash mid-transition would leave
 * behind under the old two-command design.
 *
 * $done must be a genuine by-reference capture from the caller (a plain
 * `function () use (&$done)` wrapper, never an arrow function — an `fn`
 * arrow function auto-captures by value at creation time, which would
 * freeze this loop's own view of $done at `false` forever regardless of
 * what the other concurrent task does to the real variable).
 */
function observeListToList(
    RedisClient $redis,
    string $sourceKey,
    string $destinationKey,
    string $payload,
    bool &$done,
): bool {
    $sawGap = false;

    while (!$done) {
        $inSource = in_array($payload, $redis->getList($sourceKey)->getRange(), true);
        $inDestination = in_array($payload, $redis->getList($destinationKey)->getRange(), true);

        if (!$inSource && !$inDestination) {
            $sawGap = true;
        }
    }

    return $sawGap;
}

/**
 * @param list<string> $entries
 */
function listContainsJobId(array $entries, string $jobId): bool
{
    foreach ($entries as $entry) {
        $decoded = json_decode($entry, true);

        if (is_array($decoded) && ($decoded['id'] ?? null) === $jobId) {
            return true;
        }
    }

    return false;
}

/**
 * Same shape as observeListToList(), but matches by the envelope's own
 * stable `id` field instead of exact payload equality — release()
 * deliberately re-encodes the payload it writes to $destinationKey (a
 * fresh `attempts` count), preserving only `id`/`pushedAt` across the
 * transition (see RedisQueue's own docblock), so the exact string this
 * observer started with is never present in $destinationKey even on a
 * fully successful, correctly atomic release() — only decoding each
 * entry and matching its `id` can tell "replaced" apart from "lost".
 * Same by-reference-capture requirement on $done as observeListToList().
 */
function observeListToListById(
    RedisClient $redis,
    string $sourceKey,
    string $destinationKey,
    string $jobId,
    bool &$done,
): bool {
    $sawGap = false;

    while (!$done) {
        $inSource = listContainsJobId($redis->getList($sourceKey)->getRange(), $jobId);
        $inDestination = listContainsJobId($redis->getList($destinationKey)->getRange(), $jobId);

        if (!$inSource && !$inDestination) {
            $sawGap = true;
        }
    }

    return $sawGap;
}

/**
 * Same shape as observeListToList(), but the source is a sorted set
 * (delayed) rather than a list — checked via getRank(), which returns
 * null for an absent member. $destinationKeys is a list, not a single
 * key: driving promotion through the real pop() (as the Part 2 checks
 * below do) also runs pop()'s own separate, already-atomic
 * popTailPushHeadBlocking() immediately afterward, which can move the
 * job from pending into processing before this observer's next poll —
 * a real, expected state, not a gap, so "found" here means present in
 * pending *or* processing, not pending alone. Same by-reference-capture
 * requirement on $done as observeListToList() above.
 *
 * @param list<string> $destinationKeys
 */
function observeSortedSetToList(
    RedisClient $redis,
    string $sourceKey,
    array $destinationKeys,
    string $payload,
    bool &$done,
): bool {
    $sawGap = false;

    while (!$done) {
        $inSource = $redis->getSortedSet($sourceKey)->getRank($payload) !== null;
        $inDestination = false;

        foreach ($destinationKeys as $destinationKey) {
            if (in_array($payload, $redis->getList($destinationKey)->getRange(), true)) {
                $inDestination = true;

                break;
            }
        }

        if (!$inSource && !$inDestination) {
            $sawGap = true;
        }
    }

    return $sawGap;
}

// --- release(): processing -> pending. ---

$processingKey = 'kinetis_queue:atomic-release:processing';
$pendingKey = 'kinetis_queue:atomic-release:pending';

// Part 1: reproduce the old two-command shape directly, with a real
// artificial gap, and confirm the observer catches it.
$redis->delete($processingKey, $pendingKey);
$redis->getList($processingKey)->pushHead('deliberately-broken-payload');

$done = false;
[$sawGap] = concurrently([
    function () use ($redis, $processingKey, $pendingKey, &$done): bool {
        return observeListToList($redis, $processingKey, $pendingKey, 'deliberately-broken-payload', $done);
    },
    function () use ($redis, $processingKey, $pendingKey, &$done): void {
        $redis->getList($processingKey)->remove('deliberately-broken-payload', 1);
        // Timer::delay(), not usleep(): a raw usleep() blocks the whole
        // single-threaded process, including the observer Fiber — nothing
        // could poll during it regardless of whether a real gap exists.
        // Timer::delay() suspends only this Fiber via Revolt, letting the
        // observer's own Fiber keep running exactly as it would during a
        // real network round trip.
        Timer::delay(0.05); // The exact window the old two-command code left open.
        $redis->getList($pendingKey)->pushHead('deliberately-broken-payload');
        $done = true;
    },
]);
check('the observer catches a real gap in a deliberately reproduced two-command sequence', $sawGap === true);

// Part 2: the real, current release() — repeated, since any single call
// only has to be unlucky once to prove the fix wrong.
$redis->delete($processingKey, $pendingKey);
$queue = new RedisQueue($redis);

for ($i = 0; $i < 20; $i++) {
    $queue->clear('atomic-release');
    $redis->delete($processingKey, $pendingKey);
    $queue->push(new AtomicTransitionJob("release-{$i}"), queue: 'atomic-release');
    $job = $queue->pop(timeoutSeconds: 5, queues: ['atomic-release']);
    check("release round {$i}: job popped", $job !== null);

    /** @var string $handle */
    $handle = $job->handle;

    // release() re-encodes the payload it writes to pending (a fresh
    // attempts count), preserving only id/pushedAt — matching on the
    // exact old $handle string in $pendingKey would never find anything
    // even on a fully correct, atomic release(), which is exactly why
    // this observes by the envelope's own stable id instead (see
    // observeListToListById()'s own docblock).
    /** @var array{id: string} $decodedHandle */
    $decodedHandle = json_decode($handle, true, flags: JSON_THROW_ON_ERROR);
    $jobId = $decodedHandle['id'];

    $done = false;
    [$sawGap] = concurrently([
        function () use ($redis, $processingKey, $pendingKey, $jobId, &$done): bool {
            return observeListToListById($redis, $processingKey, $pendingKey, $jobId, $done);
        },
        function () use ($queue, $job, &$done): void {
            $queue->release($job);
            $done = true;
        },
    ]);
    check("release round {$i}: the job is never observed absent from both lists", $sawGap === false);
}

// --- release(): a second call with the same (now-stale) handle must
// throw rather than writing a second replacement onto pending — the
// deterministic KIN-21 regression LREM == 0 case; the loop above only
// ever exercises the LREM == 1 (first, successful) path. ---

$redis->delete($processingKey, $pendingKey);
$queue->push(new AtomicTransitionJob('double-release'), queue: 'atomic-release');
$job = $queue->pop(timeoutSeconds: 5, queues: ['atomic-release']);
check('double-release: job popped', $job !== null);

$queue->release($job);
check(
    'double-release: exactly one pending entry after the first, successful release()',
    count($redis->getList($pendingKey)->getRange()) === 1,
);

$threwStaleJobHandleException = false;

try {
    $queue->release($job);
} catch (StaleJobHandleException) {
    $threwStaleJobHandleException = true;
}

check('double-release: the second release() with the same stale handle throws', $threwStaleJobHandleException);
check(
    'double-release: still exactly one pending entry — the stale second call wrote no replacement',
    count($redis->getList($pendingKey)->getRange()) === 1,
);

// --- promoteDelayedJobs(): delayed -> pending. ---

$delayedKey = 'kinetis_queue:atomic-promote:delayed';
$promotePendingKey = 'kinetis_queue:atomic-promote:pending';
$promoteProcessingKey = 'kinetis_queue:atomic-promote:processing';

// Part 1: same deliberate reproduction, this time source is a sorted set.
$redis->delete($delayedKey, $promotePendingKey, $promoteProcessingKey);
$redis->getSortedSet($delayedKey)->add(['deliberately-broken-delayed-payload' => 0.0]);

$done = false;
[$sawGap] = concurrently([
    function () use ($redis, $delayedKey, $promotePendingKey, &$done): bool {
        return observeSortedSetToList($redis, $delayedKey, [$promotePendingKey], 'deliberately-broken-delayed-payload', $done);
    },
    function () use ($redis, $delayedKey, $promotePendingKey, &$done): void {
        $redis->getSortedSet($delayedKey)->remove('deliberately-broken-delayed-payload');
        Timer::delay(0.05);
        $redis->getList($promotePendingKey)->pushHead('deliberately-broken-delayed-payload');
        $done = true;
    },
]);
check('the observer catches a real gap in a deliberately reproduced delayed promotion', $sawGap === true);

// Part 2: the real, current promoteDelayedJobs(), reached through pop().
// The trigger is pop(), which — once promotion moves the job into
// pending — immediately pops it again into processing via its own,
// separately-atomic popTailPushHeadBlocking() call, so "found" here
// means pending *or* processing (see observeSortedSetToList()'s own
// docblock); only genuinely absent from all three counts as a gap.
$redis->delete($delayedKey, $promotePendingKey, $promoteProcessingKey);

for ($i = 0; $i < 20; $i++) {
    $queue->clear('atomic-promote');
    $redis->delete($delayedKey, $promotePendingKey, $promoteProcessingKey);
    // A real delay — delaySeconds: 0 goes straight to pending, never
    // touching the delayed sorted set at all.
    $queue->push(new AtomicTransitionJob("promote-{$i}"), delaySeconds: 60, queue: 'atomic-promote');

    // The payload is whatever encode() produced for this push — read it
    // back off the sorted set directly, since push() doesn't return it.
    $members = $redis->getSortedSet($delayedKey)->getRange(0, -1);
    check("promote round {$i}: exactly one delayed member present", count($members) === 1);
    $payload = $members[0];

    // Re-score to a past timestamp instead of sleeping out the real
    // 60-second delay — ZADD on an existing member updates its score.
    $redis->getSortedSet($delayedKey)->add([$payload => 0.0]);

    // ack() deliberately does not run inside the racing action below: it
    // would remove the job from processing too, and if that completed
    // before the observer's own in-flight iteration finished its last
    // read, the observer would see a real, but unrelated, disappearance
    // (successfully-acknowledged, not lost) and misreport it as a gap.
    // ack() runs once both tasks below have already finished instead.
    $poppedJob = null;
    $done = false;
    [$sawGap] = concurrently([
        function () use ($redis, $delayedKey, $promotePendingKey, $promoteProcessingKey, $payload, &$done): bool {
            return observeSortedSetToList($redis, $delayedKey, [$promotePendingKey, $promoteProcessingKey], $payload, $done);
        },
        function () use ($queue, &$done, &$poppedJob): void {
            // pop() calls promoteDelayedJobs() internally before reading
            // pending — the transition under test runs as a side effect.
            $poppedJob = $queue->pop(timeoutSeconds: 5, queues: ['atomic-promote']);
            $done = true;
        },
    ]);
    check("promote round {$i}: the job is never observed absent from both structures", $sawGap === false);
    check("promote round {$i}: pop() actually returned the job", $poppedJob !== null);

    if ($poppedJob !== null) {
        $queue->ack($poppedJob);
    }
}

echo "ALL CHECKS PASSED\n";
