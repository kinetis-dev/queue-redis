<?php

declare(strict_types=1);

/**
 * Real-backend regression coverage for RedisQueue — it has no committed
 * PHPUnit test, by design: a mocked "was this method called with X" test
 * can't prove backend-specific correctness (the reliable-queue
 * ack/release mechanics, priority-queue cycling). This runs the same
 * checks originally verified by hand, on every CI push instead of once.
 */

require __DIR__ . '/../vendor/autoload.php';

use Kinetis\Queue\Exception\InvalidPopTimeoutException;
use Kinetis\Queue\Exception\InvalidQueueNameException;
use Kinetis\Queue\Exception\MalformedJobSettledException;
use Kinetis\Queue\Job;
use Kinetis\Queue\QueuedJob;
use Kinetis\Queue\QueueInterface;
use Kinetis\QueueRedis\RedisQueue;
use Amp\Redis\RedisClient;

use function Amp\Redis\createRedisClient;

function check(string $label, bool $condition): void
{
    echo ($condition ? "OK   " : "FAIL ") . $label . "\n";

    if (!$condition) {
        exit(1);
    }
}

final readonly class IntegrationTestJob implements Job
{
    public function __construct(
        public string $message,
    ) {}

    public function handle(): void
    {
    }
}

function runQueueChecks(string $backend, QueueInterface $queue): void
{
    echo "=== {$backend} ===\n";

    $queue->push(new IntegrationTestJob('hello'));
    $popped = $queue->pop(timeoutSeconds: 5);
    check("{$backend}: pop() returns the pushed job", $popped instanceof QueuedJob);
    check("{$backend}: job data round-trips correctly", $popped?->args['message'] === 'hello');
    check("{$backend}: attempts is 1 on first pop", $popped?->attempts === 1);

    $queue->ack($popped);
    check("{$backend}: nothing left after ack()", $queue->pop(timeoutSeconds: 1) === null);

    // release() increments attempts and makes the job available again.
    $queue->push(new IntegrationTestJob('retry-me'), maxAttempts: 3);
    $first = $queue->pop(timeoutSeconds: 5);
    $queue->release($first);
    $second = $queue->pop(timeoutSeconds: 5);
    check("{$backend}: released job comes back with attempts incremented", $second?->attempts === 2);
    $queue->ack($second);

    // fail() removes the job permanently.
    $queue->push(new IntegrationTestJob('doomed'));
    $doomed = $queue->pop(timeoutSeconds: 5);
    $queue->fail($doomed);
    check("{$backend}: nothing left after fail()", $queue->pop(timeoutSeconds: 1) === null);

    // Priority queues: a higher-priority queue is checked before the default one.
    $queue->push(new IntegrationTestJob('low-priority'), queue: 'default');
    $queue->push(new IntegrationTestJob('high-priority'), queue: 'high');

    $priorityPop = $queue->pop(timeoutSeconds: 5, queues: ['high', 'default']);
    check("{$backend}: the high-priority queue is checked first", $priorityPop?->args['message'] === 'high-priority');
    $queue->ack($priorityPop);

    $remaining = $queue->pop(timeoutSeconds: 5, queues: ['high', 'default']);
    check("{$backend}: falls through to the default queue next", $remaining?->args['message'] === 'low-priority');
    $queue->ack($remaining);

    echo "\n";
}

/**
 * The real fix under KINETIS-18: an empty higher-priority queue must
 * never delay finding a job already waiting in a lower-priority one —
 * the old per-queue BRPOPLPUSH loop cost a full
 * PER_QUEUE_POLL_TIMEOUT_SECONDS (1 real second) per empty queue checked
 * before it moved on, so three empty queues ahead of a ready one meant a
 * multi-second wait even though the job was there the whole time.
 * pop()'s own immediate, non-blocking sweep (backed by probeNonBlocking()'s
 * atomic Lua RPOP+LPUSH, never amphp/redis's buggy non-nullable
 * popTailPushHead() wrapper) is what closes that — verified here by
 * timing a real pop() across three genuinely empty higher-priority
 * queues, on a real Redis, not asserted from the algorithm alone.
 */
function runPrioritySweepTimingCheck(QueueInterface $queue): void
{
    echo "=== RedisQueue: immediate priority sweep ===\n";

    $queue->push(new IntegrationTestJob('found-immediately'), queue: 'lowest');

    $start = microtime(true);
    $found = $queue->pop(timeoutSeconds: 10, queues: ['empty-one', 'empty-two', 'empty-three', 'lowest']);
    $elapsed = microtime(true) - $start;

    check(
        'RedisQueue: a job in the last of four queues, the first three genuinely empty, is still found',
        $found?->args['message'] === 'found-immediately',
    );
    check(
        "RedisQueue: found well under 1 real second — took {$elapsed}s",
        $elapsed < 1.0,
    );

    $queue->ack($found);
    echo "\n";
}

/**
 * KINETIS-63: a message that's already been reserved (moved from pending
 * to processing) but turns out to be malformed once decoded must not
 * strand the poison payload in processing forever, or crash the worker.
 * Written directly onto the pending list with a real, raw RPUSH — not
 * through push(), which would never accept malformed data in the first
 * place — the same "bypass the public API to simulate a corrupted
 * payload" technique a real hand-edited Redis value or non-Kinetis
 * publisher would produce. Verified against the real backend, not
 * mocked: settleIfMalformed()'s own coordination logic is already unit
 * tested (see QueueContractTest), but only a real Redis round trip can
 * prove the exact-payload LREM this backend's settle callback issues
 * genuinely finds and removes the entry, leaving the processing list
 * truly empty rather than merely appearing to under a fake.
 */
function runMalformedMessageChecks(RedisClient $redis, RedisQueue $queue): void
{
    echo "=== RedisQueue: malformed message settlement ===\n";

    $queueName = 'malformed-test';
    $pendingKey = "kinetis_queue:{$queueName}:pending";
    $processingKey = "kinetis_queue:{$queueName}:processing";

    $malformedPayload = json_encode(['class' => 'Some\\Job', 'args' => 'not an array', 'attempts' => 0, 'maxAttempts' => null]);
    $redis->getList($pendingKey)->pushHead($malformedPayload);

    $threw = null;

    try {
        $queue->pop(timeoutSeconds: 1, queues: [$queueName]);
    } catch (MalformedJobSettledException $e) {
        $threw = $e;
    }

    check('RedisQueue: pop() throws MalformedJobSettledException for a malformed reserved message', $threw !== null);
    check('RedisQueue: the settled exception names the right queue', $threw?->queue === $queueName);
    check('RedisQueue: nothing left in processing — the poison payload was genuinely removed, not stranded', $redis->getList($processingKey)->getSize() === 0);
    check('RedisQueue: nothing left in pending either', $redis->getList($pendingKey)->getSize() === 0);

    // The loop must genuinely continue: a real, well-formed job pushed to
    // the same queue right after is still poppable normally.
    $queue->push(new IntegrationTestJob('still works after a malformed message'), queue: $queueName);
    $recovered = $queue->pop(timeoutSeconds: 5, queues: [$queueName]);
    check('RedisQueue: a real job on the same queue is still popped correctly afterward', $recovered?->args['message'] === 'still works after a malformed message');
    $queue->ack($recovered);

    echo "\n";
}

function runInputValidationChecks(QueueInterface $queue): void
{
    echo "=== RedisQueue: input validation ===\n";

    try {
        $queue->pop(timeoutSeconds: -1);
        check('RedisQueue: a negative timeout is rejected', false);
    } catch (InvalidPopTimeoutException) {
        check('RedisQueue: a negative timeout is rejected', true);
    }

    try {
        $queue->pop(queues: ['default', '']);
        check('RedisQueue: an empty queue name is rejected', false);
    } catch (InvalidQueueNameException) {
        check('RedisQueue: an empty queue name is rejected', true);
    }

    try {
        $queue->pop(queues: ['default', 'high', 'default']);
        check('RedisQueue: a duplicate queue name is rejected', false);
    } catch (InvalidQueueNameException) {
        check('RedisQueue: a duplicate queue name is rejected', true);
    }

    check('RedisQueue: an empty queue list returns null, not an error', $queue->pop(timeoutSeconds: 1, queues: []) === null);

    echo "\n";
}

$redis = createRedisClient('redis://' . (getenv('REDIS_HOST') ?: '127.0.0.1') . ':6379');
$redisQueue = new RedisQueue($redis);
runQueueChecks('RedisQueue', $redisQueue);
runPrioritySweepTimingCheck($redisQueue);
runMalformedMessageChecks($redis, $redisQueue);
runInputValidationChecks($redisQueue);

echo "ALL CHECKS PASSED\n";
