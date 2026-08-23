<?php

declare(strict_types=1);

/**
 * Real-Redis regression coverage for the KIN-24 fix: promoteDelayedJobs()
 * bounds how many ready delayed jobs it moves in one Lua script call
 * (RedisQueue::DELAYED_PROMOTION_BATCH_SIZE), rather than materializing
 * and moving an unbounded ZRANGEBYSCORE result inside one indivisible
 * script — Redis executes one command at a time, so an unbounded batch
 * would stall every other client sharing this Redis for the full
 * promotion duration under a large backlog.
 *
 * Proven against a real Redis, not by reading the script text: push more
 * delayed jobs than the batch size, all already due, then trigger exactly
 * one promotion pass via a single pop() call (pop() calls
 * promoteDelayedJobs() once per queue per loop iteration, before it reads
 * pending) and confirm the delayed sorted set still holds the un-promoted
 * remainder — proof the whole backlog was not moved in that one call.
 * pop()'s own outer while(true) loop already repeats the promotion on
 * every subsequent iteration, which is what lets the remainder drain over
 * further polls without needing any change to that loop.
 */

require __DIR__ . '/../vendor/autoload.php';

use Kinetis\Queue\Job;
use Kinetis\QueueRedis\RedisQueue;

use function Amp\Redis\createRedisClient;

final readonly class BoundedPromotionJob implements Job
{
    public function __construct(
        public int $index,
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
$queue = new RedisQueue($redis);

// A private constant on RedisQueue — read via reflection rather than
// hardcoding a second copy of the number here, so this test can't
// silently drift from whatever the real batch size actually is.
$batchSize = (new ReflectionClassConstant(RedisQueue::class, 'DELAYED_PROMOTION_BATCH_SIZE'))->getValue();
check('DELAYED_PROMOTION_BATCH_SIZE is a real, positive bound', is_int($batchSize) && $batchSize > 0);

$delayedKey = 'kinetis_queue:bounded-promotion:delayed';
$pendingKey = 'kinetis_queue:bounded-promotion:pending';
$processingKey = 'kinetis_queue:bounded-promotion:processing';

$redis->delete($delayedKey, $pendingKey, $processingKey);

$total = $batchSize + 50;

for ($i = 0; $i < $total; $i++) {
    // Pushed with a real delay, then re-scored to "already due" — the
    // same pattern redis_atomic_transitions.php uses, avoiding an actual
    // wait for the delay to elapse.
    $queue->push(new BoundedPromotionJob($i), delaySeconds: 60, queue: 'bounded-promotion');
}

$members = $redis->getSortedSet($delayedKey)->getRange(0, -1);
check("exactly {$total} delayed members present before promotion", count($members) === $total);

foreach ($members as $member) {
    $redis->getSortedSet($delayedKey)->add([$member => 0.0]);
}

// One pop() call triggers exactly one promotion pass for this queue,
// then pops one job into processing.
$job = $queue->pop(timeoutSeconds: 5, queues: ['bounded-promotion']);
check('pop() returned a job', $job !== null);

$remainingDelayed = $redis->getSortedSet($delayedKey)->getSize();
$expectedRemaining = $total - $batchSize;
check(
    "after one promotion pass, {$expectedRemaining} delayed members remain (not the whole backlog)",
    $remainingDelayed === $expectedRemaining,
);

// One promoted member became the job pop() returned (now in processing);
// the rest of that same batch is sitting in pending, waiting.
$pendingCount = $redis->getList($pendingKey)->getSize();
$processingCount = $redis->getList($processingKey)->getSize();
check(
    'the whole first batch is accounted for across pending + processing, none of it lost',
    $pendingCount + $processingCount === $batchSize,
);

// pop()'s own outer loop calls promoteDelayedJobs() again on its next
// iteration — draining the remainder over subsequent polls needs no
// change to that loop, only proven here by actually calling pop() again
// until the backlog is gone. Draining is driven by size() (pending +
// delayed), not the delayed set alone: every later pop() also re-triggers
// promoteDelayedJobs() before popping just one job, so the delayed set
// can empty out well before everything it fed into pending has actually
// been popped — checking delayed alone would stop this loop early and
// leave real, unpopped work sitting in pending.
if ($job !== null) {
    $queue->ack($job);
}

$drained = 1;

while ($queue->size('bounded-promotion') > 0) {
    $next = $queue->pop(timeoutSeconds: 5, queues: ['bounded-promotion']);

    if ($next === null) {
        check('a subsequent pop() keeps draining the remainder', false);

        break;
    }

    $queue->ack($next);
    $drained++;
}

check("every one of the {$total} delayed jobs was eventually promoted and popped", $drained === $total);
check('the delayed set is fully drained', $redis->getSortedSet($delayedKey)->getSize() === 0);

echo "ALL CHECKS PASSED\n";
