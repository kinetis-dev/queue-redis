<?php

declare(strict_types=1);

/**
 * The crash path against a real Redis: a reservation is a finite lease,
 * an abandoned lease is reclaimed by an ordinary pop() once it expires,
 * and the abandoning worker's handle can no longer settle anything.
 *
 * Only a real server proves this. The lease clock is Redis's own TIME,
 * the expiry sweep is a ZRANGEBYSCORE inside a Lua script, and the
 * conditional replacement that makes one reclaimer the winner depends on
 * Redis executing that script indivisibly — none of which a fake can
 * stand in for. The lease is two seconds so the waits stay short.
 */

require __DIR__ . '/../vendor/autoload.php';

use Kinetis\Queue\Exception\StaleJobHandleException;
use Kinetis\Queue\Job;
use Kinetis\Queue\JobSettlement;
use Kinetis\QueueRedis\RedisQueue;

use function Amp\Redis\createRedisClient;

final readonly class LeaseRecoveryJob implements Job
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

$redis = createRedisClient('redis://' . (getenv('REDIS_HOST') ?: '127.0.0.1') . ':6379');

$queueName = 'lease-recovery';
$pendingKey = "kinetis_queue:{$queueName}:pending";
$leasedKey = "kinetis_queue:{$queueName}:leased";
$delayedKey = "kinetis_queue:{$queueName}:delayed";
$redis->delete($pendingKey, $leasedKey, $delayedKey);

$queue = new RedisQueue($redis, visibilityTimeoutSeconds: 2);

echo "=== RedisQueue: lease recovery ===\n";

$queue->push(new LeaseRecoveryJob('survives-a-crash'), queue: $queueName);
$abandoned = $queue->pop(timeoutSeconds: 5, queues: [$queueName]);

check('a reservation reports attempts=1', $abandoned?->attempts === 1);
check('the reserved job is leased, not left pending', $redis->getList($pendingKey)->getSize() === 0);
check('the lease holds exactly one member', $redis->getSortedSet($leasedKey)->getSize() === 1);

// The crash: the worker dies here, never calling ack(), release() or fail().
check('size() excludes a live lease', $queue->size($queueName) === 0);
check('clear() leaves a live lease intact', $queue->clear($queueName) === 0);
check('the lease survived the clear', $redis->getSortedSet($leasedKey)->getSize() === 1);

sleep(3);

check('size() counts the lease once it expires', $queue->size($queueName) === 1);

$reclaimed = $queue->pop(timeoutSeconds: 5, queues: [$queueName]);

check('the abandoned job is delivered again', $reclaimed?->args['message'] === 'survives-a-crash');
check('the redelivery reports attempts=2', $reclaimed?->attempts === 2);
check('the redelivery is a different handle', $reclaimed?->handle !== $abandoned?->handle);
check('only the redelivery is leased', $redis->getSortedSet($leasedKey)->getSize() === 1);

// The old envelope is no longer a member of the leased set, so every
// settlement the crashed worker could still attempt finds nothing.
foreach ([JobSettlement::Ack, JobSettlement::Release, JobSettlement::Fail] as $operation) {
    $threw = null;

    try {
        match ($operation) {
            JobSettlement::Ack => $queue->ack($abandoned),
            JobSettlement::Release => $queue->release($abandoned),
            JobSettlement::Fail => $queue->fail($abandoned),
        };
    } catch (StaleJobHandleException $e) {
        $threw = $e;
    }

    check("a stale {$operation->value} is rejected", $threw?->operation === $operation);
}

check('the stale settlements changed nothing', $redis->getSortedSet($leasedKey)->getSize() === 1);
check('and enqueued no duplicate', $redis->getList($pendingKey)->getSize() === 0);

$queue->ack($reclaimed);
check('the redelivery settles normally', $redis->getSortedSet($leasedKey)->getSize() === 0);

// An unwritable lease destination must never cost the job. pop() sweeps
// expired leases before it reserves, so a wrong-typed leased key fails
// there first — what this proves is the public operation's outcome: it
// raises, and the only copy of the job is still pending afterward. The
// reserve script's own lease-before-remove ordering is proven by the
// unit suite's source-order assertion.
$brokenQueue = 'lease-wrong-type';
$brokenPending = "kinetis_queue:{$brokenQueue}:pending";
$brokenLeased = "kinetis_queue:{$brokenQueue}:leased";
$redis->delete($brokenPending, $brokenLeased);

$queue->push(new LeaseRecoveryJob('must-not-be-lost'), queue: $brokenQueue);
$redis->set($brokenLeased, 'not a sorted set');

$failed = false;

try {
    $queue->pop(timeoutSeconds: 1, queues: [$brokenQueue]);
} catch (Throwable) {
    $failed = true;
}

check('a wrong-typed lease destination fails pop()', $failed);
check('and the only copy of the job is still pending', $redis->getList($brokenPending)->getSize() === 1);

$redis->delete($brokenPending, $brokenLeased);

echo "\nALL CHECKS PASSED\n";
