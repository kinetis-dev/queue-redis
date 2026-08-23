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

use Kinetis\Queue\Job;
use Kinetis\Queue\QueuedJob;
use Kinetis\Queue\QueueInterface;
use Kinetis\QueueRedis\RedisQueue;

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

$redis = createRedisClient('redis://' . (getenv('REDIS_HOST') ?: '127.0.0.1') . ':6379');
runQueueChecks('RedisQueue', new RedisQueue($redis));

echo "ALL CHECKS PASSED\n";
