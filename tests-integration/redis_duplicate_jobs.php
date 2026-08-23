<?php

declare(strict_types=1);

/**
 * Permanent regression coverage for the delayed-job duplicate-collapse
 * bug: RedisQueue's delayed sorted set used the job's own JSON payload as
 * its member, and sorted-set members are unique — pushing the exact same
 * class/args/attempts/maxAttempts twice as delayed jobs produced one
 * member, not two, silently losing the second job outright.
 * RedisQueue::encode() now stamps a fresh, cryptographically random `id`
 * into every envelope, which is what makes two otherwise-identical
 * envelopes different strings. Two duplicate immediate pushes are also
 * checked here, even though a plain Redis list never had a uniqueness
 * constraint to begin with — a permanent guard against a future refactor
 * accidentally routing pending jobs through a set-like structure too.
 */

require __DIR__ . '/../vendor/autoload.php';

use Kinetis\Queue\Job;
use Kinetis\QueueRedis\RedisQueue;

use function Amp\Redis\createRedisClient;

final readonly class DuplicateJob implements Job
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
$queue = new RedisQueue($redis);

// --- Duplicate immediate jobs: never at risk (a list allows duplicate
// values), checked anyway as a permanent guard. ---

$queue->clear('duplicate-immediate');
$queue->push(new DuplicateJob('same-payload'), queue: 'duplicate-immediate');
$queue->push(new DuplicateJob('same-payload'), queue: 'duplicate-immediate');

check('two identical immediate pushes report size 2', $queue->size('duplicate-immediate') === 2);

$first = $queue->pop(timeoutSeconds: 5, queues: ['duplicate-immediate']);
$second = $queue->pop(timeoutSeconds: 5, queues: ['duplicate-immediate']);
check('both identical immediate jobs are independently poppable', $first !== null && $second !== null);
check('nothing left after popping both', $queue->pop(timeoutSeconds: 1, queues: ['duplicate-immediate']) === null);
$queue->ack($first);
$queue->ack($second);

// --- Duplicate delayed jobs: the actual bug. Two byte-identical delayed
// pushes must produce two distinct sorted-set members, not one. ---

$queue->clear('duplicate-delayed');
$queue->push(new DuplicateJob('same-payload'), delaySeconds: 1, queue: 'duplicate-delayed');
$queue->push(new DuplicateJob('same-payload'), delaySeconds: 1, queue: 'duplicate-delayed');

check('two identical delayed pushes report size 2, not 1', $queue->size('duplicate-delayed') === 2);

sleep(2);

$delayedFirst = $queue->pop(timeoutSeconds: 5, queues: ['duplicate-delayed']);
$delayedSecond = $queue->pop(timeoutSeconds: 5, queues: ['duplicate-delayed']);
check('both identical delayed jobs survive the delay independently', $delayedFirst !== null && $delayedSecond !== null);
check('nothing left after popping both delayed jobs', $queue->pop(timeoutSeconds: 1, queues: ['duplicate-delayed']) === null);
$queue->ack($delayedFirst);
$queue->ack($delayedSecond);

echo "ALL CHECKS PASSED\n";
