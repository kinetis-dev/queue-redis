<?php

declare(strict_types=1);

/**
 * Real-backend coverage for RedisQueue's ordinary delivery cycle: push,
 * reserve, settle, priority order, and malformed-message settlement.
 * Only a real server proves these — the unit suite scripts the wire, but
 * whether Redis actually removed the member a settlement named is a
 * question the server has to answer. Lease expiry and crash recovery
 * live in redis_lease_recovery.php.
 */

require __DIR__ . '/../vendor/autoload.php';

use Kinetis\Queue\Exception\InvalidQueueArgumentException;
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
 * An empty higher-priority queue must never delay finding a job already
 * waiting in a lower-priority one: pop() sweeps every named queue before
 * it waits at all. Timed here against a real Redis across three empty
 * higher-priority queues rather than asserted from the algorithm alone.
 */
function runPrioritySweepTimingCheck(QueueInterface $queue): void
{
    echo "=== RedisQueue: immediate priority sweep ===\n";

    $queue->push(new IntegrationTestJob('found-immediately'), queue: 'lowest');

    $start = microtime(true);
    $found = $queue->pop(timeoutSeconds: 10, queues: ['empty-one', 'empty-two', 'empty-three', 'lowest']);
    $elapsed = microtime(true) - $start;

    check(
        'RedisQueue: a job in the last of four queues, the first three empty, is still found',
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
 * A message that has already been reserved but turns out to be malformed
 * once decoded must not strand the poison payload under its lease, or
 * crash the worker. Written directly onto the pending list with a real,
 * raw push — not through push(), which would never accept malformed data
 * — the same corruption a hand-edited Redis value or a non-Kinetis
 * publisher would produce. Only a real Redis round trip proves the
 * exact-member removal this backend's settle callback issues genuinely
 * empties the leased set rather than merely appearing to under a fake.
 *
 * Every case below is a complete, current envelope with exactly one
 * field corrupted, so what it proves is that field's own rule and not
 * some earlier check firing first. The identity and required-key cases
 * — a missing `metadata`, an `id` outside the 32-lowercase-hex shape
 * push() writes, a `pushedAt` stored as a numeric string or as a
 * non-positive timestamp — are the ones a looser decoder would accept
 * as a job, which is why each is walked all the way through a real
 * reservation to a real settlement here rather than only at the unit
 * level.
 */
function runMalformedMessageChecks(RedisClient $redis, RedisQueue $queue): void
{
    echo "=== RedisQueue: malformed message settlement ===\n";

    $queueName = 'malformed-test';
    $pendingKey = "kinetis_queue:{$queueName}:pending";
    $leasedKey = "kinetis_queue:{$queueName}:leased";

    $envelope = static function (array $overrides = [], array $missing = []): string {
        $envelope = [
            'id' => bin2hex(random_bytes(16)),
            'pushedAt' => time(),
            'class' => 'Some\\Job',
            'args' => [],
            'attempts' => 0,
            'maxAttempts' => null,
            'metadata' => [],
            ...$overrides,
        ];

        foreach ($missing as $field) {
            unset($envelope[$field]);
        }

        return json_encode($envelope, JSON_THROW_ON_ERROR);
    };

    $cases = [
        'an args field that is not an array' => $envelope(['args' => 'not an array']),
        'a missing metadata key' => $envelope(missing: ['metadata']),
        'an id that is not 32 lowercase hex characters' => $envelope(['id' => 'not-a-current-id']),
        'a pushedAt stored as a numeric string' => $envelope(['pushedAt' => (string) time()]),
        'a pushedAt of zero' => $envelope(['pushedAt' => 0]),
    ];

    foreach ($cases as $label => $malformedPayload) {
        $redis->getList($pendingKey)->pushHead($malformedPayload);

        $threw = null;

        try {
            $queue->pop(timeoutSeconds: 1, queues: [$queueName]);
        } catch (MalformedJobSettledException $e) {
            $threw = $e;
        }

        check("RedisQueue: pop() throws MalformedJobSettledException for a reserved message with {$label}", $threw !== null);
        check("RedisQueue: the settled exception names the right queue for {$label}", $threw?->queue === $queueName);
        check("RedisQueue: nothing left leased after {$label} — the poison payload was removed, not stranded", $redis->getSortedSet($leasedKey)->getSize() === 0);
        check("RedisQueue: nothing left in pending either after {$label}", $redis->getList($pendingKey)->getSize() === 0);
    }

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
    } catch (InvalidQueueArgumentException) {
        check('RedisQueue: a negative timeout is rejected', true);
    }

    try {
        $queue->pop(queues: ['default', '']);
        check('RedisQueue: an empty queue name is rejected', false);
    } catch (InvalidQueueArgumentException) {
        check('RedisQueue: an empty queue name is rejected', true);
    }

    try {
        $queue->pop(queues: ['default', 'high', 'default']);
        check('RedisQueue: a duplicate queue name is rejected', false);
    } catch (InvalidQueueArgumentException) {
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
