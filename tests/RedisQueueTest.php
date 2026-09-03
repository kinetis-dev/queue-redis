<?php

declare(strict_types=1);

namespace Kinetis\QueueRedis\Tests;

use Kinetis\Queue\Exception\InvalidDelaySecondsException;
use Kinetis\Queue\Exception\InvalidMaxAttemptsException;
use Kinetis\Queue\Exception\InvalidQueueNameException;
use Kinetis\Queue\Exception\MalformedQueuedJobDataException;
use Kinetis\Queue\JobSerializer;
use Kinetis\QueueRedis\RedisQueue;
use Kinetis\QueueRedis\Tests\Fixtures\Priority;
use Kinetis\QueueRedis\Tests\Fixtures\RichPayloadJob;
use PHPUnit\Framework\TestCase;
use ReflectionMethod;
use function Amp\Redis\createRedisClient;

/**
 * Queue-name validation, plus the pure envelope encode()/decodeQueuedJob()
 * round trip — RedisQueue's own genuinely backend-specific correctness
 * (the reliable pending->processing move, priority cycling, a real
 * BRPOPLPUSH round trip) is still deliberately never unit-tested against
 * a fake, matching this package's established "swap the storage, not the
 * whole system, and don't fake what a real backend has to prove"
 * discipline — real-backend verification lives in tests-integration/
 * instead. Both kinds of check here are pure PHP: the queue-name checks
 * throw before the Redis client is ever touched, and encode()/
 * decodeQueuedJob() (both private, invoked via reflection — pure JSON
 * work with zero I/O either way) never touch a connection at all, so a
 * real server has nothing to prove that a fast unit test can't already
 * prove faster — the same reasoning kinetis/cache-redis's own
 * RedisSimpleCacheTest already applies to its key-validation checks.
 *
 * createRedisClient() never connects eagerly (confirmed by
 * RedisSimpleCache's own docblock and tests) — the underlying socket
 * only opens on the first command actually executed — so a RedisClient
 * pointed at localhost with nothing listening is safe to construct and
 * pass to RedisQueue here with no real server required.
 */
final class RedisQueueTest extends TestCase
{
    private function neverConnectedQueue(): RedisQueue
    {
        return new RedisQueue(createRedisClient('redis://localhost:1'));
    }

    public function test_size_rejects_an_empty_queue_name_before_ever_touching_redis(): void
    {
        $queue = $this->neverConnectedQueue();

        $this->expectException(InvalidQueueNameException::class);
        $queue->size('');
    }

    public function test_size_rejects_a_malformed_queue_name_before_ever_touching_redis(): void
    {
        $queue = $this->neverConnectedQueue();

        $this->expectException(InvalidQueueNameException::class);
        $queue->size('has spaces');
    }

    public function test_clear_rejects_an_empty_queue_name_before_ever_touching_redis(): void
    {
        $queue = $this->neverConnectedQueue();

        $this->expectException(InvalidQueueNameException::class);
        $queue->clear('');
    }

    public function test_push_rejects_a_negative_delay_before_ever_touching_redis(): void
    {
        $queue = $this->neverConnectedQueue();

        $this->expectException(InvalidDelaySecondsException::class);
        $queue->push(new RichPayloadJob(4.0, [], Priority::High), delaySeconds: -1);
    }

    public function test_push_rejects_a_negative_max_attempts_before_ever_touching_redis(): void
    {
        $queue = $this->neverConnectedQueue();

        $this->expectException(InvalidMaxAttemptsException::class);
        $queue->push(new RichPayloadJob(4.0, [], Priority::High), maxAttempts: -1);
    }

    /**
     * decodeQueuedJob()'s own $decoded['attempts']/['maxAttempts'] read is
     * where a corrupted JSON payload's malformed value is actually caught
     * — proven directly with a hand-built payload, not merely at
     * QueueContract::coerceStoredInteger()'s own unit level, so the wiring
     * between the two is exercised too.
     */
    public function test_decode_queued_job_rejects_a_non_numeric_stored_attempts_value(): void
    {
        $payload = json_encode([
            'id' => 'fixed-id',
            'pushedAt' => 1_700_000_000,
            'class' => RichPayloadJob::class,
            'args' => [],
            'attempts' => 'garbage',
            'maxAttempts' => null,
            'metadata' => [],
        ], JSON_THROW_ON_ERROR);

        $queue = $this->neverConnectedQueue();
        $decodeQueuedJob = new ReflectionMethod(RedisQueue::class, 'decodeQueuedJob');

        $this->expectException(MalformedQueuedJobDataException::class);
        $this->expectExceptionMessage('"attempts"');
        $decodeQueuedJob->invoke($queue, 'default', $payload);
    }

    public function test_decode_queued_job_rejects_a_non_numeric_stored_max_attempts_value(): void
    {
        $payload = json_encode([
            'id' => 'fixed-id',
            'pushedAt' => 1_700_000_000,
            'class' => RichPayloadJob::class,
            'args' => [],
            'attempts' => 0,
            'maxAttempts' => 'garbage',
            'metadata' => [],
        ], JSON_THROW_ON_ERROR);

        $queue = $this->neverConnectedQueue();
        $decodeQueuedJob = new ReflectionMethod(RedisQueue::class, 'decodeQueuedJob');

        $this->expectException(MalformedQueuedJobDataException::class);
        $this->expectExceptionMessage('"maxAttempts"');
        $decodeQueuedJob->invoke($queue, 'default', $payload);
    }

    /**
     * The reviewer's own reported overflow gap, at the real decode level:
     * a stored completed-attempts count of exactly PHP_INT_MAX is
     * syntactically a perfectly valid integer — coerceStoredInteger()
     * alone would accept it — but decodeQueuedJob()'s own `+ 1` would
     * silently overflow it to a float, which would then fail QueuedJob's
     * strictly-typed constructor with a confusing TypeError. This proves
     * the real, wired decode path rejects it cleanly instead, via
     * QueueContract::coerceStoredCompletedAttempts().
     */
    public function test_decode_queued_job_rejects_a_stored_attempts_value_of_php_int_max(): void
    {
        $payload = json_encode([
            'id' => 'fixed-id',
            'pushedAt' => 1_700_000_000,
            'class' => RichPayloadJob::class,
            'args' => [],
            'attempts' => PHP_INT_MAX,
            'maxAttempts' => null,
            'metadata' => [],
        ], JSON_THROW_ON_ERROR);

        $queue = $this->neverConnectedQueue();
        $decodeQueuedJob = new ReflectionMethod(RedisQueue::class, 'decodeQueuedJob');

        $this->expectException(MalformedQueuedJobDataException::class);
        $this->expectExceptionMessage('PHP_INT_MAX');
        $decodeQueuedJob->invoke($queue, 'default', $payload);
    }

    public function test_decode_queued_job_rejects_a_payload_that_is_not_valid_json(): void
    {
        $queue = $this->neverConnectedQueue();
        $decodeQueuedJob = new ReflectionMethod(RedisQueue::class, 'decodeQueuedJob');

        $this->expectException(MalformedQueuedJobDataException::class);
        $this->expectExceptionMessage('"payload"');
        $decodeQueuedJob->invoke($queue, 'default', '{not valid json');
    }

    public function test_decode_queued_job_rejects_a_payload_missing_the_class_field(): void
    {
        $payload = json_encode(['args' => [], 'attempts' => 0, 'maxAttempts' => null], JSON_THROW_ON_ERROR);

        $queue = $this->neverConnectedQueue();
        $decodeQueuedJob = new ReflectionMethod(RedisQueue::class, 'decodeQueuedJob');

        $this->expectException(MalformedQueuedJobDataException::class);
        $this->expectExceptionMessage('"class"');
        $decodeQueuedJob->invoke($queue, 'default', $payload);
    }

    public function test_decode_queued_job_rejects_a_payload_whose_args_field_is_not_an_array(): void
    {
        $payload = json_encode(
            ['class' => RichPayloadJob::class, 'args' => 'not an array', 'attempts' => 0, 'maxAttempts' => null],
            JSON_THROW_ON_ERROR,
        );

        $queue = $this->neverConnectedQueue();
        $decodeQueuedJob = new ReflectionMethod(RedisQueue::class, 'decodeQueuedJob');

        $this->expectException(MalformedQueuedJobDataException::class);
        $this->expectExceptionMessage('"args"');
        $decodeQueuedJob->invoke($queue, 'default', $payload);
    }

    /**
     * A JSON *list* args value ("args": ["value"], no object keys) is a
     * real, distinct malformed shape from "not an array at all" above —
     * is_array() alone would have accepted it. Confirming it throws
     * MalformedQueuedJobDataException here, from decodeQueuedJob()
     * itself (the exact function probeNonBlocking()/probeBlocking()
     * wrap in QueueContract::settleIfMalformed()), is what proves this
     * reaches the settle-and-remove path rather than QueueWorker's
     * ordinary job-execution failure handling (which would otherwise
     * release/retry a message that can never succeed, up to
     * maxAttempts, before finally giving up).
     */
    public function test_decode_queued_job_rejects_a_payload_whose_args_field_is_a_json_list(): void
    {
        $payload = json_encode(
            ['class' => RichPayloadJob::class, 'args' => ['positional value'], 'attempts' => 0, 'maxAttempts' => null],
            JSON_THROW_ON_ERROR,
        );

        $queue = $this->neverConnectedQueue();
        $decodeQueuedJob = new ReflectionMethod(RedisQueue::class, 'decodeQueuedJob');

        $this->expectException(MalformedQueuedJobDataException::class);
        $this->expectExceptionMessage('"args"');
        $decodeQueuedJob->invoke($queue, 'default', $payload);
    }

    public function test_decode_queued_job_rejects_a_payload_whose_metadata_field_has_a_non_string_value(): void
    {
        $payload = json_encode(
            ['class' => RichPayloadJob::class, 'args' => [], 'attempts' => 0, 'maxAttempts' => null, 'metadata' => ['trace_id' => 5]],
            JSON_THROW_ON_ERROR,
        );

        $queue = $this->neverConnectedQueue();
        $decodeQueuedJob = new ReflectionMethod(RedisQueue::class, 'decodeQueuedJob');

        $this->expectException(MalformedQueuedJobDataException::class);
        $this->expectExceptionMessage('"metadata"');
        $decodeQueuedJob->invoke($queue, 'default', $payload);
    }

    /**
     * encode() always writes the `maxAttempts` key, even when its own
     * value is null (see that method's own docblock) — so a payload
     * missing the key entirely, not merely one with an explicit null
     * value, is a sign of a truncated or otherwise corrupted envelope.
     * A plain `?? null` read could never tell the two apart; this proves
     * the real, wired decode path does.
     */
    public function test_decode_queued_job_rejects_a_payload_missing_the_max_attempts_key_entirely(): void
    {
        $payload = json_encode(
            ['class' => RichPayloadJob::class, 'args' => [], 'attempts' => 0],
            JSON_THROW_ON_ERROR,
        );

        $queue = $this->neverConnectedQueue();
        $decodeQueuedJob = new ReflectionMethod(RedisQueue::class, 'decodeQueuedJob');

        $this->expectException(MalformedQueuedJobDataException::class);
        $this->expectExceptionMessage('"maxAttempts"');
        $this->expectExceptionMessage('missing entirely');
        $decodeQueuedJob->invoke($queue, 'default', $payload);
    }

    /**
     * The real mechanism every push()/pop() ultimately relies on: a
     * JobSerializer::serialize()-normalized payload — a float, a nested
     * list of maps, a BackedEnum tag — survives encode() (real
     * json_encode with JSON_PRESERVE_ZERO_FRACTION) followed by
     * decodeQueuedJob() (real json_decode) with every value's exact type
     * intact, the float included. Both are private, invoked via
     * reflection — PHPUnit's `?ReflectionMethod::invoke()`-based calls
     * bypass visibility without needing setAccessible(), and neither
     * method does any I/O for this to be unsafe against.
     */
    public function test_encode_then_decode_preserves_every_value_including_float_type(): void
    {
        $serialized = JobSerializer::serialize(new RichPayloadJob(
            4.0,
            [['id' => 1, 'tags' => ['a', 'b']]],
            Priority::High,
        ));

        $encode = new ReflectionMethod(RedisQueue::class, 'encode');
        $payload = $encode->invoke(null, $serialized, 0, null, [], 'fixed-id', 1_700_000_000);

        self::assertIsString($payload);

        $queue = $this->neverConnectedQueue();
        $decodeQueuedJob = new ReflectionMethod(RedisQueue::class, 'decodeQueuedJob');
        $queuedJob = $decodeQueuedJob->invoke($queue, 'default', $payload);

        self::assertSame($serialized['class'], $queuedJob->class);
        self::assertSame($serialized['args'], $queuedJob->args);
        self::assertIsFloat($queuedJob->args['ratio']);
        self::assertSame(4.0, $queuedJob->args['ratio']);
    }
}
