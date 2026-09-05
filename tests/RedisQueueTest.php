<?php

declare(strict_types=1);

namespace Kinetis\QueueRedis\Tests;

use Amp\Redis\RedisClient;
use Kinetis\Queue\ClearableQueueInterface;
use Kinetis\Queue\Exception\InvalidDelaySecondsException;
use Kinetis\Queue\Exception\InvalidMaxAttemptsException;
use Kinetis\Queue\Exception\InvalidQueueNameException;
use Kinetis\Queue\Exception\MalformedQueuedJobDataException;
use Kinetis\Queue\Exception\StaleJobHandleException;
use Kinetis\Queue\JobSerializer;
use Kinetis\Queue\JobSettlement;
use Kinetis\Queue\QueuedJob;
use Kinetis\QueueRedis\RedisQueue;
use Kinetis\QueueRedis\Tests\Fixtures\Priority;
use Kinetis\QueueRedis\Tests\Fixtures\RichPayloadJob;
use Kinetis\QueueRedis\Tests\Fixtures\ScriptedRedisLink;
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
 *
 * ack()/fail()/clear() are the exception to "never unit-tested against a
 * fake", and the reason is what they test: not that Redis removes an
 * entry — only a real server proves that — but which command this class
 * issues, against which keys, and what it makes of the reply. That is
 * pure request-building and branching over one integer, and
 * Amp\Redis\Connection\RedisLink is the seam that supplies both (see
 * {@see ScriptedRedisLink}).
 */
final class RedisQueueTest extends TestCase
{
    /**
     * One value of the exact shape push() writes for `id` — 32 lowercase
     * hexadecimal characters — held fixed so a test that corrupts some
     * other field isn't rejected for its identity first.
     */
    private const string VALID_ID = '0123456789abcdef0123456789abcdef';

    private function neverConnectedQueue(): RedisQueue
    {
        return new RedisQueue(createRedisClient('redis://localhost:1'));
    }

    /**
     * A complete, current envelope — the seven keys encode() writes, with
     * the values it writes them as — for a test that then corrupts one
     * field. $overrides replaces a field's value; $missing drops a key
     * outright, the one corruption a value override can't express.
     *
     * @param array<string, mixed> $overrides
     * @param list<string> $missing
     * @return array<string, mixed>
     */
    private static function envelope(array $overrides = [], array $missing = []): array
    {
        $envelope = [
            'id' => self::VALID_ID,
            'pushedAt' => 1_700_000_000,
            'class' => RichPayloadJob::class,
            'args' => [],
            'attempts' => 0,
            'maxAttempts' => null,
            'metadata' => [],
            ...$overrides,
        ];

        foreach ($missing as $field) {
            unset($envelope[$field]);
        }

        return $envelope;
    }

    /**
     * Drives decodeQueuedJob() over a whole set of rejected values for
     * one field in a single test — expectException() can only assert one
     * throw per test, and a rule like "32 lowercase hex characters" is
     * only pinned down by the near-misses it turns away, not by any one
     * of them.
     *
     * @param array<string, mixed> $envelope
     */
    private function assertRejectedAsMalformed(array $envelope, string $expectedFragment): void
    {
        $decodeQueuedJob = new ReflectionMethod(RedisQueue::class, 'decodeQueuedJob');

        try {
            $decodeQueuedJob->invoke(
                $this->neverConnectedQueue(),
                'default',
                json_encode($envelope, JSON_THROW_ON_ERROR),
            );
        } catch (MalformedQueuedJobDataException $e) {
            self::assertStringContainsString($expectedFragment, $e->getMessage());

            return;
        }

        self::fail("Expected a malformed-data rejection naming {$expectedFragment}.");
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
        $payload = json_encode(self::envelope(['attempts' => 'garbage']), JSON_THROW_ON_ERROR);

        $queue = $this->neverConnectedQueue();
        $decodeQueuedJob = new ReflectionMethod(RedisQueue::class, 'decodeQueuedJob');

        $this->expectException(MalformedQueuedJobDataException::class);
        $this->expectExceptionMessage('"attempts"');
        $decodeQueuedJob->invoke($queue, 'default', $payload);
    }

    public function test_decode_queued_job_rejects_a_non_numeric_stored_max_attempts_value(): void
    {
        $payload = json_encode(self::envelope(['maxAttempts' => 'garbage']), JSON_THROW_ON_ERROR);

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
        $payload = json_encode(self::envelope(['attempts' => PHP_INT_MAX]), JSON_THROW_ON_ERROR);

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
        $payload = json_encode(self::envelope(missing: ['class']), JSON_THROW_ON_ERROR);

        $queue = $this->neverConnectedQueue();
        $decodeQueuedJob = new ReflectionMethod(RedisQueue::class, 'decodeQueuedJob');

        $this->expectException(MalformedQueuedJobDataException::class);
        $this->expectExceptionMessage('"class"');
        $decodeQueuedJob->invoke($queue, 'default', $payload);
    }

    public function test_decode_queued_job_rejects_a_payload_whose_args_field_is_not_an_array(): void
    {
        $payload = json_encode(self::envelope(['args' => 'not an array']), JSON_THROW_ON_ERROR);

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
        $payload = json_encode(self::envelope(['args' => ['positional value']]), JSON_THROW_ON_ERROR);

        $queue = $this->neverConnectedQueue();
        $decodeQueuedJob = new ReflectionMethod(RedisQueue::class, 'decodeQueuedJob');

        $this->expectException(MalformedQueuedJobDataException::class);
        $this->expectExceptionMessage('"args"');
        $decodeQueuedJob->invoke($queue, 'default', $payload);
    }

    public function test_decode_queued_job_rejects_a_payload_whose_metadata_field_has_a_non_string_value(): void
    {
        $payload = json_encode(self::envelope(['metadata' => ['trace_id' => 5]]), JSON_THROW_ON_ERROR);

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
        $payload = json_encode(self::envelope(missing: ['maxAttempts']), JSON_THROW_ON_ERROR);

        $queue = $this->neverConnectedQueue();
        $decodeQueuedJob = new ReflectionMethod(RedisQueue::class, 'decodeQueuedJob');

        $this->expectException(MalformedQueuedJobDataException::class);
        $this->expectExceptionMessage('"maxAttempts"');
        $this->expectExceptionMessage('missing entirely');
        $decodeQueuedJob->invoke($queue, 'default', $payload);
    }

    /**
     * `id` is what keeps two byte-identical jobs from collapsing into
     * one member of the delayed sorted set, and release() carries it
     * across every retry — so an envelope without it is corrupted
     * storage, caught here rather than reaching release() as a job with
     * no identity to preserve.
     */
    public function test_decode_queued_job_rejects_a_payload_missing_the_id_field(): void
    {
        $payload = json_encode(self::envelope(missing: ['id']), JSON_THROW_ON_ERROR);

        $queue = $this->neverConnectedQueue();
        $decodeQueuedJob = new ReflectionMethod(RedisQueue::class, 'decodeQueuedJob');

        $this->expectException(MalformedQueuedJobDataException::class);
        $this->expectExceptionMessage('"id"');
        $this->expectExceptionMessage('missing entirely');
        $decodeQueuedJob->invoke($queue, 'default', $payload);
    }

    /**
     * push() writes `id` as bin2hex(random_bytes(16)), so the shape the
     * decoder accepts is 32 lowercase hexadecimal characters and nothing
     * else. Every value here is a near miss an "any non-empty string"
     * rule would have let through as an identity release() then carries
     * across retries — one the delayed sorted set's own uniqueness never
     * rested on.
     */
    public function test_decode_queued_job_rejects_every_id_outside_the_current_encoder_shape(): void
    {
        $rejected = [
            '',
            '0123456789abcdef0123456789abcde',
            '0123456789abcdef0123456789abcdef0',
            '0123456789ABCDEF0123456789ABCDEF',
            '0123456789abcdef0123456789abcdeg',
            '01234567-89ab-cdef-0123-456789abcdef',
            ' 0123456789abcdef0123456789abcdef',
            self::VALID_ID . "\n",
            null,
            17,
            [self::VALID_ID],
        ];

        foreach ($rejected as $id) {
            $this->assertRejectedAsMalformed(self::envelope(['id' => $id]), '"id"');
        }
    }

    public function test_decode_queued_job_rejects_a_payload_missing_the_pushed_at_field(): void
    {
        $payload = json_encode(self::envelope(missing: ['pushedAt']), JSON_THROW_ON_ERROR);

        $queue = $this->neverConnectedQueue();
        $decodeQueuedJob = new ReflectionMethod(RedisQueue::class, 'decodeQueuedJob');

        $this->expectException(MalformedQueuedJobDataException::class);
        $this->expectExceptionMessage('"pushedAt"');
        $this->expectExceptionMessage('missing entirely');
        $decodeQueuedJob->invoke($queue, 'default', $payload);
    }

    public function test_decode_queued_job_rejects_a_non_numeric_stored_pushed_at_value(): void
    {
        $payload = json_encode(self::envelope(['pushedAt' => 'garbage']), JSON_THROW_ON_ERROR);

        $queue = $this->neverConnectedQueue();
        $decodeQueuedJob = new ReflectionMethod(RedisQueue::class, 'decodeQueuedJob');

        $this->expectException(MalformedQueuedJobDataException::class);
        $this->expectExceptionMessage('"pushedAt"');
        $decodeQueuedJob->invoke($queue, 'default', $payload);
    }

    /**
     * push() writes `pushedAt` as time(): a JSON number json_decode()
     * hands back as a native, positive integer. The numeric strings here
     * are exactly what QueueContract::coerceStoredInteger() accepts for
     * the backends that keep the same bookkeeping in text columns and
     * headers — this envelope never stores one, so the decoder turns
     * them away alongside a float, a bool, a null, an array, and any
     * timestamp of zero or below.
     */
    public function test_decode_queued_job_rejects_every_pushed_at_outside_the_current_encoder_shape(): void
    {
        $rejected = [
            '1700000000',
            '0',
            '-1',
            1_700_000_000.5,
            true,
            null,
            [1_700_000_000],
            0,
            -1,
            PHP_INT_MIN,
        ];

        foreach ($rejected as $pushedAt) {
            $this->assertRejectedAsMalformed(self::envelope(['pushedAt' => $pushedAt]), '"pushedAt"');
        }
    }

    /**
     * encode() always writes the `metadata` key — an empty map when a job
     * carries none — so a missing key is a truncated envelope.
     * QueueContract::coerceStoredMetadata() reads an absent value as that
     * same empty map, on behalf of the backends that write the field only
     * when a caller supplied metadata, so without a presence check of its
     * own the decoder would accept the truncated envelope as a job.
     */
    public function test_decode_queued_job_rejects_a_payload_missing_the_metadata_key_entirely(): void
    {
        $payload = json_encode(self::envelope(missing: ['metadata']), JSON_THROW_ON_ERROR);

        $queue = $this->neverConnectedQueue();
        $decodeQueuedJob = new ReflectionMethod(RedisQueue::class, 'decodeQueuedJob');

        $this->expectException(MalformedQueuedJobDataException::class);
        $this->expectExceptionMessage('"metadata"');
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
        $payload = $encode->invoke(null, $serialized, 0, null, [], self::VALID_ID, 1_700_000_000);

        self::assertIsString($payload);

        $queue = $this->neverConnectedQueue();
        $decodeQueuedJob = new ReflectionMethod(RedisQueue::class, 'decodeQueuedJob');
        $queuedJob = $decodeQueuedJob->invoke($queue, 'default', $payload);

        self::assertSame($serialized['class'], $queuedJob->class);
        self::assertSame($serialized['args'], $queuedJob->args);
        self::assertIsFloat($queuedJob->args['ratio']);
        self::assertSame(4.0, $queuedJob->args['ratio']);
    }

    /**
     * The delivery a settlement names, with $handle the exact envelope
     * string ack()/fail() hand to LREM.
     */
    private static function delivery(string $payload = '{"job":"payload"}'): QueuedJob
    {
        return new QueuedJob(RichPayloadJob::class, [], handle: $payload, queue: 'default');
    }

    /**
     * @param array<string, int|string|list<mixed>|null> $replies
     * @return array{RedisQueue, ScriptedRedisLink}
     */
    private static function scriptedQueue(array $replies): array
    {
        $link = new ScriptedRedisLink($replies);

        return [new RedisQueue(new RedisClient($link)), $link];
    }

    public function test_ack_removes_the_reserved_delivery_from_the_processing_list(): void
    {
        [$queue, $link] = self::scriptedQueue(['lrem' => 1]);

        $queue->ack(self::delivery());

        self::assertSame(
            [['lrem', ['kinetis_queue:default:processing', 1, '{"job":"payload"}']]],
            $link->commands,
        );
    }

    /**
     * LREM removing nothing means the processing list holds no entry for
     * this handle: the delivery was settled through another call, or
     * reclaimed. Reporting success would tell a worker its job is durably
     * done when nothing was written.
     */
    public function test_ack_rejects_a_handle_whose_delivery_is_already_over(): void
    {
        [$queue] = self::scriptedQueue(['lrem' => 0]);

        try {
            $queue->ack(self::delivery());
            self::fail('Expected the stale delivery to be rejected.');
        } catch (StaleJobHandleException $e) {
            self::assertSame(JobSettlement::Ack, $e->operation);
            self::assertStringContainsString('default', $e->getMessage());
        }
    }

    public function test_fail_removes_the_reserved_delivery_from_the_processing_list(): void
    {
        [$queue, $link] = self::scriptedQueue(['lrem' => 1]);

        $queue->fail(self::delivery());

        self::assertSame(
            [['lrem', ['kinetis_queue:default:processing', 1, '{"job":"payload"}']]],
            $link->commands,
        );
    }

    /**
     * fail() carries its own operation on the exception, not ack()'s —
     * QueueWorker reports a lost settlement by which one it attempted.
     */
    public function test_fail_rejects_a_handle_whose_delivery_is_already_over(): void
    {
        [$queue] = self::scriptedQueue(['lrem' => 0]);

        try {
            $queue->fail(self::delivery());
            self::fail('Expected the stale delivery to be rejected.');
        } catch (StaleJobHandleException $e) {
            self::assertSame(JobSettlement::Fail, $e->operation);
        }
    }

    /**
     * One EVAL, not an LLEN/ZCARD pair followed by a DEL: a job pushed
     * between a separate count and delete would be removed without being
     * counted. Amp\Redis\RedisClient::eval() puts the script on the wire
     * as EVALSHA, so the recorded command carries the script's hash, the
     * key count, and then the keys themselves.
     */
    public function test_clear_counts_and_deletes_in_one_script_over_the_pending_and_delayed_keys(): void
    {
        [$queue, $link] = self::scriptedQueue(['evalsha' => 7]);

        self::assertSame(7, $queue->clear('high'));

        self::assertCount(1, $link->commands, 'counting and deleting are one round trip');
        [$command, $parameters] = $link->commands[0];

        self::assertSame('evalsha', $command);
        self::assertSame(2, $parameters[1], 'the script declares exactly two keys');
        self::assertSame(
            ['kinetis_queue:high:pending', 'kinetis_queue:high:delayed'],
            \array_slice($parameters, 2),
        );
    }

    /**
     * The processing list holds deliveries workers own, and clearing must
     * never reach it — asserted over every parameter that went to the
     * wire rather than over the two keys the test above names, so a third
     * key added later fails here too.
     */
    public function test_clear_never_addresses_the_processing_list(): void
    {
        [$queue, $link] = self::scriptedQueue(['evalsha' => 0]);

        $queue->clear('high');

        foreach ($link->commands[0][1] as $parameter) {
            self::assertStringNotContainsString('processing', (string) $parameter);
        }
    }

    /**
     * A malformed name is turned away before any command reaches Redis —
     * proven by the empty command log, not merely by an unreachable
     * server refusing the connection.
     */
    public function test_clear_validates_the_queue_name_before_issuing_any_command(): void
    {
        [$queue, $link] = self::scriptedQueue(['evalsha' => 0]);

        try {
            $queue->clear('has spaces');
            self::fail('Expected the malformed queue name to be rejected.');
        } catch (InvalidQueueNameException) {
            self::assertSame([], $link->commands);
        }
    }

    public function test_the_backend_is_usable_through_the_clear_capability_type(): void
    {
        $queue = $this->neverConnectedQueue();

        self::assertInstanceOf(ClearableQueueInterface::class, $queue);

        // Called through the capability type, not the concrete class: a
        // backend that stopped declaring ClearableQueueInterface fails
        // here as a TypeError instead of passing quietly. The queue-name
        // check still throws before Redis is touched.
        $this->expectException(InvalidQueueNameException::class);
        self::clearThrough($queue, '');
    }

    /** Typed as the capability, which is the whole point of the test above. */
    private static function clearThrough(ClearableQueueInterface $queue, string $name): int
    {
        return $queue->clear($name);
    }
}
