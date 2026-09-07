<?php

declare(strict_types=1);

namespace Kinetis\QueueRedis\Tests;

use Amp\Redis\RedisClient;
use Kinetis\Queue\Exception\InvalidQueueArgumentException;
use Kinetis\Queue\ClearableQueueInterface;
use InvalidArgumentException;
use Kinetis\Queue\Exception\MalformedJobSettledException;
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
use ReflectionClass;
use ReflectionMethod;
use function Amp\Redis\createRedisClient;

/**
 * Two kinds of check, both pure PHP.
 *
 * Argument validation and the envelope encode()/decodeQueuedJob() round
 * trip need no server at all: the queue-name and push-argument rules
 * throw before the Redis client is touched, and the codec is JSON work
 * with no I/O. createRedisClient() never connects eagerly, so a client
 * pointed at a dead port is safe to construct for those.
 *
 * The rest script the wire. What they prove is not that Redis moves an
 * entry — only a real server does that, in tests-integration/ — but
 * which commands this class issues, against which keys, in which order,
 * and what it makes of each reply: the lease it installs on a
 * reservation, the sweep pop() performs before reserving, the
 * conditional replacement that makes one reclaimer the winner, and the
 * exact-member removal that fences a settlement.
 * Amp\Redis\Connection\RedisLink is the seam that supplies both halves
 * (see {@see ScriptedRedisLink}).
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

        $this->expectException(InvalidQueueArgumentException::class);
        $queue->size('');
    }

    public function test_size_rejects_a_malformed_queue_name_before_ever_touching_redis(): void
    {
        $queue = $this->neverConnectedQueue();

        $this->expectException(InvalidQueueArgumentException::class);
        $queue->size('has spaces');
    }

    public function test_clear_rejects_an_empty_queue_name_before_ever_touching_redis(): void
    {
        $queue = $this->neverConnectedQueue();

        $this->expectException(InvalidQueueArgumentException::class);
        $queue->clear('');
    }

    public function test_push_rejects_a_negative_delay_before_ever_touching_redis(): void
    {
        $queue = $this->neverConnectedQueue();

        $this->expectException(InvalidQueueArgumentException::class);
        $queue->push(new RichPayloadJob(4.0, [], Priority::High), delaySeconds: -1);
    }

    public function test_push_rejects_a_negative_max_attempts_before_ever_touching_redis(): void
    {
        $queue = $this->neverConnectedQueue();

        $this->expectException(InvalidQueueArgumentException::class);
        $queue->push(new RichPayloadJob(4.0, [], Priority::High), maxAttempts: -1);
    }

    /**
     * decodeQueuedJob()'s own $decoded['attempts']/['maxAttempts'] read is
     * where a corrupted JSON payload's malformed value is actually caught
     * — proven directly with a hand-built payload, not merely at
     * QueueContract::storedInt()'s own unit level, so the wiring
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
     * A stored completed-attempts count of exactly PHP_INT_MAX is a
     * valid integer, but decodeQueuedJob()'s `+ 1` would overflow it to
     * a float and fail QueuedJob's typed constructor with a TypeError.
     * The decode path rejects it as corrupted storage instead.
     */
    public function test_decode_queued_job_rejects_a_stored_attempts_value_of_php_int_max(): void
    {
        $payload = json_encode(self::envelope(['attempts' => PHP_INT_MAX]), JSON_THROW_ON_ERROR);

        $queue = $this->neverConnectedQueue();
        $decodeQueuedJob = new ReflectionMethod(RedisQueue::class, 'decodeQueuedJob');

        $this->expectException(MalformedQueuedJobDataException::class);
        $this->expectExceptionMessage('"attempts"');
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
     * itself (the function reserveImmediately()/reserveBlocking() wrap
     * in QueueContract::settleIfMalformed()), is what proves this
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
     * are exactly what QueueContract::storedInt() accepts for
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
     * QueueContract::storedMetadata() reads an absent value as that
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
     * list of maps, an enum case's backing value — survives encode()
     * (real json_encode with JSON_PRESERVE_ZERO_FRACTION) followed by
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
     * The complete envelope of self::envelope(), as the JSON string a
     * handle and a leased member actually are.
     *
     * @param array<string, mixed> $overrides
     */
    private static function envelopeString(array $overrides = []): string
    {
        return json_encode(self::envelope($overrides), JSON_THROW_ON_ERROR | JSON_PRESERVE_ZERO_FRACTION);
    }

    /** The delivery a settlement names, $handle being its leased member. */
    private static function delivery(string $payload = '{"job":"payload"}', int $attempts = 1): QueuedJob
    {
        return new QueuedJob(RichPayloadJob::class, [], handle: $payload, queue: 'default', attempts: $attempts);
    }

    /**
     * @param array<string, int|string|list<mixed>|null> $replies
     * @param array<string, list<int|string|list<mixed>|null>> $sequences
     *     replies consumed one per call, for the several scripts one
     *     pop() puts on the wire under the same `evalsha` name
     * @return array{RedisQueue, ScriptedRedisLink}
     */
    private static function scriptedQueue(array $replies, array $sequences = []): array
    {
        $link = new ScriptedRedisLink($replies, [], $sequences);

        return [new RedisQueue(new RedisClient($link)), $link];
    }

    /**
     * The keys one recorded EVALSHA declares. RedisClient::eval() puts a
     * script on the wire as `evalsha sha1 numkeys key... arg...`, so the
     * keys are what identifies which of pop()'s scripts a call was.
     *
     * @param array{string, list<int|float|string>} $command
     * @return list<int|float|string>
     */
    private static function scriptKeys(array $command): array
    {
        [, $parameters] = $command;

        return \array_slice($parameters, 2, (int) $parameters[1]);
    }

    /**
     * @param array{string, list<int|float|string>} $command
     * @return list<int|float|string>
     */
    private static function scriptArgs(array $command): array
    {
        [, $parameters] = $command;

        return \array_slice($parameters, 2 + (int) $parameters[1]);
    }

    public function test_a_non_positive_visibility_timeout_is_rejected(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('visibilityTimeoutSeconds of at least 1, got 0');

        new RedisQueue(createRedisClient('redis://localhost:1'), 0);
    }

    /**
     * A reservation is a lease with an end: the reserve script addresses
     * the pending and leased keys and carries the visibility window Redis
     * adds to its own clock.
     */
    public function test_reserving_leases_the_pending_tail_for_the_configured_window(): void
    {
        [$queue, $link] = self::scriptedQueue(
            ['evalsha' => null],
            ['evalsha' => [null, [], self::envelopeString()]],
        );

        $job = $queue->pop(timeoutSeconds: 1);

        self::assertSame(1, $job?->attempts);
        self::assertSame(
            ['kinetis_queue:default:pending', 'kinetis_queue:default:leased'],
            self::scriptKeys($link->commands[2]),
        );
        self::assertSame(['300'], self::scriptArgs($link->commands[2]));
    }

    /**
     * The one ordering the reserve script cannot get wrong: the lease is
     * written before the sole pending copy is removed, so a wrong-typed
     * or otherwise failing leased key aborts the script with the job
     * still on pending.
     */
    public function test_the_reserve_script_leases_before_it_removes_the_pending_copy(): void
    {
        $script = (new ReflectionClass(RedisQueue::class))->getConstant('RESERVE_SCRIPT');

        self::assertIsString($script);
        self::assertLessThan(strpos($script, "'ZADD'"), strpos($script, "'LINDEX'"));
        self::assertLessThan(strpos($script, "'LREM'"), strpos($script, "'ZADD'"));
    }

    /**
     * Recovery has no reaper behind it: the sweep an ordinary pop()
     * already performs is what promotes due delayed jobs and reclaims
     * expired leases, in that order, before it reserves.
     */
    public function test_pop_promotes_delayed_jobs_and_reclaims_expired_leases_before_reserving(): void
    {
        [$queue, $link] = self::scriptedQueue(
            ['evalsha' => null],
            ['evalsha' => [null, [], self::envelopeString()]],
        );

        $queue->pop(timeoutSeconds: 1);

        self::assertSame(
            [
                ['kinetis_queue:default:delayed', 'kinetis_queue:default:pending'],
                ['kinetis_queue:default:leased'],
                ['kinetis_queue:default:pending', 'kinetis_queue:default:leased'],
            ],
            array_map(self::scriptKeys(...), $link->commands),
        );
    }

    /**
     * Priority is list position: every script issued before the job is
     * found addresses the higher-priority queue, and the job comes back
     * from the lower one only once that queue has nothing.
     */
    public function test_pop_sweeps_named_queues_in_priority_order(): void
    {
        [$queue, $link] = self::scriptedQueue(
            ['evalsha' => null],
            ['evalsha' => [null, [], null, null, [], self::envelopeString()]],
        );

        $job = $queue->pop(timeoutSeconds: 1, queues: ['high', 'default']);

        self::assertSame('default', $job?->queue);

        foreach (\array_slice($link->commands, 0, 3) as $command) {
            foreach (self::scriptKeys($command) as $key) {
                self::assertStringContainsString(':high:', (string) $key);
            }
        }
    }

    /**
     * Nothing anywhere means one paced wait, bounded by what is left of
     * the caller's deadline, and then a return — not a spin, and not a
     * second sweep the caller has stopped waiting for.
     */
    public function test_pop_paces_its_sweeps_and_reserves_nothing_past_the_deadline(): void
    {
        [$queue, $link] = self::scriptedQueue(['evalsha' => null]);

        $start = microtime(true);
        self::assertNull($queue->pop(timeoutSeconds: 1));
        $elapsed = microtime(true) - $start;

        self::assertGreaterThanOrEqual(0.9, $elapsed);
        self::assertLessThan(2.0, $elapsed);
        self::assertCount(3, $link->commands, 'one sweep, then the wait consumed the deadline');
    }

    /**
     * The central crash path: a lease whose worker died is reclaimed by
     * the next pop(), and the envelope written back onto pending carries
     * the attempt that delivery consumed.
     */
    public function test_an_expired_lease_is_requeued_with_its_attempt_advanced(): void
    {
        $abandoned = self::envelopeString(['attempts' => 0]);
        $reclaimed = self::envelopeString(['attempts' => 1]);

        [$queue, $link] = self::scriptedQueue(
            ['evalsha' => null],
            ['evalsha' => [null, [$abandoned], 1, $reclaimed]],
        );

        $job = $queue->pop(timeoutSeconds: 1);

        self::assertSame(2, $job?->attempts);

        $requeue = $link->commands[2];
        self::assertSame(
            ['kinetis_queue:default:leased', 'kinetis_queue:default:pending'],
            self::scriptKeys($requeue),
        );
        self::assertSame([$abandoned, $reclaimed], self::scriptArgs($requeue));
    }

    /**
     * The replacement is conditional on the old member still being
     * leased, so a second sweeper — or a settlement that got there first
     * — makes this one the loser: it writes nothing further and simply
     * carries on to reserve.
     */
    public function test_a_reclaim_that_loses_the_race_writes_nothing_further(): void
    {
        [$queue, $link] = self::scriptedQueue(
            ['evalsha' => null],
            ['evalsha' => [null, [self::envelopeString()], 0, null]],
        );

        self::assertNull($queue->pop(timeoutSeconds: 1));

        self::assertSame(
            [
                ['kinetis_queue:default:delayed', 'kinetis_queue:default:pending'],
                ['kinetis_queue:default:leased'],
                ['kinetis_queue:default:leased', 'kinetis_queue:default:pending'],
                ['kinetis_queue:default:pending', 'kinetis_queue:default:leased'],
            ],
            array_map(self::scriptKeys(...), \array_slice($link->commands, 0, 4)),
        );
    }

    /**
     * Abandoned corruption settles through the malformed boundary rather
     * than being requeued: it is removed from the leased set, so the next
     * sweep does not find it again.
     */
    public function test_malformed_abandoned_lease_data_is_removed_rather_than_requeued(): void
    {
        [$queue, $link] = self::scriptedQueue(
            ['evalsha' => null, 'zrem' => 1],
            ['evalsha' => [null, ['{not valid json']]],
        );

        try {
            $queue->pop(timeoutSeconds: 1);
            self::fail('Expected the abandoned malformed member to be settled.');
        } catch (MalformedJobSettledException $e) {
            self::assertSame('default', $e->queue);
        }

        self::assertSame(
            ['zrem', ['kinetis_queue:default:leased', '{not valid json']],
            $link->commands[2],
        );
    }

    /**
     * release() replaces this delivery's exact leased member with the
     * envelope carrying the attempt it consumed, keeping the job's own
     * identity and enqueue time.
     */
    public function test_release_replaces_only_the_exact_leased_member(): void
    {
        $held = self::envelopeString(['attempts' => 0]);

        [$queue, $link] = self::scriptedQueue(['evalsha' => 1]);

        $queue->release(self::delivery($held));

        self::assertSame(
            ['kinetis_queue:default:leased', 'kinetis_queue:default:pending'],
            self::scriptKeys($link->commands[0]),
        );

        [$old, $new] = self::scriptArgs($link->commands[0]);
        self::assertSame($held, $old);

        $replacement = json_decode((string) $new, true, flags: JSON_THROW_ON_ERROR);
        self::assertSame(1, $replacement['attempts']);
        self::assertSame(self::VALID_ID, $replacement['id']);
        self::assertSame(1_700_000_000, $replacement['pushedAt']);
    }

    /**
     * A zero means the member was no longer leased: the delivery is over,
     * and reporting success would tell a worker a retry was enqueued when
     * nothing was written.
     */
    public function test_release_rejects_a_handle_whose_delivery_is_already_over(): void
    {
        [$queue] = self::scriptedQueue(['evalsha' => 0]);

        try {
            $queue->release(self::delivery(self::envelopeString()));
            self::fail('Expected the stale delivery to be rejected.');
        } catch (StaleJobHandleException $e) {
            self::assertSame(JobSettlement::Release, $e->operation);
            self::assertStringContainsString('default', $e->getMessage());
        }
    }

    public function test_ack_removes_the_exact_leased_member(): void
    {
        [$queue, $link] = self::scriptedQueue(['zrem' => 1]);

        $queue->ack(self::delivery());

        self::assertSame(
            [['zrem', ['kinetis_queue:default:leased', '{"job":"payload"}']]],
            $link->commands,
        );
    }

    /**
     * ZREM removing nothing means the leased set holds no member for this
     * handle: the delivery was settled through another call, or reclaimed
     * once its lease expired. Reporting success would tell a worker its
     * job is durably done when nothing was written.
     */
    public function test_ack_rejects_a_handle_whose_delivery_is_already_over(): void
    {
        [$queue] = self::scriptedQueue(['zrem' => 0]);

        try {
            $queue->ack(self::delivery());
            self::fail('Expected the stale delivery to be rejected.');
        } catch (StaleJobHandleException $e) {
            self::assertSame(JobSettlement::Ack, $e->operation);
            self::assertStringContainsString('default', $e->getMessage());
        }
    }

    public function test_fail_removes_the_exact_leased_member(): void
    {
        [$queue, $link] = self::scriptedQueue(['zrem' => 1]);

        $queue->fail(self::delivery());

        self::assertSame(
            [['zrem', ['kinetis_queue:default:leased', '{"job":"payload"}']]],
            $link->commands,
        );
    }

    /**
     * fail() carries its own operation on the exception, not ack()'s —
     * QueueWorker reports a lost settlement by which one it attempted.
     */
    public function test_fail_rejects_a_handle_whose_delivery_is_already_over(): void
    {
        [$queue] = self::scriptedQueue(['zrem' => 0]);

        try {
            $queue->fail(self::delivery());
            self::fail('Expected the stale delivery to be rejected.');
        } catch (StaleJobHandleException $e) {
            self::assertSame(JobSettlement::Fail, $e->operation);
        }
    }

    /**
     * Work a worker could pick up, counted in one script: pending,
     * delayed, and leases past their expiry. The leased key is addressed
     * for the expired count alone — the script's own bound is the server
     * clock, which only a real Redis supplies.
     */
    public function test_size_counts_pending_delayed_and_expired_leases_in_one_script(): void
    {
        [$queue, $link] = self::scriptedQueue(['evalsha' => 9]);

        self::assertSame(9, $queue->size('high'));

        self::assertCount(1, $link->commands, 'one round trip, one server clock');
        self::assertSame(
            ['kinetis_queue:high:pending', 'kinetis_queue:high:delayed', 'kinetis_queue:high:leased'],
            self::scriptKeys($link->commands[0]),
        );
    }

    /**
     * One EVAL, not an LLEN/ZCARD pair followed by a DEL: a job pushed
     * between a separate count and delete would be removed without being
     * counted.
     */
    public function test_clear_counts_and_deletes_in_one_script_over_the_pending_and_delayed_keys(): void
    {
        [$queue, $link] = self::scriptedQueue(['evalsha' => 7]);

        self::assertSame(7, $queue->clear('high'));

        self::assertCount(1, $link->commands, 'counting and deleting are one round trip');
        self::assertSame(
            ['kinetis_queue:high:pending', 'kinetis_queue:high:delayed'],
            self::scriptKeys($link->commands[0]),
        );
    }

    /**
     * A live lease is a job a worker is running and still has to settle,
     * so clear() must never reach the leased key — asserted over every
     * parameter that went to the wire, so a third key added later fails
     * here too.
     */
    public function test_clear_leaves_live_leases_intact(): void
    {
        [$queue, $link] = self::scriptedQueue(['evalsha' => 0]);

        $queue->clear('high');

        foreach ($link->commands[0][1] as $parameter) {
            self::assertStringNotContainsString('leased', (string) $parameter);
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
        } catch (InvalidQueueArgumentException) {
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
        $this->expectException(InvalidQueueArgumentException::class);
        self::clearThrough($queue, '');
    }

    /** Typed as the capability, which is the whole point of the test above. */
    private static function clearThrough(ClearableQueueInterface $queue, string $name): int
    {
        return $queue->clear($name);
    }
}
