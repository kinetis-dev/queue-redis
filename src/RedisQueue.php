<?php

declare(strict_types=1);

namespace Kinetis\QueueRedis;

use Kinetis\Instrumentation\Telemetry;
use Amp\Redis\RedisClient;
use Kinetis\Queue\ClearableQueueInterface;
use Kinetis\Queue\Exception\MalformedQueuedJobDataException;
use Kinetis\Queue\Exception\StaleJobHandleException;
use Kinetis\Queue\Job;
use Kinetis\Queue\JobSerializer;
use Kinetis\Queue\JobSettlement;
use Kinetis\Queue\QueueContract;
use Kinetis\Queue\QueuedJob;
use Throwable;

/**
 * A naive Redis list pop removes the item at pop time — a worker that
 * crashes mid-job loses it with no way to detect or retry. This uses the
 * reliable-queue pattern instead: pop() atomically moves a payload from a
 * queue's pending list to a separate processing list, ack() removes it
 * from there, release() moves it back. That gives the same at-least-once
 * guarantee SqlQueue's `reserved_at` column gives.
 *
 * Every key is scoped by queue name (`kinetis_queue:{queue}:pending`,
 * `:processing`, `:delayed`) — named queues are genuinely separate Redis
 * structures, not one shared structure with a filter on top.
 *
 * pop() sweeps every queue in priority order with a non-blocking reserve
 * first, then suspends on the highest-priority queue for one second
 * before sweeping again — see QueueInterface for the contract this
 * meets. The immediate sweep cannot use amphp/redis's popTailPushHead()
 * (RPOPLPUSH): its declared return type is non-nullable `string` and
 * Redis answers nil on an empty list, which throws a TypeError inside
 * amphp/redis. reserveImmediately() runs the equivalent RPOP+LPUSH as
 * one Lua script instead. reserveBlocking() uses the correctly-nullable
 * popTailPushHeadBlocking() and is never called with 0, which BRPOPLPUSH
 * reads as "block forever".
 *
 * Deliberately not built: a reaper for jobs stuck in a processing list
 * because the worker that popped them died before settling. That needs a
 * visibility-timeout mechanism this backend does not have. Such a job is
 * stranded, not lost — it is still in the processing list, exactly where
 * a reaper would find it.
 *
 * release() and promoteDelayedJobs() each run as a single Lua script.
 * Redis executes one as an indivisible unit, so a crash can never leave
 * a job removed from the source with nothing written to the destination,
 * and a concurrent worker cannot observe a partially applied script.
 *
 * Indivisible is not the same as conditional, and release() needs both:
 * its script performs the LPUSH only when the LREM actually removed the
 * source entry, so a duplicate release() — a stale QueuedJob, a retry
 * after a connection drop with an unknown server-side outcome — raises
 * Exception\StaleJobHandleException instead of writing a second copy onto
 * pending. promoteDelayedJobs() needs no such guard: it has no
 * caller-supplied handle to go stale.
 *
 * All three settlements are fenced the same way. LREM reports how many
 * entries it removed, so a zero means this delivery is over and the
 * settlement raises StaleJobHandleException rather than reporting a
 * removal that never happened. The malformed-message path settles
 * through the same LREM without reading the count back: that message was
 * reserved moments earlier by this very call, so there is no stale
 * delivery to report.
 *
 * Every envelope carries a random `id`. Two pushes of byte-identical job
 * data would otherwise produce the same JSON string, and a sorted set's
 * members are unique, so a delayed duplicate would collapse onto the
 * first. release() preserves the `id`/`pushedAt` it reads off the
 * envelope it replaces: uniqueness only has to hold between independent
 * pushes, and regenerating either would erase the job's logical identity
 * and original enqueue time.
 *
 * Redis has no per-job columns, so bookkeeping travels inside the JSON
 * payload: the envelope is exactly {id, pushedAt, class, args, attempts,
 * maxAttempts, metadata} and all seven keys are required — `metadata`
 * included, whose absence means a truncated envelope rather than "none
 * was stored". decodeQueuedJob() validates each against the shape
 * encode() writes before a QueuedJob exists; anything else settles
 * through QueueContract::settleIfMalformed(). The stored `attempts` is
 * the number of *completed* attempts (0 at push time); QueuedJob::$attempts
 * is that value plus one.
 */
final readonly class RedisQueue implements ClearableQueueInterface
{
    /**
     * How long pop() suspends on the highest-priority queue when nothing
     * is waiting anywhere. Redis's blocking primitives take whole
     * seconds only, and 0 means "block forever", so one second is the
     * shortest wait available.
     */
    private const int BLOCK_SECONDS = 1;

    /**
     * A ceiling on how many delayed jobs promoteDelayedJobs() moves in one
     * call. Without one, a large ready backlog is read and moved entirely
     * inside a single Lua script — Redis executes one command at a time,
     * so every other client sharing this Redis (cache reads, other
     * queues) stalls for the full duration. Redis's own Lua time-limit
     * setting doesn't help once writes have started: it warns/blocks
     * other clients rather than rolling the script back, so it's not a
     * safe substitute for bounding the work up front. Any excess stays in
     * the delayed set (still due, since their score is unchanged) and is
     * picked up by pop()'s own loop, which already calls this on every
     * iteration.
     */
    private const int DELAYED_PROMOTION_BATCH_SIZE = 100;

    /**
     * The exact shape push() writes for `id`: bin2hex(random_bytes(16)),
     * which is 32 hexadecimal characters, lowercase because that is the
     * only case bin2hex() emits. envelopeIdentity() matches against this
     * rather than accepting any non-empty string, so an identity from
     * some other producer is corrupted storage here, not a job.
     *
     * The end anchor is `\z`, not `$`: PCRE's `$` also matches directly
     * before a trailing newline, which would let 32 hex characters plus
     * a newline through as a valid id.
     */
    private const string ID_PATTERN = '/^[0-9a-f]{32}\z/';

    public function __construct(
        private RedisClient $redis,
    ) {}

    #[\Override]
    public function push(Job $job, int $delaySeconds = 0, string $queue = 'default', ?int $maxAttempts = null): void
    {
        QueueContract::assertValidPushArguments($delaySeconds, $queue, $maxAttempts);

        $telemetryToken = Telemetry::global()->jobPushStarted($job::class, $queue);

        try {
            $metadata = Telemetry::global()->jobPushMetadata($telemetryToken);
            $payload = self::encode(
                JobSerializer::serialize($job),
                attempts: 0,
                maxAttempts: $maxAttempts,
                metadata: $metadata,
                id: bin2hex(random_bytes(16)),
                pushedAt: time(),
            );

            if ($delaySeconds > 0) {
                $this->redis->getSortedSet(self::delayedKey($queue))->add([$payload => (float) (time() + $delaySeconds)]);
            } else {
                $this->redis->getList(self::pendingKey($queue))->pushHead($payload);
            }

            Telemetry::global()->jobPushEnded($telemetryToken, null);
        } catch (Throwable $e) {
            Telemetry::global()->jobPushEnded($telemetryToken, $e);

            throw $e;
        }
    }

    #[\Override]
    public function pop(int $timeoutSeconds = 0, array $queues = ['default']): ?QueuedJob
    {
        QueueContract::assertValidPopArguments($timeoutSeconds, $queues);

        if ($queues === []) {
            return null;
        }

        $deadline = $timeoutSeconds > 0 ? microtime(true) + $timeoutSeconds : null;

        while (true) {
            foreach ($queues as $queue) {
                $this->promoteDelayedJobs($queue);

                $job = $this->reserveImmediately($queue);

                if ($job !== null) {
                    return $job;
                }
            }

            if ($deadline !== null && microtime(true) >= $deadline) {
                return null;
            }

            // Nothing waiting anywhere, so suspend on the highest-priority
            // queue rather than spinning. Delayed promotion and the
            // lower-priority queues are re-checked on the next sweep.
            // BRPOPLPUSH counts whole seconds and reads 0 as "block
            // forever", so the wait is a fixed one second rather than
            // what is left of the deadline.
            $job = $this->reserveBlocking($queues[0], self::BLOCK_SECONDS);

            if ($job !== null) {
                return $job;
            }

            // That wait can consume the rest of the deadline on its own.
            // Rechecking here, rather than only at the top of the next
            // sweep, keeps an expired deadline from reserving a job the
            // caller has already stopped waiting for — a reservation
            // nothing would settle until it was reclaimed.
            if ($deadline !== null && microtime(true) >= $deadline) {
                return null;
            }
        }
    }

    /**
     * An atomic RPOP+LPUSH pair as one Lua script — the same reliable
     * pending-to-processing move, without amphp/redis's non-nullable
     * popTailPushHead() wrapper. A Lua `false` from an empty list comes
     * back as a RESP null bulk reply, and eval() is typed `mixed`, so
     * there is no coercion to trip over.
     */
    private function reserveImmediately(string $queue): ?QueuedJob
    {
        $payload = $this->redis->eval(
            <<<'LUA'
            local payload = redis.call('RPOP', KEYS[1])
            if payload then
                redis.call('LPUSH', KEYS[2], payload)
            end
            return payload
            LUA,
            [self::pendingKey($queue), self::processingKey($queue)],
            [],
        );

        if ($payload === null) {
            return null;
        }

        /** @var string $payload */
        return QueueContract::settleIfMalformed(
            $queue,
            fn (): QueuedJob => $this->decodeQueuedJob($queue, $payload),
            function () use ($queue, $payload): void {
                $this->removeFromProcessing($queue, $payload);
            },
        );
    }

    /**
     * A genuine BRPOPLPUSH, suspending the calling Fiber through Revolt
     * for up to $waitSeconds. Never called with 0, which Redis reads as
     * "block forever".
     */
    private function reserveBlocking(string $queue, int $waitSeconds): ?QueuedJob
    {
        $payload = $this->redis->getList(self::pendingKey($queue))
            ->popTailPushHeadBlocking(self::processingKey($queue), $waitSeconds);

        if ($payload === null) {
            return null;
        }

        return QueueContract::settleIfMalformed(
            $queue,
            fn (): QueuedJob => $this->decodeQueuedJob($queue, $payload),
            function () use ($queue, $payload): void {
                $this->removeFromProcessing($queue, $payload);
            },
        );
    }

    /**
     * Every field goes through a QueueContract helper rather than a
     * PHPStan-asserted shape: a corrupted payload could carry anything.
     * `attempts` is the completed-attempts count that gets a `+ 1` right
     * below, so its upper bound keeps a stored PHP_INT_MAX from
     * overflowing that addition into a float. `maxAttempts` and
     * `metadata` are checked for *presence* first, since encode() always
     * writes both keys and a legitimately-null or empty value is
     * indistinguishable from a truncated envelope through a `?? null`
     * read alone. Every failure here is caught by the caller through
     * QueueContract::settleIfMalformed(), so a malformed payload settles
     * the already-reserved message instead of crashing the worker.
     */
    private function decodeQueuedJob(string $queue, string $payload): QueuedJob
    {
        $decoded = QueueContract::storedJsonArray($payload, 'payload');

        self::envelopeIdentity($decoded);

        $class = QueueContract::storedClass($decoded['class'] ?? null);
        $args = QueueContract::storedArgs($decoded['args'] ?? null);

        QueueContract::assertFieldPresent($decoded, 'metadata');
        $metadata = QueueContract::storedMetadata($decoded['metadata']);

        QueueContract::assertFieldPresent($decoded, 'maxAttempts');
        $maxAttempts = QueueContract::storedNullableInt($decoded['maxAttempts'], 'maxAttempts', 0);

        return new QueuedJob(
            $class,
            $args,
            handle: $payload,
            queue: $queue,
            attempts: QueueContract::storedInt($decoded['attempts'] ?? null, 'attempts', 0, PHP_INT_MAX - 1) + 1,
            maxAttempts: $maxAttempts,
            metadata: $metadata,
        );
    }

    #[\Override]
    public function ack(QueuedJob $job): void
    {
        /** @var string $payload */
        $payload = $job->handle;
        $this->settle(JobSettlement::Ack, $job->queue, $payload);
    }

    #[\Override]
    public function release(QueuedJob $job): void
    {
        /** @var string $oldPayload */
        $oldPayload = $job->handle;

        // id/pushedAt are carried over from the envelope being replaced,
        // not regenerated — a fresh id only needs to be unique *between
        // independent pushes* (see encode()'s own docblock, and this
        // class's own docblock for why the delayed sorted set needs
        // that). Regenerating either here would erase the job's logical
        // identity and original enqueue time across every retry instead.
        //
        // Both are required fields, read through the same
        // envelopeIdentity() check decodeQueuedJob() already applied to
        // this exact payload before handing back the QueuedJob whose
        // handle it is.
        [$id, $pushedAt] = self::envelopeIdentity(
            QueueContract::storedJsonArray($oldPayload, 'payload'),
        );

        $newPayload = self::encode(
            ['class' => $job->class, 'args' => $job->args],
            attempts: $job->attempts,
            maxAttempts: $job->maxAttempts,
            metadata: $job->metadata,
            id: $id,
            pushedAt: $pushedAt,
        );

        // One Lua script, not a remove() call followed by a separate
        // pushHead() — see this class's own docblock for why the two-command
        // version could lose the job outright on a crash between them.
        // The destination write is gated on LREM actually having found and
        // removed $oldPayload: without that check, this is indivisible
        // but not a valid *conditional* transition — a duplicate
        // release() call with the same handle, or a client retry after a
        // connection drop whose server-side outcome is unknown, would
        // otherwise LPUSH a second replacement even though the source
        // entry the caller thinks it's releasing is already gone.
        $removed = $this->redis->eval(
            <<<'LUA'
            local removed = redis.call('LREM', KEYS[1], 1, ARGV[1])
            if removed == 1 then
                redis.call('LPUSH', KEYS[2], ARGV[2])
            end
            return removed
            LUA,
            [self::processingKey($job->queue), self::pendingKey($job->queue)],
            [$oldPayload, $newPayload],
        );

        if ($removed !== 1) {
            throw StaleJobHandleException::forSettlement(JobSettlement::Release, $job->queue);
        }
    }

    #[\Override]
    public function fail(QueuedJob $job): void
    {
        /** @var string $payload */
        $payload = $job->handle;
        $this->settle(JobSettlement::Fail, $job->queue, $payload);
    }

    /**
     * Pending plus delayed: a delayed job is waiting on this queue even
     * while its own delay keeps it from being popped yet, so counting it
     * is what makes "how much work is outstanding" match reality. The
     * processing list is excluded — those belong to a worker already.
     */
    #[\Override]
    public function size(string $queue = 'default'): int
    {
        QueueContract::assertValidQueueName($queue);

        return $this->redis->getList(self::pendingKey($queue))->getSize()
            + $this->redis->getSortedSet(self::delayedKey($queue))->getSize();
    }

    /**
     * Counting and deleting run as one Lua script rather than a size()
     * followed by a DEL: a job pushed between the two would otherwise be
     * deleted without being counted, and DEL's own return value counts
     * keys, not the jobs inside them. The number returned is what this
     * call removed.
     */
    #[\Override]
    public function clear(string $queue = 'default'): int
    {
        QueueContract::assertValidQueueName($queue);

        $removed = $this->redis->eval(
            <<<'LUA'
            local removed = redis.call('LLEN', KEYS[1]) + redis.call('ZCARD', KEYS[2])
            redis.call('DEL', KEYS[1], KEYS[2])
            return removed
            LUA,
            [self::pendingKey($queue), self::delayedKey($queue)],
        );

        return (int) $removed;
    }

    /**
     * ack()/fail(), which settle a delivery a caller holds a handle for
     * and so have someone to answer when that delivery is already over.
     */
    private function settle(JobSettlement $operation, string $queue, string $payload): void
    {
        if ($this->removeFromProcessing($queue, $payload) !== 1) {
            throw StaleJobHandleException::forSettlement($operation, $queue);
        }
    }

    /**
     * Shared by settle() (a real QueuedJob's handle and queue) and the
     * malformed-message path (the raw payload a decode failure was
     * caught for, with no QueuedJob to read either off). The count is
     * how many entries LREM removed: 1 for a live reservation, 0 for a
     * delivery that is already over.
     */
    private function removeFromProcessing(string $queue, string $payload): int
    {
        return $this->redis->getList(self::processingKey($queue))->remove($payload, 1);
    }

    /**
     * Writes every field of the envelope, identity included — the id and
     * pushedAt are always the caller's own, never invented here: push()
     * generates a fresh pair per *independent* push (which is what keeps
     * two envelopes with byte-identical job data from ever becoming the
     * same string — see this class's own docblock for why that matters
     * specifically for the delayed sorted set, whose members must be
     * unique), and release() passes the pair it read off the envelope
     * being replaced.
     *
     * @param array{class: class-string, args: array<string, mixed>} $serialized
     * @param array<string, string> $metadata
     */
    private static function encode(array $serialized, int $attempts, ?int $maxAttempts, array $metadata, string $id, int $pushedAt): string
    {
        return json_encode([
            'id' => $id,
            'pushedAt' => $pushedAt,
            ...$serialized,
            'attempts' => $attempts,
            'maxAttempts' => $maxAttempts,
            'metadata' => $metadata,
            // PRESERVE_ZERO_FRACTION: without it, an integral-valued
            // float argument (4.0) encodes as "4" and decodes back as
            // an int — a silent type change JobSerializer::serialize()'s
            // own portable-value contract promises never happens.
        ], JSON_THROW_ON_ERROR | JSON_PRESERVE_ZERO_FRACTION);
    }

    /**
     * The two identity fields every envelope carries, validated rather
     * than read optionally: `id` keeps two byte-identical jobs from
     * collapsing into one member of the delayed sorted set, and
     * `pushedAt` is the original enqueue time release() carries across
     * every retry. Both are checked against the exact shape encode()
     * writes — `id` is bin2hex(random_bytes(16)), `pushedAt` a positive
     * Unix time json_decode() returned as a native integer — because
     * accepting a wider shape here would mean release() carrying an
     * identity the sorted set's uniqueness never rested on.
     *
     * @param array<array-key, mixed> $decoded
     * @return array{0: string, 1: int}
     */
    private static function envelopeIdentity(array $decoded): array
    {
        QueueContract::assertFieldPresent($decoded, 'id');
        $id = $decoded['id'];

        if (!is_string($id) || preg_match(self::ID_PATTERN, $id) !== 1) {
            throw MalformedQueuedJobDataException::invalidShape('id', $id);
        }

        QueueContract::assertFieldPresent($decoded, 'pushedAt');
        $pushedAt = $decoded['pushedAt'];

        if (!is_int($pushedAt)) {
            throw MalformedQueuedJobDataException::invalidShape('pushedAt', $pushedAt);
        }

        if ($pushedAt < 1) {
            throw MalformedQueuedJobDataException::outOfBounds('pushedAt', 'an enqueue timestamp must be a positive Unix time');
        }

        return [$id, $pushedAt];
    }

    /**
     * One Lua script does the read (ZRANGEBYSCORE, bounded by
     * DELAYED_PROMOTION_BATCH_SIZE — see that constant's own docblock)
     * and every move (ZREM+LPUSH per ready member) as a single indivisible
     * unit — see this class's own docblock for why a read followed by
     * separate remove-then-push commands per member would both
     * double-process under concurrent workers and lose a job outright on
     * a crash mid-loop. Redis executes one EVAL to completion before
     * touching another command from any client, so two workers calling
     * this concurrently are simply serialized by Redis itself: whichever
     * one runs first moves its whole batch of ready members, and the
     * second sees whatever's left (nothing, or the next batch) — no
     * return-value check needed to tell which worker "won."
     */
    private function promoteDelayedJobs(string $queue): void
    {
        $this->redis->eval(
            <<<'LUA'
            local ready = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
            for _, member in ipairs(ready) do
                redis.call('ZREM', KEYS[1], member)
                redis.call('LPUSH', KEYS[2], member)
            end
            LUA,
            [self::delayedKey($queue), self::pendingKey($queue)],
            [(string) time(), (string) self::DELAYED_PROMOTION_BATCH_SIZE],
        );
    }

    private static function pendingKey(string $queue): string
    {
        return "kinetis_queue:{$queue}:pending";
    }

    private static function processingKey(string $queue): string
    {
        return "kinetis_queue:{$queue}:processing";
    }

    private static function delayedKey(string $queue): string
    {
        return "kinetis_queue:{$queue}:delayed";
    }
}
