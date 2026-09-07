<?php

declare(strict_types=1);

namespace Kinetis\QueueRedis;

use Kinetis\Instrumentation\Telemetry;
use Amp\Redis\RedisClient;
use InvalidArgumentException;
use Kinetis\Queue\ClearableQueueInterface;
use Kinetis\Queue\Exception\MalformedQueuedJobDataException;
use Kinetis\Queue\Exception\StaleJobHandleException;
use Kinetis\Queue\Job;
use Kinetis\Queue\JobSerializer;
use Kinetis\Queue\JobSettlement;
use Kinetis\Queue\QueueContract;
use Kinetis\Queue\QueuedJob;
use Throwable;
use function Amp\delay;

/**
 * Each queue is three Redis keys: a `:pending` list, a `:delayed` sorted
 * set scored by ready-at time, and a `:leased` sorted set scored by lease
 * expiry. Named queues are separate Redis structures, not one structure
 * with a filter on top.
 *
 * A reservation is a finite lease. pop() moves the pending tail into the
 * leased set with an expiry of `now + $visibilityTimeoutSeconds`, where
 * `now` is Redis's own `TIME`: every worker then reads one lease clock
 * regardless of its host's. A lease that passes its expiry is reclaimed
 * by any worker's next pop(), which is what makes a job whose worker died
 * mid-execution available again.
 *
 * The lease is not renewed. A job still running when its lease expires
 * can execute concurrently with its replacement, so
 * `QUEUE_VISIBILITY_TIMEOUT_SECONDS` must be sized above normal job
 * duration and handlers must be idempotent.
 *
 * The member stored in the leased set is the exact envelope string handed
 * back on QueuedJob::$handle, and that string is the delivery's fence.
 * Every envelope carries a random `id`, and reclaiming increments
 * `attempts`, so a reclaimed job's replacement envelope is a different
 * string: an earlier worker's ack(), release() or fail() finds no member
 * and raises Exception\StaleJobHandleException rather than settling the
 * delivery somebody else now holds.
 *
 * Every state change is one Lua script, which Redis executes as an
 * indivisible unit:
 *
 * - reserve() reads the pending tail, adds that exact member to the
 *   leased set, and only then removes it from pending. A wrong-typed or
 *   otherwise failing leased key aborts the script before the sole
 *   pending copy is gone.
 * - release() and lease reclaim both check that the exact member is still
 *   leased, write the incremented-attempt replacement onto pending, and
 *   then remove the old member. Two sweepers racing each other, or a
 *   sweep racing a settlement, therefore produce one winner: the loser's
 *   check fails and it writes nothing.
 * - ack() and fail() remove only the exact member, and read the removal
 *   count back so a delivery that is already over is reported rather than
 *   silently accepted.
 *
 * pop() sweeps each named queue in priority order — promote due delayed
 * jobs, reclaim expired leases, then reserve — and paces itself with
 * Amp\delay() bounded by the caller's own deadline. There is no blocking
 * Redis command and no reaper process: a blocking list move cannot
 * install a sorted-set lease in the same atomic step, and the sweep every
 * pop() already performs is the recovery path.
 *
 * Redis has no per-job columns, so bookkeeping travels inside the JSON
 * payload: the envelope is exactly {id, pushedAt, class, args, attempts,
 * maxAttempts, metadata} and all seven keys are required — `metadata`
 * included, whose absence means a truncated envelope rather than "none
 * was stored". decodeQueuedJob() validates each against the shape
 * encode() writes before a QueuedJob exists; anything else settles
 * through QueueContract::settleIfMalformed(), on the reserve path and on
 * the reclaim path alike, so abandoned corruption is removed rather than
 * swept forever. The stored `attempts` is the number of *completed*
 * attempts (0 at push time); QueuedJob::$attempts is that value plus one.
 */
final readonly class RedisQueue implements ClearableQueueInterface
{
    /**
     * The longest pop() waits between sweeps. Every wait is also bounded
     * by what is left of the caller's deadline, so a short pop() does not
     * overshoot.
     */
    private const float POLL_INTERVAL_SECONDS = 1.0;

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
     * The same ceiling for expired leases, for the same reason: each
     * reclaim is its own round trip, and an unbounded sweep would delay
     * the reservation pop() is there to make. The remainder is still
     * expired on the next sweep.
     */
    private const int LEASE_RECLAIM_BATCH_SIZE = 100;

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

    /**
     * Reads the pending tail, leases that exact member until Redis's own
     * clock passes `TIME + ARGV[1]`, and removes it from pending last, so
     * a failing leased key cannot destroy the only copy. LREM's negative
     * count removes the tail occurrence, the one this script read.
     */
    private const string RESERVE_SCRIPT = <<<'LUA'
        local member = redis.call('LINDEX', KEYS[1], -1)
        if not member then
            return false
        end
        local now = redis.call('TIME')[1]
        redis.call('ZADD', KEYS[2], now + tonumber(ARGV[1]), member)
        redis.call('LREM', KEYS[1], -1, member)
        return member
        LUA;

    /**
     * The conditional replacement release() and lease reclaim share: the
     * old member must still be leased, the replacement is written before
     * the old member is removed, and the return value says which caller
     * won.
     */
    private const string REQUEUE_SCRIPT = <<<'LUA'
        if redis.call('ZSCORE', KEYS[1], ARGV[1]) == false then
            return 0
        end
        redis.call('LPUSH', KEYS[2], ARGV[2])
        redis.call('ZREM', KEYS[1], ARGV[1])
        return 1
        LUA;

    /**
     * @param int $visibilityTimeoutSeconds how long a reservation is
     *     leased before any worker may reclaim it
     */
    public function __construct(
        private RedisClient $redis,
        private int $visibilityTimeoutSeconds = 300,
    ) {
        // A timeout below one second would make a reservation reclaimable
        // within the same second it was made, letting a second worker take
        // over work that has barely started.
        if ($visibilityTimeoutSeconds < 1) {
            throw new InvalidArgumentException(
                "RedisQueue needs a visibilityTimeoutSeconds of at least 1, got {$visibilityTimeoutSeconds}.",
            );
        }
    }

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
                $this->reclaimExpiredLeases($queue);

                $job = $this->reserve($queue);

                if ($job !== null) {
                    return $job;
                }
            }

            if ($deadline === null) {
                delay(self::POLL_INTERVAL_SECONDS);

                continue;
            }

            // Bounded by what is left of the deadline, so pop() neither
            // overshoots nor reserves a job after it: a wait that reaches
            // the deadline ends the call instead of sweeping again for a
            // caller who has stopped waiting.
            $remaining = $deadline - microtime(true);

            if ($remaining <= 0.0) {
                return null;
            }

            delay(min(self::POLL_INTERVAL_SECONDS, $remaining));

            if (microtime(true) >= $deadline) {
                return null;
            }
        }
    }

    /**
     * A Lua `false` for an empty pending list comes back as a RESP null
     * bulk reply, and eval() is typed `mixed`, so there is no coercion to
     * trip over.
     */
    private function reserve(string $queue): ?QueuedJob
    {
        $payload = $this->redis->eval(
            self::RESERVE_SCRIPT,
            [self::pendingKey($queue), self::leasedKey($queue)],
            [(string) $this->visibilityTimeoutSeconds],
        );

        if ($payload === null) {
            return null;
        }

        /** @var string $payload */
        return QueueContract::settleIfMalformed(
            $queue,
            fn (): QueuedJob => $this->decodeQueuedJob($queue, $payload),
            function () use ($queue, $payload): void {
                $this->removeLease($queue, $payload);
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
     * the message it was read from instead of crashing the worker.
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

    /**
     * Replaces this delivery's leased member with an envelope carrying
     * the attempt it just consumed. The replacement is written only if
     * that exact member is still leased, so a duplicate release() — a
     * stale handle, or a retry after a connection drop whose server-side
     * outcome is unknown — raises Exception\StaleJobHandleException
     * instead of enqueueing a second copy.
     */
    #[\Override]
    public function release(QueuedJob $job): void
    {
        /** @var string $oldPayload */
        $oldPayload = $job->handle;

        if ($this->requeue($job->queue, $oldPayload, self::advancedEnvelope($job, $oldPayload)) !== 1) {
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
     * Work a worker could pick up: pending, delayed — a delayed job is
     * outstanding even while its own delay keeps it from being popped —
     * and leases past their expiry, which the next pop() reclaims. Live
     * leases belong to a worker already and are excluded. Counted in one
     * script so the three reads share the server clock the expiry
     * comparison needs.
     */
    #[\Override]
    public function size(string $queue = 'default'): int
    {
        QueueContract::assertValidQueueName($queue);

        $size = $this->redis->eval(
            <<<'LUA'
            local now = redis.call('TIME')[1]
            return redis.call('LLEN', KEYS[1])
                + redis.call('ZCARD', KEYS[2])
                + redis.call('ZCOUNT', KEYS[3], '-inf', now)
            LUA,
            [self::pendingKey($queue), self::delayedKey($queue), self::leasedKey($queue)],
        );

        return (int) $size;
    }

    /**
     * Counting and deleting run as one Lua script rather than a size()
     * followed by a DEL: a job pushed between the two would otherwise be
     * deleted without being counted, and DEL's own return value counts
     * keys, not the jobs inside them. The number returned is what this
     * call removed.
     *
     * The leased key is untouched. A lease is work a worker is running,
     * and clear() has no handover to make: dropping it would leave that
     * worker's settlement raising a stale-handle failure for a job
     * nothing recorded.
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
        if ($this->removeLease($queue, $payload) !== 1) {
            throw StaleJobHandleException::forSettlement($operation, $queue);
        }
    }

    /**
     * Shared by settle() (a real QueuedJob's handle and queue) and the
     * malformed-message paths (the raw payload a decode failure was
     * caught for, with no QueuedJob to read either off). The count is how
     * many members ZREM removed: 1 for a live lease, 0 for a delivery
     * that is already over.
     */
    private function removeLease(string $queue, string $payload): int
    {
        return $this->redis->getSortedSet(self::leasedKey($queue))->remove($payload);
    }

    /**
     * Returns 1 when this caller performed the transition and 0 when the
     * member was no longer leased — the fence release() reports on and
     * reclaimExpiredLeases() reads to decide it lost a race.
     */
    private function requeue(string $queue, string $oldPayload, string $newPayload): int
    {
        return (int) $this->redis->eval(
            self::REQUEUE_SCRIPT,
            [self::leasedKey($queue), self::pendingKey($queue)],
            [$oldPayload, $newPayload],
        );
    }

    /**
     * Moves every lease whose expiry has passed back onto pending with
     * its attempt count advanced, in batches (see
     * LEASE_RECLAIM_BATCH_SIZE). The expiry comparison uses Redis's own
     * TIME, so a worker with a skewed host clock neither reclaims early
     * nor holds a lease past its end.
     *
     * A member that no longer decodes is abandoned corruption: it settles
     * through the same malformed boundary a reservation does, which
     * removes it and raises Exception\MalformedJobSettledException, so it
     * is neither requeued forever nor swept on every subsequent pop().
     */
    private function reclaimExpiredLeases(string $queue): void
    {
        $expired = $this->redis->eval(
            <<<'LUA'
            local now = redis.call('TIME')[1]
            return redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now, 'LIMIT', 0, ARGV[1])
            LUA,
            [self::leasedKey($queue)],
            [(string) self::LEASE_RECLAIM_BATCH_SIZE],
        );

        /** @var list<string> $expired */
        $expired = \is_array($expired) ? $expired : [];

        foreach ($expired as $payload) {
            $replacement = QueueContract::settleIfMalformed(
                $queue,
                fn (): string => self::advancedEnvelope($this->decodeQueuedJob($queue, $payload), $payload),
                function () use ($queue, $payload): void {
                    $this->removeLease($queue, $payload);
                },
            );

            // A zero means another sweeper reclaimed this member first, or
            // its own worker settled it after the expiry was read. Either
            // way that caller owns the outcome and this one writes nothing.
            $this->requeue($queue, $payload, $replacement);
        }
    }

    /**
     * The envelope that replaces $oldPayload once its attempt is spent:
     * the same job with QueuedJob::$attempts — the completed-attempt
     * count including this delivery — persisted.
     *
     * id/pushedAt are carried over from the envelope being replaced, not
     * regenerated. A fresh id only needs to be unique between independent
     * pushes (see encode()'s own docblock); regenerating either here
     * would erase the job's logical identity and original enqueue time
     * across every retry. Both are required fields, read through the same
     * envelopeIdentity() check decodeQueuedJob() already applied to this
     * exact payload.
     */
    private static function advancedEnvelope(QueuedJob $job, string $oldPayload): string
    {
        [$id, $pushedAt] = self::envelopeIdentity(
            QueueContract::storedJsonArray($oldPayload, 'payload'),
        );

        return self::encode(
            ['class' => $job->class, 'args' => $job->args],
            attempts: $job->attempts,
            maxAttempts: $job->maxAttempts,
            metadata: $job->metadata,
            id: $id,
            pushedAt: $pushedAt,
        );
    }

    /**
     * Writes every field of the envelope, identity included — the id and
     * pushedAt are always the caller's own, never invented here: push()
     * generates a fresh pair per *independent* push (which is what keeps
     * two envelopes with byte-identical job data from ever becoming the
     * same string — the delayed and leased sorted sets both need their
     * members to be unique), and a retry passes the pair it read off the
     * envelope being replaced.
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
     * collapsing into one member of a sorted set, and `pushedAt` is the
     * original enqueue time every retry carries forward. Both are checked
     * against the exact shape encode() writes — `id` is
     * bin2hex(random_bytes(16)), `pushedAt` a positive Unix time
     * json_decode() returned as a native integer — because accepting a
     * wider shape here would mean carrying an identity the sorted set's
     * uniqueness never rested on.
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
     * unit, so two workers calling this concurrently are serialized by
     * Redis itself: whichever runs first moves its whole batch of ready
     * members, and the second sees whatever is left.
     *
     * The due-time comparison is this process's own clock, matching the
     * score push() writes. Lease expiry is the one clock that must be
     * shared across workers, and it reads Redis TIME for exactly that.
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

    private static function leasedKey(string $queue): string
    {
        return "kinetis_queue:{$queue}:leased";
    }

    private static function delayedKey(string $queue): string
    {
        return "kinetis_queue:{$queue}:delayed";
    }
}
