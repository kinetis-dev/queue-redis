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
use Kinetis\Queue\Support\PopSweep;
use LogicException;
use Throwable;

/**
 * A naive Redis list pop already removes the item at pop time — if a
 * worker crashes mid-job, it's just gone, no way to detect or retry. This
 * uses the "reliable queue" pattern instead: pop() atomically moves a
 * job's payload from a queue's pending list to a separate processing list
 * (popTailPushHeadBlocking() — genuinely BRPOPLPUSH, suspending the
 * calling Fiber via Revolt, not busy-polling) rather than deleting it
 * outright. ack() removes it from the processing list; release() moves it
 * back onto the pending list. This gives the same at-least-once
 * possibility SqlQueue's `reserved_at` column gives, not a silently
 * weaker guarantee just because the backend differs.
 *
 * Every key is scoped by queue name (`kinetis_queue:{queue}:pending`,
 * `:processing`, `:delayed`) — named queues are genuinely separate Redis
 * lists/sorted sets, not one shared structure with a filter on top.
 *
 * pop($timeoutSeconds, $queues) delegates its whole priority/timeout
 * algorithm to Kinetis\Queue\Support\PopSweep — see that class and
 * QueueInterface's own docblock for the full cross-backend contract.
 * This class supplies exactly one thing PopSweep needs: probe(), a
 * single-queue check that can spend up to a given wait budget.
 *
 * probe() cannot use amphp/redis's non-blocking popTailPushHead() (plain
 * RPOPLPUSH) for PopSweep's own zero-wait, immediate phase — its declared
 * return type is non-nullable `string`, but Redis returns nil for an
 * empty source list, which throws a TypeError inside amphp/redis itself.
 * probeNonBlocking() instead runs its own small Lua script — an atomic,
 * genuinely non-blocking RPOP+LPUSH pair — bypassing that buggy wrapper
 * entirely while keeping the exact same reliable pending->processing
 * move semantics. For a real, positive wait budget, probeBlocking() uses
 * the correctly-nullable popTailPushHeadBlocking() instead — but never
 * with a literal 0 for the timeout: BRPOPLPUSH's own timeout=0 means
 * "block forever," the opposite of PopSweep's own "don't block at all"
 * meaning for a zero wait budget, so a sub-one-second remaining budget
 * (Redis's blocking primitives have no fractional timeout) is handled by
 * one more non-blocking probe instead of rounding either up (overshooting
 * the deadline) or down to a literal 0 (blocking forever).
 *
 * Deliberately not built: a reaper for jobs stuck in a processing list
 * because the worker that popped them crashed before ack()/release()
 * ever ran. That's a real gap, not an oversight — closing it needs a
 * visibility-timeout mechanism (a per-job "reserved at" timestamp plus a
 * periodic scan) this first cut doesn't have yet. A job in that state is
 * stranded, not lost — it's still sitting in the processing list, exactly
 * where a future reaper would find it.
 *
 * Two other transitions could otherwise lose a job outright: release()
 * (processing -> pending) and promoteDelayedJobs() (delayed -> pending)
 * both run as a single Lua script (eval()), which Redis executes as one
 * indivisible unit — a naive remove-then-push pair of separate commands
 * would leave a job removed from the source with nothing ever written to
 * the destination if a process crashed between them. No other command,
 * including a second worker's own concurrent promotion, can observe or
 * interleave with a partially-applied script, and a client that dies
 * before or after the call can never observe a state where the job
 * exists in neither list, only "still in the source" or "already in the
 * destination".
 *
 * Indivisible isn't the same as conditional, and release() needs both:
 * its script only performs the LPUSH when the LREM actually found and
 * removed the source entry (returning that count so the PHP side can
 * tell), so a second release() call with the same handle — a duplicate
 * call, a stale QueuedJob, or a retry after a connection drop whose
 * server-side outcome is unknown — throws Exception\StaleJobHandleException
 * instead of writing a second replacement onto pending. promoteDelayedJobs()
 * doesn't need the same guard: it has no caller-supplied handle to go
 * stale, only a self-contained read-and-move over whatever's currently
 * ready, so two concurrent calls simply serialize through Redis with
 * nothing left for the second to double-process.
 *
 * All three settlements are fenced against a delivery that is already
 * over. LREM reports how many entries it removed, so ack() and fail()
 * read that count the same way release()'s script returns its own: a
 * zero means the processing list held no entry for this handle, and the
 * settlement raises Exception\StaleJobHandleException rather than
 * reporting a removal that never happened. QueuedJob's own docblock
 * states the delivery-receipt contract this meets.
 *
 * The malformed-message path settles through the same LREM without
 * reading it back: the message it settles was reserved moments earlier
 * by this very call, so there is no stale delivery to report and pop()
 * raises Exception\MalformedQueuedJobDataException instead.
 *
 * Every envelope carries a cryptographically random `id`, generated fresh
 * for an independent push(): two pushes with byte-identical job data
 * would otherwise produce the exact same JSON string, and a Redis sorted
 * set's members are unique, so the second push() of a delayed duplicate
 * would collapse onto the first instead of creating a second entry.
 * release() preserves the `id`/`pushedAt` it reads off the envelope it's
 * replacing rather than regenerating them — uniqueness only ever has to
 * hold *between* independent pushes, and regenerating on every retry
 * would erase the job's own logical identity and original enqueue time
 * for no benefit.
 *
 * Redis has no per-job columns the way SqlQueue has an `attempts` column,
 * so attempts/maxAttempts travel inside the JSON payload itself. The
 * envelope is exactly {id, pushedAt, class, args, attempts, maxAttempts,
 * metadata}, reread and rewritten by release() on every retry, and all
 * seven keys are required — `metadata` included, whose absence is a
 * truncated envelope rather than "no metadata was stored".
 * decodeQueuedJob() validates every one of them before a QueuedJob
 * exists, each against the shape encode() writes rather than the widest
 * shape a cross-backend coercer accepts: `id` is 32 lowercase
 * hexadecimal characters, and `pushedAt` a positive Unix timestamp that
 * json_decode() returned as a native integer. An envelope missing or
 * corrupting any of them settles through
 * QueueContract::settleIfMalformed() instead of reaching ack()/release()
 * as a partially accepted job. The stored `attempts` value is the number
 * of *completed* attempts (0 at push time); QueuedJob::$attempts is
 * always that value plus one.
 */
final readonly class RedisQueue implements ClearableQueueInterface
{
    private const PER_QUEUE_POLL_TIMEOUT_SECONDS = 1;

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
        // PopSweep::run() itself validates $timeoutSeconds/$queues via
        // QueueContract before touching either — see that class's own
        // docblock for why it doesn't trust a caller to have already
        // done so.
        return PopSweep::run(
            timeoutSeconds: $timeoutSeconds,
            queues: $queues,
            probe: function (string $queue, float $waitSeconds): ?QueuedJob {
                $this->promoteDelayedJobs($queue);

                return $waitSeconds < 1.0
                    ? $this->probeNonBlocking($queue)
                    : $this->probeBlocking($queue, (int) floor($waitSeconds));
            },
            probeCanBlock: true,
            waitCapSeconds: (float) self::PER_QUEUE_POLL_TIMEOUT_SECONDS,
            sleep: static function (): never {
                throw new LogicException('RedisQueue never paces via sleep() — every probe either blocks natively or is instant.');
            },
        );
    }

    /**
     * The immediate, non-blocking half of probe() — an atomic RPOP+LPUSH
     * pair run as one Lua script, never amphp/redis's own buggy
     * popTailPushHead() wrapper (see this class's own docblock for why).
     * `redis.call('RPOP', ...)` returning nothing becomes a Lua `false`,
     * which the RESP protocol reports back as a null bulk reply —
     * $this->redis->eval() itself is typed `mixed`, so unlike the buggy
     * wrapper there's no non-nullable-string coercion to trip over here.
     */
    private function probeNonBlocking(string $queue): ?QueuedJob
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
     * The bounded-wait half of probe() — a genuine BRPOPLPUSH, blocking
     * for up to $waitSeconds real seconds. Never called with 0: Redis's
     * own blocking primitives treat a literal 0 timeout as "block
     * forever," not "don't block at all" — probe()'s own dispatch routes
     * anything below one second (where that ambiguity would otherwise
     * bite, since these primitives have no fractional timeout either) to
     * probeNonBlocking() instead.
     */
    private function probeBlocking(string $queue, int $waitSeconds): ?QueuedJob
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
     * Every field is read through one of QueueContract's own coercion
     * helpers rather than trusted at a PHPStan-asserted @var shape — a
     * hand-edited or otherwise corrupted JSON payload could carry
     * anything, from a body that isn't even valid JSON to a `class`
     * field that's missing, an `args` field that isn't an array, or an
     * `attempts`/`maxAttempts` value that isn't a clean integer.
     * attempts specifically goes through coerceStoredCompletedAttempts(),
     * not the plain coerceStoredInteger() maxAttempts uses — this stored
     * value is the completed-attempts count (0-indexed) that gets a real
     * `+ 1` right below, and that method is what keeps a stored
     * PHP_INT_MAX from silently overflowing that addition into a float,
     * and also rejects a negative stored count outright — a value that
     * parses cleanly but would otherwise produce a final attempts value
     * below QueuedJob's own 1-indexed floor. maxAttempts is checked for
     * *presence* first — QueueContract::assertFieldPresent() — since
     * encode() below always writes this key, even when its own value is
     * null; only a genuinely missing key is a sign of a truncated
     * envelope, which a plain `?? null` read could never distinguish
     * from a present, legitimately-null value. `metadata` gets the same
     * presence check before its own shape is coerced, and for the same
     * reason: coerceStoredMetadata() reads an absent value as an empty
     * map — the right reading for the backends that write the field only
     * when a caller supplied metadata — so a missing key here would
     * otherwise decode into an accepted job. encode() always writes it,
     * so presence has to be established separately. `id` and `pushedAt`
     * go through envelopeIdentity() below, which checks presence and
     * then the exact shape this encoder writes, before any QueuedJob
     * exists — so an envelope missing or corrupting either one settles
     * here rather than being accepted as a job whose release() would
     * then have no identity to preserve. Every failure here is
     * caught by this class's own probeNonBlocking()/probeBlocking() —
     * see QueueContract::settleIfMalformed() — so a malformed payload
     * settles the already-reserved message rather than crashing the
     * worker.
     */
    private function decodeQueuedJob(string $queue, string $payload): QueuedJob
    {
        $decoded = QueueContract::coerceStoredJsonArray($payload, 'payload');

        self::envelopeIdentity($decoded);

        $class = QueueContract::coerceStoredClass($decoded['class'] ?? null);
        $args = QueueContract::coerceStoredArgs($decoded['args'] ?? null);

        QueueContract::assertFieldPresent($decoded, 'metadata');
        $metadata = QueueContract::coerceStoredMetadata($decoded['metadata']);

        QueueContract::assertFieldPresent($decoded, 'maxAttempts');
        $maxAttempts = QueueContract::coerceStoredMaxAttempts($decoded['maxAttempts'], 'maxAttempts');

        return new QueuedJob(
            $class,
            $args,
            handle: $payload,
            queue: $queue,
            attempts: QueueContract::coerceStoredCompletedAttempts($decoded['attempts'] ?? null, 'attempts') + 1,
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
            QueueContract::coerceStoredJsonArray($oldPayload, 'payload'),
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
     * Shared by settle() (a real QueuedJob's own handle/queue) and the
     * malformed-message settlement path in probeNonBlocking()/
     * probeBlocking() (the raw payload/queue a decode failure was
     * caught for, with no QueuedJob to read them off of) — the same
     * underlying LREM either way, just reached from two different
     * starting shapes. The count is how many entries LREM removed: 1
     * for a live reservation, 0 for a delivery that is already over.
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
     * The two identity fields of a current envelope, validated rather
     * than read optionally: `id` is what keeps two byte-identical jobs
     * from collapsing into one member of the delayed sorted set, and
     * `pushedAt` is the job's original enqueue time, which release()
     * carries across every retry. Both are written by encode() on every
     * push, so an envelope missing or corrupting either is corrupted
     * storage — the same judgment decodeQueuedJob() already makes for
     * `class`/`args`/`attempts`/`maxAttempts`/`metadata`, reached
     * through the same MalformedQueuedJobDataException.
     *
     * Each is checked against the one shape this encoder writes, not the
     * widest shape a value of that kind could take. `id` must match
     * ID_PATTERN: push() writes bin2hex(random_bytes(16)), so a UUID's
     * dashed form, uppercase hex, or a hex run of the wrong length came
     * from somewhere else, and accepting one would mean release()
     * carrying an identity the delayed sorted set's uniqueness never
     * rested on. `pushedAt` must be a native integer as json_decode()
     * returned it, and positive: push() writes time() as a JSON number,
     * so a numeric string, a float, a bool, or a zero/negative timestamp
     * is a value this format never stored.
     * QueueContract::coerceStoredInteger() accepts the numeric-string
     * form on purpose, since other backends keep the same bookkeeping in
     * text columns and headers — which is why the narrower check belongs
     * here rather than there.
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
            throw MalformedQueuedJobDataException::outOfBounds('pushedAt', $pushedAt, 'an enqueue timestamp must be a positive Unix time');
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
