<?php

declare(strict_types=1);

namespace Kinetis\QueueRedis\Tests\Fixtures;

use Kinetis\Queue\Job;

/**
 * One constructor argument per rich/nested wire shape — used to prove
 * RedisQueue's own real envelope encode()/decodeQueuedJob() round-trips
 * a JobSerializer::serialize()-normalized payload losslessly, floats
 * included (see RedisQueueTest's own docblock for why the float
 * specifically needs proving).
 */
final readonly class RichPayloadJob implements Job
{
    /**
     * @param list<array<string, mixed>> $items
     */
    public function __construct(
        public float $ratio,
        public array $items,
        public Priority $priority,
    ) {}

    public function handle(): void
    {
    }
}
