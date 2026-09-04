<?php

declare(strict_types=1);

namespace Kinetis\QueueRedis;

use InvalidArgumentException;
use Kinetis\Config\Config;
use Kinetis\Queue\ClearableQueueInterface;
use Kinetis\SimpleCache\RedisSimpleCache;

use function Amp\Redis\createRedisClient;

/**
 * Builds the Redis queue backend `QUEUE_CONNECTION=redis` selects —
 * called by `kinetis/queue`'s own `QueueFactory::fromConfig()`, gated
 * behind a `class_exists()` check so core never depends on this package
 * directly, the same pattern used for every other optional queue
 * backend (`kinetis/queue-sqs`, `kinetis/queue-rabbitmq`).
 *
 * Returns `ClearableQueueInterface`, the capability this backend
 * declares; see `QueueFactory` for why the connection-driven factory
 * stays on `QueueInterface`.
 */
final class RedisQueueFactory
{
    public static function fromConfig(Config $config, string $connectionName = 'default'): ClearableQueueInterface
    {
        $redisConfig = RedisSimpleCache::buildRedisConfig($config, $connectionName);

        if ($redisConfig === null) {
            throw new InvalidArgumentException('REDIS_URL or REDIS_HOST must be set when QUEUE_CONNECTION=redis.');
        }

        return new RedisQueue(createRedisClient($redisConfig));
    }
}
