<?php

declare(strict_types=1);

namespace Kinetis\QueueRedis\Tests;

use InvalidArgumentException;
use Kinetis\Config\Config;
use Kinetis\QueueRedis\RedisQueue;
use Kinetis\QueueRedis\RedisQueueFactory;
use PHPUnit\Framework\TestCase;

/**
 * Construction only — createRedisClient() never connects eagerly (the
 * same fact kinetis/cache-redis's own RedisSimpleCacheTest relies on),
 * so this is safe to run with no real Redis server reachable.
 * RedisQueue's own backend-specific correctness (the reliable-queue
 * ack/release mechanics, priority-queue cycling) is deliberately never
 * unit-tested against a fake — see tests-integration/.
 */
final class RedisQueueFactoryTest extends TestCase
{
    public function test_builds_a_queue_for_the_default_connection(): void
    {
        $config = new Config(['REDIS_HOST' => 'localhost']);

        self::assertInstanceOf(RedisQueue::class, RedisQueueFactory::fromConfig($config));
    }

    public function test_a_named_connection_reads_its_own_host(): void
    {
        $config = new Config(['REDIS_CACHE2_HOST' => 'localhost']);

        self::assertInstanceOf(RedisQueue::class, RedisQueueFactory::fromConfig($config, 'cache2'));
    }

    public function test_neither_url_nor_host_configured_throws_a_clear_error(): void
    {
        $config = new Config([]);

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('REDIS_URL or REDIS_HOST must be set when QUEUE_CONNECTION=redis.');
        RedisQueueFactory::fromConfig($config);
    }
}
