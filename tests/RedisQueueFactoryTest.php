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

    public function test_a_cluster_connection_with_a_host_is_rejected(): void
    {
        $config = new Config(['REDIS_CLUSTER' => 'true', 'REDIS_HOST' => 'localhost']);

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            'REDIS_CLUSTER enables Redis Cluster, but kinetis/queue-redis supports standalone Redis only.',
        );
        RedisQueueFactory::fromConfig($config);
    }

    public function test_a_named_cluster_connection_with_a_url_is_rejected_by_its_own_key(): void
    {
        $config = new Config(['REDIS_FAST_CLUSTER' => 'true', 'REDIS_FAST_URL' => 'redis://localhost:6379/0']);

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            'REDIS_FAST_CLUSTER enables Redis Cluster, but kinetis/queue-redis supports standalone Redis only.',
        );
        RedisQueueFactory::fromConfig($config, 'fast');
    }

    public function test_an_explicit_false_cluster_setting_still_builds(): void
    {
        $config = new Config(['REDIS_CLUSTER' => 'false', 'REDIS_HOST' => 'localhost']);

        self::assertInstanceOf(RedisQueue::class, RedisQueueFactory::fromConfig($config));
    }

    /**
     * The cache may run on a cluster through the unscoped keys while the
     * queue's named connection points at a standalone server.
     */
    public function test_a_default_cluster_setting_does_not_reject_a_named_standalone_connection(): void
    {
        $config = new Config([
            'REDIS_CLUSTER' => 'true',
            'REDIS_CLUSTER_SEEDS' => 'node1:7001',
            'REDIS_FAST_HOST' => 'localhost',
        ]);

        self::assertInstanceOf(RedisQueue::class, RedisQueueFactory::fromConfig($config, 'fast'));
    }

    public function test_neither_url_nor_host_configured_throws_a_clear_error(): void
    {
        $config = new Config([]);

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('REDIS_URL or REDIS_HOST must be set for a Redis queue connection.');
        RedisQueueFactory::fromConfig($config);
    }

    /**
     * The unscoped keys do not configure a named connection, so the
     * failure names the scoped keys that were read.
     */
    public function test_a_named_connection_without_url_or_host_names_its_scoped_keys(): void
    {
        $config = new Config(['REDIS_HOST' => 'localhost']);

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('REDIS_JOBS_URL or REDIS_JOBS_HOST must be set for a Redis queue connection.');
        RedisQueueFactory::fromConfig($config, 'jobs');
    }
}
