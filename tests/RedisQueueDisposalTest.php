<?php

declare(strict_types=1);

namespace Kinetis\QueueRedis\Tests;

use Amp\Redis\RedisClient;
use Closure;
use Kinetis\Config\Config;
use Kinetis\Container\AppScope;
use Kinetis\Queue\DisposableQueueInterface;
use Kinetis\Queue\PackageBootstrap;
use Kinetis\Queue\QueueInterface;
use Kinetis\QueueRedis\RedisQueue;
use Kinetis\QueueRedis\RedisQueueFactory;
use Kinetis\QueueRedis\Tests\Fixtures\ScriptedRedisLink;
use Kinetis\Redis\Client;
use PHPUnit\Framework\TestCase;
use ReflectionFunction;
use ReflectionProperty;

/**
 * Who closes the connection, and when. `Amp\Redis\RedisClient` is a
 * command facade with no close of its own, so the transport this queue
 * can own is the {@see Client} its link came from — the object the
 * factory keeps hold of for exactly that reason. Nothing here connects:
 * a Client opens its socket on the first command, so the whole file
 * runs with no Redis reachable, which is also what makes it the check
 * that disposal is safe before the queue's first command.
 */
final class RedisQueueDisposalTest extends TestCase
{
    public function test_the_factory_hands_the_queue_the_client_it_created(): void
    {
        $queue = RedisQueueFactory::fromConfig(new Config(['REDIS_HOST' => 'localhost']));
        $client = self::ownedClient($queue);
        $link = new ReflectionProperty(Client::class, 'link');

        self::assertNotNull($link->getValue($client), 'the factory built the link the queue runs commands through');

        $queue->dispose();

        self::assertNull($link->getValue($client), 'disposal closed the client the factory created');
    }

    public function test_disposing_twice_closes_the_factory_client_once(): void
    {
        $queue = RedisQueueFactory::fromConfig(new Config(['REDIS_URL' => 'redis://localhost:6379/0']));
        $client = self::ownedClient($queue);

        $queue->dispose();
        $queue->dispose();

        self::assertNull(new ReflectionProperty(Client::class, 'link')->getValue($client));
    }

    /**
     * A client the caller built stays the caller's — the queue was lent
     * a command facade, not given a connection to close, so disposal
     * puts nothing on the wire and ends nothing.
     */
    public function test_a_directly_constructed_queue_owns_no_client(): void
    {
        $link = new ScriptedRedisLink();
        $queue = new RedisQueue(new RedisClient($link));

        self::assertNull(new ReflectionProperty(RedisQueue::class, 'disposer')->getValue($queue));

        $queue->dispose();

        self::assertSame([], $link->commands);
    }

    public function test_a_caller_can_hand_the_queue_a_transport_to_own(): void
    {
        $closes = 0;
        $queue = new RedisQueue(new RedisClient(new ScriptedRedisLink()), 300, static function () use (&$closes): void {
            ++$closes;
        });

        $queue->dispose();
        $queue->dispose();

        self::assertSame(1, $closes);
    }

    /**
     * End to end through the binding an application actually gets:
     * nothing is registered until something injects the queue, and the
     * worker's own teardown is what closes the connection the binding
     * opened.
     */
    public function test_the_package_bootstrap_closes_the_queue_it_built_when_the_worker_ends(): void
    {
        $app = new AppScope();
        new PackageBootstrap()->register($app, new Config([
            'QUEUE_CONNECTION' => 'redis',
            'REDIS_HOST' => 'localhost',
        ]));
        $app->boot();

        self::assertSame([], self::disposeCallbacks($app));

        $queue = $app->get(QueueInterface::class);
        self::assertInstanceOf(RedisQueue::class, $queue);
        self::assertInstanceOf(DisposableQueueInterface::class, $queue);
        self::assertCount(1, self::disposeCallbacks($app), 'one close for the one backend built');

        $client = self::ownedClient($queue);
        $app->dispose();

        self::assertNull(new ReflectionProperty(Client::class, 'link')->getValue($client));
    }

    /**
     * The transport the queue was handed to close, read back off the
     * disposer itself: a factory that dropped the Client would leave a
     * connection nothing can reach, and that is the shape this reads.
     */
    private static function ownedClient(RedisQueue $queue): Client
    {
        $disposer = new ReflectionProperty(RedisQueue::class, 'disposer')->getValue($queue);
        self::assertInstanceOf(Closure::class, $disposer);

        $client = new ReflectionFunction($disposer)->getClosureThis();
        self::assertInstanceOf(Client::class, $client);

        return $client;
    }

    /**
     * @return list<callable(): void>
     */
    private static function disposeCallbacks(AppScope $app): array
    {
        /** @var list<callable(): void> */
        return new ReflectionProperty(AppScope::class, 'disposeCallbacks')->getValue($app);
    }
}
