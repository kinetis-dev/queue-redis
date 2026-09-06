<?php

declare(strict_types=1);

namespace Kinetis\QueueRedis;

use Amp\Redis\RedisClient;
use Amp\Socket\ClientTlsContext;
use InvalidArgumentException;
use Kinetis\Config\Config;
use Kinetis\Queue\ClearableQueueInterface;
use Kinetis\Redis\Client;
use Kinetis\Redis\ClientOptions;
use Kinetis\Redis\ConnectionUri;
use Kinetis\Redis\Endpoint;

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
 *
 * The queue owns its own physical connection. A `BRPOPLPUSH` parks on
 * the socket for up to a second, and a connection shared with the cache
 * would stall every pipelined neighbour behind it for that whole second.
 */
final class RedisQueueFactory
{
    /**
     * The queue's operation budget is the configured transport allowance
     * plus the longest blocking wait `RedisQueue` asks Redis for, so a
     * `BRPOPLPUSH` that legitimately waits out its whole timeout is not
     * mistaken for a lost reply.
     */
    private const float BLOCKING_WAIT_ALLOWANCE_SECONDS = 1.0;

    public static function fromConfig(Config $config, string $connectionName = 'default'): ClearableQueueInterface
    {
        $options = self::options($config, $connectionName);
        $url = $config->string(Config::scopedKey('REDIS_URL', $connectionName), '');

        if ($url !== '') {
            $parsed = ConnectionUri::parse($url);
            $client = Client::create(
                $parsed->endpoint,
                $options->withPassword($parsed->password ?? $options->password)->withDatabase($parsed->database),
            );

            return new RedisQueue(new RedisClient($client->link()));
        }

        $host = $config->string(Config::scopedKey('REDIS_HOST', $connectionName), '');

        if ($host === '') {
            throw new InvalidArgumentException('REDIS_URL or REDIS_HOST must be set when QUEUE_CONNECTION=redis.');
        }

        $client = Client::create(
            Endpoint::fromParts($host, $config->int(Config::scopedKey('REDIS_PORT', $connectionName), 6379)),
            $options->withDatabase($config->int(Config::scopedKey('REDIS_DATABASE', $connectionName), 0)),
        );

        return new RedisQueue(new RedisClient($client->link()));
    }

    private static function options(Config $config, string $connectionName): ClientOptions
    {
        $timeoutKey = Config::scopedKey('REDIS_TIMEOUT', $connectionName);
        $timeout = $config->float($timeoutKey, 5.0);

        if ($timeout <= 0.0) {
            throw new InvalidArgumentException("{$timeoutKey} must be a positive number of seconds, got {$timeout}.");
        }

        return new ClientOptions(
            $timeout + self::BLOCKING_WAIT_ALLOWANCE_SECONDS,
            $config->get(Config::scopedKey('REDIS_PASSWORD', $connectionName)),
            tls: self::tls($config, $connectionName),
        );
    }

    private static function tls(Config $config, string $connectionName): ?ClientTlsContext
    {
        if (!$config->bool(Config::scopedKey('REDIS_TLS', $connectionName), false)) {
            return null;
        }

        $context = new ClientTlsContext('');

        if (!$config->bool(Config::scopedKey('REDIS_TLS_VERIFY_PEER', $connectionName), true)) {
            $context = $context->withoutPeerVerification();
        }

        $caFile = $config->get(Config::scopedKey('REDIS_TLS_CA_FILE', $connectionName));

        return $caFile !== null ? $context->withCaFile($caFile) : $context;
    }
}
