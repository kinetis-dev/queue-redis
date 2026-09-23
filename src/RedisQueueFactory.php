<?php

declare(strict_types=1);

namespace Kinetis\QueueRedis;

use Amp\Redis\RedisClient;
use Amp\Socket\ClientTlsContext;
use InvalidArgumentException;
use Kinetis\Config\Config;
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
 * Returns the concrete `RedisQueue`, which declares both the clearing
 * and the disposal capability; see `QueueFactory` for why the
 * connection-driven factory stays on `QueueInterface`.
 *
 * The queue builds its own physical connection from the `REDIS_*`
 * settings rather than sharing the cache's, so its operation budget and
 * connection lifetime are its own. That connection is the queue's to
 * close: the `Kinetis\Redis\Client` opened here is kept and its
 * close() handed over as the queue's disposer, since the
 * `Amp\Redis\RedisClient` facade the queue runs commands through
 * exposes none. A caller binding this result itself registers
 * `$queue->dispose(...)` on the scope it binds into;
 * `Kinetis\Queue\PackageBootstrap` does that for the queue
 * `QUEUE_CONNECTION=redis` builds.
 */
final class RedisQueueFactory
{
    /**
     * Long enough that an ordinary job settles well inside it, short
     * enough that a dead worker's job is redelivered without operator
     * action. See `RedisQueue` for what expiry means for a job still
     * running.
     */
    private const int DEFAULT_VISIBILITY_TIMEOUT_SECONDS = 300;

    /**
     * A queue script names several keys with no shared hash tag, which a
     * cluster would reject or split across nodes, so the connection's own
     * `REDIS_CLUSTER` is refused before any client is built.
     */
    public static function fromConfig(Config $config, string $connectionName = 'default'): RedisQueue
    {
        $clusterKey = Config::scopedKey('REDIS_CLUSTER', $connectionName);

        if ($config->bool($clusterKey, false)) {
            throw new InvalidArgumentException(
                "{$clusterKey} enables Redis Cluster, but kinetis/queue-redis supports standalone Redis only.",
            );
        }

        $options = self::options($config, $connectionName);
        $visibilityTimeout = self::visibilityTimeoutSeconds($config, $connectionName);
        $url = $config->string(Config::scopedKey('REDIS_URL', $connectionName), '');

        if ($url !== '') {
            $parsed = ConnectionUri::parse($url);
            $client = Client::create(
                $parsed->endpoint,
                $options->withPassword($parsed->password ?? $options->password)->withDatabase($parsed->database),
            );

            return new RedisQueue(new RedisClient($client->link()), $visibilityTimeout, $client->close(...));
        }

        $host = $config->string(Config::scopedKey('REDIS_HOST', $connectionName), '');

        if ($host === '') {
            throw new InvalidArgumentException('REDIS_URL or REDIS_HOST must be set when QUEUE_CONNECTION=redis.');
        }

        $client = Client::create(
            Endpoint::fromParts($host, $config->int(Config::scopedKey('REDIS_PORT', $connectionName), 6379)),
            $options->withDatabase($config->int(Config::scopedKey('REDIS_DATABASE', $connectionName), 0)),
        );

        return new RedisQueue(new RedisClient($client->link()), $visibilityTimeout, $client->close(...));
    }

    /**
     * A reservation lease is finite, so this setting has a real default
     * rather than an "off" state: an absent value still recovers a
     * crashed worker's job.
     */
    private static function visibilityTimeoutSeconds(Config $config, string $connectionName): int
    {
        $key = Config::scopedKey('QUEUE_VISIBILITY_TIMEOUT_SECONDS', $connectionName);
        $seconds = $config->int($key, self::DEFAULT_VISIBILITY_TIMEOUT_SECONDS);

        if ($seconds < 1) {
            throw new InvalidArgumentException("{$key} must be a positive number of seconds, got {$seconds}.");
        }

        return $seconds;
    }

    private static function options(Config $config, string $connectionName): ClientOptions
    {
        $timeoutKey = Config::scopedKey('REDIS_TIMEOUT', $connectionName);
        $timeout = $config->float($timeoutKey, 5.0);

        if ($timeout <= 0.0) {
            throw new InvalidArgumentException("{$timeoutKey} must be a positive number of seconds, got {$timeout}.");
        }

        return new ClientOptions(
            $timeout,
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
