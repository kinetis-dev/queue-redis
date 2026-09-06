<?php

declare(strict_types=1);

namespace Kinetis\QueueRedis\Tests\Fixtures;

use Amp\Redis\Connection\RedisLink;
use Amp\Redis\Protocol\RedisResponse;
use Amp\Redis\Protocol\RedisValue;
use RuntimeException;

/**
 * The one seam a RedisClient is built over — Amp\Redis\RedisClient takes
 * a RedisLink and routes every command through it — so the exact command
 * an operation puts on the wire, and the reply it reads back, are both
 * under a test's control with no server involved.
 *
 * Narrow on purpose: it answers the commands a test scripts and refuses
 * anything else, so an operation that started issuing some other command
 * fails here rather than silently reading a default.
 *
 * A command can also be given a duration. Redis's blocking primitives
 * are the one place where the time a command takes is part of what a
 * caller has to get right, and a reply that arrives instantly cannot
 * exercise a deadline that the wait itself consumed.
 */
final class ScriptedRedisLink implements RedisLink
{
    /** @var list<array{string, list<int|float|string>}> */
    public array $commands = [];

    /**
     * @param array<string, int|string|list<mixed>|null> $replies keyed by
     *     lowercase command name, the form RedisClient sends
     * @param array<string, int> $durations microseconds a command takes
     *     to answer, keyed the same way; absent means answering at once
     */
    public function __construct(
        private readonly array $replies = [],
        private readonly array $durations = [],
    ) {}

    #[\Override]
    public function execute(string $command, array $parameters): RedisResponse
    {
        $this->commands[] = [$command, array_values($parameters)];

        if (!\array_key_exists($command, $this->replies)) {
            throw new RuntimeException("No reply scripted for the \"{$command}\" command.");
        }

        if (isset($this->durations[$command])) {
            usleep($this->durations[$command]);
        }

        return new RedisValue($this->replies[$command]);
    }
}
