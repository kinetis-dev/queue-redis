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
 * One pop() puts several scripts on the wire under the same `evalsha`
 * command name, so a reply can also be scripted as a sequence consumed
 * in order. A sequence that runs out falls back to the command's fixed
 * reply, which is what lets a test script only the round trips it cares
 * about ordering.
 *
 * A command can also be given a duration, for a test whose subject is
 * how long a call takes rather than what it answers.
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
     * @param array<string, list<int|string|list<mixed>|null>> $sequences
     *     replies consumed one per call, keyed the same way, before
     *     $replies applies
     */
    public function __construct(
        private readonly array $replies = [],
        private readonly array $durations = [],
        private array $sequences = [],
    ) {}

    #[\Override]
    public function execute(string $command, array $parameters): RedisResponse
    {
        $this->commands[] = [$command, array_values($parameters)];

        if (isset($this->durations[$command])) {
            usleep($this->durations[$command]);
        }

        if (($this->sequences[$command] ?? []) !== []) {
            return new RedisValue(array_shift($this->sequences[$command]));
        }

        if (!\array_key_exists($command, $this->replies)) {
            throw new RuntimeException("No reply scripted for the \"{$command}\" command.");
        }

        return new RedisValue($this->replies[$command]);
    }
}
