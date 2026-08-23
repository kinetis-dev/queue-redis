<?php

declare(strict_types=1);

/**
 * One racing worker for redis_delayed_race.php — a genuinely separate OS
 * process, not a Fiber within the orchestrator's own process, since the
 * actual bug being proven fixed (every delayed job running once per
 * polling worker) only reproduces across real concurrent processes each
 * running their own promoteDelayedJobs() call. Pops for a bounded window
 * and prints one line per job it actually received, so the orchestrator
 * can check for duplicates across every worker's output combined.
 */

require __DIR__ . '/../vendor/autoload.php';

use Kinetis\QueueRedis\RedisQueue;

use function Amp\Redis\createRedisClient;

$redis = createRedisClient('redis://' . ($argv[1] ?? '127.0.0.1') . ':6379');
$queue = new RedisQueue($redis);

$deadline = microtime(true) + 6.0;

while (microtime(true) < $deadline) {
    $job = $queue->pop(timeoutSeconds: 1);

    if ($job === null) {
        continue;
    }

    echo $job->args['sequence'] . "\n";
    $queue->ack($job);
}
