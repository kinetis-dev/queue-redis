<?php

declare(strict_types=1);

/**
 * Permanent regression coverage for the delayed-job duplication bug fixed
 * in RedisQueue::promoteDelayedJobs() (every delayed job used to run once
 * per polling worker, because ZREM's return value was ignored). Pushes a
 * batch of delayed jobs, then races several genuinely separate OS
 * processes (proc_open(), not Fibers — the bug only reproduces across
 * real concurrent processes) popping them concurrently, and checks that
 * every job was delivered exactly once in total, not once per worker.
 */

require __DIR__ . '/../vendor/autoload.php';

use Kinetis\Queue\Job;
use Kinetis\QueueRedis\RedisQueue;

use function Amp\Redis\createRedisClient;

final readonly class DelayedRaceJob implements Job
{
    public function __construct(
        public int $sequence,
    ) {}

    public function handle(): void
    {
    }
}

function check(string $label, bool $condition): void
{
    echo ($condition ? "OK   " : "FAIL ") . $label . "\n";

    if (!$condition) {
        exit(1);
    }
}

const JOB_COUNT = 30;
const WORKER_COUNT = 10;
const DELAY_SECONDS = 2;

$redisHost = getenv('REDIS_HOST') ?: '127.0.0.1';
$redis = createRedisClient("redis://{$redisHost}:6379");
$queue = new RedisQueue($redis);

// Fresh queue/delayed-set state for this run.
for ($i = 1; $i <= JOB_COUNT; $i++) {
    $queue->push(new DelayedRaceJob($i), delaySeconds: DELAY_SECONDS);
}

$workerScript = __DIR__ . '/redis_delayed_race_worker.php';
$processes = [];
$pipes = [];

for ($i = 0; $i < WORKER_COUNT; $i++) {
    $process = proc_open(
        ['php', $workerScript, $redisHost],
        [1 => ['pipe', 'w'], 2 => ['pipe', 'w']],
        $pipeHandles,
    );

    if ($process === false) {
        check("worker {$i} spawned", false);
    }

    $processes[] = $process;
    $pipes[] = $pipeHandles;
}

$delivered = [];

foreach ($processes as $index => $process) {
    $stdout = stream_get_contents($pipes[$index][1]);
    $stderr = stream_get_contents($pipes[$index][2]);
    fclose($pipes[$index][1]);
    fclose($pipes[$index][2]);
    $exitCode = proc_close($process);

    check("worker {$index} exited cleanly (stderr: " . trim($stderr) . ')', $exitCode === 0);

    foreach (array_filter(explode("\n", trim($stdout))) as $line) {
        $delivered[] = (int) $line;
    }
}

$counts = array_count_values($delivered);
$duplicated = array_filter($counts, static fn (int $n): bool => $n > 1);
$missing = array_diff(range(1, JOB_COUNT), array_keys($counts));

check('every job delivered exactly once total (not once per worker)', $duplicated === []);
check('no job missing', $missing === []);
check('total delivered count matches job count', count($delivered) === JOB_COUNT);

echo "ALL CHECKS PASSED\n";
