<?php

declare(strict_types=1);

namespace Kinetis\QueueRedis\Tests\Fixtures;

enum Priority: string
{
    case High = 'high';
    case Low = 'low';
}
