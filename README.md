<p align="center">
  <img src="logo.svg" alt="Kinetis" width="420">
</p>

<p align="center">
  <strong>kinetis/queue-redis</strong>
  <br>
  <strong>A Redis-backed queue implementation for kinetis/queue's <code>QueueInterface</code></strong>
</p>

<p align="center">
  <a href="https://packagist.org/packages/kinetis/queue-redis"><img src="https://img.shields.io/packagist/v/kinetis/queue-redis?label=version" alt="Packagist Version"></a>
  <a href="https://packagist.org/packages/kinetis/queue-redis"><img src="https://img.shields.io/packagist/dt/kinetis/queue-redis" alt="Packagist Downloads"></a>
  <a href="https://packagist.org/packages/kinetis/queue-redis"><img src="https://img.shields.io/packagist/php-v/kinetis/queue-redis" alt="PHP Version"></a>
  <a href="https://packagist.org/packages/kinetis/queue-redis"><img src="https://img.shields.io/packagist/l/kinetis/queue-redis" alt="License"></a>
  <a href="https://github.com/kinetis-dev/kinetis/actions/workflows/ci.yml"><img src="https://github.com/kinetis-dev/kinetis/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
</p>

---

Part of [Kinetis](https://kinetis.dev/), a non-blocking PHP framework for
API-first applications, developed in the
[kinetis-dev/kinetis](https://github.com/kinetis-dev/kinetis) monorepo.

Adds Redis as a queue backend. `push()`/`pop()`/`ack()`/`release()`/`fail()`
work exactly like any other backend — only your configuration changes. A
reservation is a finite lease: `pop()` moves a job from the queue's
`pending` list into a `leased` sorted set scored with an expiry, and any
worker's next `pop()` returns a lease past its expiry to `pending` with
`attempts` incremented. A job whose worker died mid-execution is
redelivered rather than stranded.

```php
use Kinetis\Config\Config;
use Kinetis\QueueRedis\RedisQueueFactory;

$queue = RedisQueueFactory::fromConfig($config);

$queue->push(new SendWelcomeEmail($email, $name), queue: 'default');
```

`RedisQueue` declares `Kinetis\Queue\ClearableQueueInterface`.
Clearing counts and removes the queue's pending and delayed entries in
one Lua script, so the number it reports is what it removed; live leases
are untouched, since they are work a running worker still owns.
`size()` counts pending, delayed and expired leases, not live ones.

The leased member is the exact envelope string handed back as the job's
handle, and reclaiming rewrites it with the incremented attempt count.
That makes the handle a fence: `ack()`, `release()` and `fail()` act only
on that exact member, so a settlement for a delivery that has been
reclaimed or already settled raises
`Kinetis\Queue\Exception\StaleJobHandleException` and writes nothing.

Recovery is not renewal. A job still running when its lease expires can
execute alongside its replacement, so set
`QUEUE_VISIBILITY_TIMEOUT_SECONDS` above normal job duration and keep
handlers idempotent. `maxAttempts` bounds handlers that throw; it cannot
bound a succession of processes that each die mid-execution.

There is no reaper process. Every `pop()` promotes due delayed jobs and
reclaims expired leases for each queue it is given, in priority order,
before it waits.

## Configuration

```
QUEUE_CONNECTION=redis
REDIS_HOST=127.0.0.1
QUEUE_VISIBILITY_TIMEOUT_SECONDS=300
```

`QUEUE_VISIBILITY_TIMEOUT_SECONDS` is how long a reservation is leased
before any worker may reclaim it. It defaults to 300 and must be a
positive integer. Every other key this backend reads — `REDIS_HOST`/
`REDIS_URL`/`REDIS_TLS`/... — is the exact one [`kinetis/cache-redis`](https://github.com/kinetis-dev/cache-redis)'s
`RedisSimpleCache` already reads, scoped by `QUEUE_CONNECTION_NAME` the
same way every other backend is. `REDIS_CLUSTER` is not among them: this
backend is single-node, and it opens its own connection over
[`kinetis/redis`](https://github.com/kinetis-dev/redis) rather than
sharing the cache's, so its connection lifetime is its own. [`kinetis/queue`](https://github.com/kinetis-dev/queue)'s own keys
(`QUEUE_CONNECTION`, `QUEUE_MAX_ATTEMPTS`, ...) are documented in that
package; full reference:
[kinetis.dev/docs/config.html](https://kinetis.dev/docs/config.html).

## Installation

```sh
composer require kinetis/queue-redis
```

Requires PHP 8.4+, [`kinetis/framework`](https://github.com/kinetis-dev/framework), [`kinetis/queue`](https://github.com/kinetis-dev/queue), and
[`kinetis/redis`](https://github.com/kinetis-dev/redis). Full documentation:
[kinetis.dev/docs/queue-redis.html](https://kinetis.dev/docs/queue-redis.html).

## License

MIT — see [LICENSE](LICENSE).
