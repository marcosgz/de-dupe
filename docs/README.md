# de-dupe

Distributed deduplication and locking for Ruby, backed by Redis Sorted Sets.

Use it to prevent duplicate work across processes or servers: the same job running twice, the same message being handled twice, the same API call being made twice. Expired lock entries are cleaned up automatically — no janitor job required.

## Contents

- [Usage guide](usage.md) — common patterns (job dedup, batch processing, rate limiting)
- [API reference](api.md) — `DeDupe`, `Lock`, `Dataset`, `Config`, `LockKey`

## Install

```ruby
# Gemfile
gem 'de-dupe'
gem 'redis'
gem 'connection_pool' # optional but recommended
```

## One-minute tour

```ruby
require 'de-dupe'
require 'connection_pool'

DeDupe.configure do |config|
  config.redis       = ConnectionPool.new(size: 10) { Redis.new }
  config.namespace   = 'myapp'
  config.expires_in  = 300  # seconds
end

# Acquire a lock scoped by a hierarchical key, run a block, auto-release.
DeDupe.acquire('import', 'users', user_id) do
  User.import(user_id)
end
```

- Namespace keys: `myapp:import:users` → lock id = `user_id`.
- If the block completes, the lock is released. If it raises, the lock is released.
- If another process is already holding the lock, the block is not executed and `acquire` returns `nil`.

## How it works

Each lock is a member of a Redis Sorted Set. The member is the lock id; the score is an expiration timestamp (`now + ttl`). Acquisition uses `ZADD NX`. "Is it locked?" means "does the member exist with score > now?" TTL is enforced by `ZREMRANGEBYSCORE('-inf', '(now')` at acquisition time.

The design scales to tens of thousands of active locks per namespace with sub-millisecond acquisition latency.

## Version

- Version: **0.0.2**
- Ruby: `>= 2.7`
- Depends on: `redis`, `zeitwerk`

## License

MIT.
