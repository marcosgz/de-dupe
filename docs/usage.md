# Usage Guide

## Installation

```ruby
# Gemfile
gem 'de-dupe'
gem 'redis'
gem 'connection_pool'  # optional but recommended for production
```

## Configuration

```ruby
# config/initializers/de_dupe.rb
DeDupe.configure do |config|
  config.redis      = ConnectionPool.new(size: 10, timeout: 1) { Redis.new(url: ENV['REDIS_URL']) }
  config.namespace  = 'myapp'   # default: "de-dupe"
  config.expires_in = 3600      # default: 300 seconds
end
```

Both a bare `Redis.new` and a `ConnectionPool.new { Redis.new }` are accepted. A pool is recommended for multi-threaded servers.

Calling `DeDupe.configure` again resets the internal Redis pool — safe to call from a Rails reloader.

## Pattern: prevent duplicate job execution

```ruby
class UserImportJob
  def perform(user_id)
    DeDupe.acquire('import', 'users', user_id, ttl: 3600) do
      User.import(user_id)
    end
  end
end
```

If the same `user_id` is enqueued twice before the first run finishes, only one execution does real work; the second call returns `nil` without running the block.

## Pattern: rate limiting

```ruby
lock = DeDupe::Lock.new(lock_key: 'api:rate-limit', lock_id: "user-#{user_id}", ttl: 60)

if lock.locked?
  raise 'Rate limited; try again in a minute'
end

lock.with_lock { call_external_api(user_id) }
```

## Pattern: batch deduplication

`Dataset` manages many lock ids under a single namespace — a natural fit for filtering an input list down to items that aren't already being processed.

```ruby
dataset = DeDupe::Dataset.new('batch:nightly-refresh', ttl: 7200)

# Acquire locks for a batch of ids atomically.
dataset.acquire(*ids_to_process)

# Enumerate everything still held under this namespace:
dataset.members.to_a  # => ["id-1", "id-2", ...]

# Split an input list into "already in flight" vs "free to start":
busy = dataset.locked_members(*candidate_ids)
free = dataset.unlocked_members(*candidate_ids)
```

`Dataset#acquire` flushes expired entries before adding, which keeps the set bounded even if callers don't explicitly release.

## Manual lock lifecycle

```ruby
lock = DeDupe::Lock.new(lock_key: 'jobs', lock_id: 'job-42', ttl: 60)

if lock.acquire
  begin
    do_work
  ensure
    lock.release
  end
end
```

`with_lock` is the same thing with guaranteed release on exception:

```ruby
lock.with_lock { do_work }
```

## Inspecting the dataset

```ruby
dataset = DeDupe::Dataset.new('workers:tasks')

dataset.size                     # count of non-expired members
dataset.members { |id| puts id } # iterate non-expired members
dataset.flush_expired_members    # prune TTL'd entries now
dataset.flush                    # remove everything under this key
```

## Namespaced keys

Keys are built as `<namespace>:<part>:<part>:...`:

```ruby
DeDupe::LockKey.new('import', 'users', 'chunk-3').to_s
# => "myapp:import:users:chunk-3"
```

`DeDupe.acquire(*keys, ...)` treats all but the last arg as key parts and the last arg as the lock id:

```ruby
DeDupe.acquire('import', 'users', user_id) { ... }
# lock_key = "myapp:import:users"
# lock_id  = user_id
```

## Cleanup and introspection

```ruby
DeDupe.keys.to_a    # enumerate every key under the namespace
DeDupe.flush_all    # drop every key under the namespace — use carefully
```

## Choosing a TTL

Pick a TTL comfortably longer than your worst-case work duration. TTL is a safety net, not a timeout: it ensures a crashed process doesn't leave a lock wedged forever, but it shouldn't fire during normal operation.

If a job typically takes 2 minutes and occasionally 5, a 10–15 minute TTL is a reasonable default. If you can't bound the duration, consider heartbeating by periodically re-acquiring with the same id — each acquire extends the score.
