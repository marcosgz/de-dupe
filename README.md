# DeDupe

A Ruby gem for distributed deduplication and locking using Redis Sorted Sets. DeDupe provides a simple and efficient way to prevent duplicate execution of tasks across multiple processes or servers, with automatic TTL-based expiration and cleanup.

## Features

- 🔒 **Distributed Locking**: Prevent duplicate execution across multiple processes/servers
- ⏱️ **TTL-based Expiration**: Automatic expiration of locks with configurable time-to-live
- 🧹 **Automatic Cleanup**: Expired entries are automatically removed
- 📦 **Multiple ID Management**: Handle multiple locks/IDs within the same namespace
- 🚀 **Simple API**: Easy-to-use interface with block support
- 🔄 **Redis-backed**: Uses Redis Sorted Sets for efficient storage and querying

## Installation

Add this line to your application's Gemfile:

```ruby
gem "de-dupe"
```

And then execute:

```bash
$ bundle install
```

Or install it yourself as:

```bash
$ gem install de-dupe
```

## Configuration

Configure DeDupe with your Redis connection:

```ruby
require "de-dupe"
require "connection_pool"

DeDupe.configure do |config|
  # Redis connection (supports ConnectionPool or Redis instance)
  config.redis = ConnectionPool.new(size: 10, timeout: 1) do
    Redis.new(url: ENV.fetch("REDIS_URL", "redis://localhost:6379"))
  end

  # Namespace for all keys (default: "de-dupe")
  config.namespace = "my-app"

  # Default TTL in seconds (default: 300 = 5 minutes)
  config.expires_in = 3600 # 1 hour
end
```

### Configuration Options

- `redis`: Redis connection instance or ConnectionPool (required)
- `namespace`: Prefix for all Redis keys (default: `"de-dupe"`)
- `expires_in`: Default TTL in seconds for locks (default: `300`)

## Usage

### Simple Locking with `DeDupe.acquire`

The simplest way to use DeDupe is with the `acquire` method:

```ruby
# Prevent duplicate execution of a long-running job
DeDupe.acquire("import", "user-12345", ttl: 3600) do
  # This block will only execute if the lock can be acquired
  # If another process is already running this, the block won't execute
  import_user_data("user-12345")
end

# With multiple namespace levels
DeDupe.acquire("import", "users", "batch-2024-01-01", ttl: 7200) do
  process_batch("batch-2024-01-01")
end
```

**Note**: The last argument is the lock ID, all previous arguments form the namespace.

### Using the Lock Class

For more control, use the `Lock` class directly:

```ruby
# Create a lock
lock = DeDupe::Lock.new(
  lock_key: "import:users",
  lock_id: "user-12345",
  ttl: 3600 # optional, uses config default if not provided
)

# Check if locked
if lock.locked?
  puts "Already processing"
else
  # Acquire the lock
  if lock.acquire
    begin
      # Do work
      process_user("user-12345")
    ensure
      # Always release
      lock.release
    end
  end
end

# Or use with_lock for automatic release
lock.with_lock do
  # Block only executes if lock can be acquired
  # Lock is automatically released after block execution (even on error)
  process_user("user-12345")
end
```

### Using the Dataset Class

The `Dataset` class allows you to manage multiple IDs within the same namespace:

```ruby
# Create a dataset
dataset = DeDupe::Dataset.new("import:users", ttl: 3600)

# Acquire multiple IDs at once
if dataset.acquire("user-1", "user-2", "user-3")
  puts "All IDs acquired"
end

# Check which IDs are locked
locked = dataset.locked_members("user-1", "user-2", "user-3")
# => ["user-1", "user-2", "user-3"]

# Check which IDs are unlocked
unlocked = dataset.unlocked_members("user-1", "user-2", "user-3")
# => []

# Release specific IDs
dataset.release("user-1", "user-2")

# Check lock status for multiple IDs
if dataset.locked?("user-1", "user-2", "user-3")
  puts "At least one ID is locked"
end

# Flush all entries
dataset.flush

# Manually clean up expired entries
dataset.flush_expired_members
```

## API Reference

### `DeDupe.acquire(*keys, ttl: nil, &block)`

Convenience method for acquiring a lock and executing a block.

- `*keys`: Namespace components (last one becomes the lock_id)
- `ttl`: Optional TTL in seconds (uses config default if not provided)
- `&block`: Block to execute if lock is acquired

**Returns**: Result of the block, or `nil` if lock couldn't be acquired

### `DeDupe::Lock`

#### `new(lock_key:, lock_id:, ttl: nil)`

Create a new lock instance.

- `lock_key`: Namespace/group for the lock
- `lock_id`: Unique identifier within the namespace
- `ttl`: Optional TTL in seconds

#### `acquire` / `lock`

Attempt to acquire the lock. Returns `true` if acquired, `false` if already exists.

#### `release` / `unlock`

Release the lock. Returns `true` if released, `false` if lock didn't exist.

#### `locked?`

Check if the lock is currently active (exists and not expired). Returns `true` or `false`.

#### `with_lock(&block)`

Acquire lock, execute block, and automatically release lock. Block only executes if lock can be acquired.

- Returns block result if lock acquired, `nil` otherwise
- Always releases lock, even if block raises an exception
- Propagates exceptions from the block

### `DeDupe::Dataset`

#### `new(lock_key, ttl: nil)`

Create a new dataset instance.

- `lock_key`: Namespace for the dataset
- `ttl`: Optional TTL in seconds

#### `acquire(*ids)` / `lock(*ids)`

Acquire locks for multiple IDs. Returns `true` if at least one ID was acquired.

#### `release(*ids)` / `unlock(*ids)`

Release locks for multiple IDs. Returns `true` if at least one ID was released.

#### `locked?(*ids)`

Check if any of the given IDs are locked. Returns `true` if at least one is locked.

#### `locked_members(*ids)`

Return array of IDs that are currently locked (not expired).

#### `unlocked_members(*ids)`

Return array of IDs that are unlocked (don't exist or expired).

#### `flush`

Remove all entries from the dataset.

#### `flush_expired_members`

Remove all expired entries from the dataset (automatically called during `acquire`).

## Examples

### Preventing Duplicate Job Execution

```ruby
# In a background job processor
class ImportUserJob
  def perform(user_id)
    DeDupe.acquire("import", "user-#{user_id}", ttl: 3600) do
      # Only one instance of this job will run per user_id
      UserImporter.new(user_id).import
    end
  end
end
```

### Batch Processing with Deduplication

```ruby
# Process a batch, ensuring each item is only processed once
dataset = DeDupe::Dataset.new("batch:process-#{batch_id}", ttl: 7200)

items_to_process.each do |item|
  next if dataset.locked?(item.id) # Skip if already processing

  if dataset.acquire(item.id)
    begin
      process_item(item)
    ensure
      dataset.release(item.id)
    end
  end
end
```

### Rate Limiting with Locking

```ruby
# Ensure only one API call per user per minute
user_id = current_user.id
lock = DeDupe::Lock.new(
  lock_key: "api:rate-limit",
  lock_id: "user-#{user_id}",
  ttl: 60 # 1 minute
)

unless lock.locked?
  lock.with_lock do
    make_api_call(user_id)
  end
else
  puts "Rate limit: Please wait before making another request"
end
```

### Parallel Processing Safety

```ruby
# Ensure only one worker processes a task, even in parallel environments
task_id = "task-12345"

DeDupe.acquire("workers", "process", task_id, ttl: 300) do
  # This will only execute in one worker, even if multiple workers
  # try to process the same task simultaneously
  process_task(task_id)
end
```

## How It Works

DeDupe uses Redis Sorted Sets to store locks with expiration timestamps as scores:

1. **Lock Acquisition**: When you acquire a lock, it's stored in Redis with a score of `current_time + ttl`
2. **Lock Checking**: A lock is considered active if its score (expiration time) is greater than the current time
3. **Automatic Cleanup**: Expired entries (where score <= current_time) are automatically removed
4. **Distributed**: Since it uses Redis, locks work across multiple processes and servers

## Requirements

- Ruby >= 2.7
- Redis server
- `redis` gem
- `zeitwerk` gem

## Development

After checking out the repo, run:

```bash
bundle install
```

Run tests:

```bash
bundle exec rspec
```

Run RuboCop:

```bash
bundle exec rubocop
```

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/marcosgz/de-dupe.

## License

The gem is available as open source under the terms of the [MIT License](LICENSE.txt).
