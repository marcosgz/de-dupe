# API Reference

## `DeDupe`

Top-level module.

### Configuration

| Method | Description |
|--------|-------------|
| `DeDupe.configure { |config| ... }` | DSL entry point. Yields a `DeDupe::Config`. Resets the internal Redis pool. |
| `DeDupe.config` | Returns the current `DeDupe::Config`. |

### Lock operations

| Method | Description |
|--------|-------------|
| `DeDupe.acquire(*keys, ttl: nil) { \|\| ... }` | Build a lock from `keys` (last arg is lock id, rest join into the lock key) and yield the block if acquired. Returns the block's value, or `nil` if not acquired. |
| `DeDupe.redis_pool` | Returns the wrapped `DeDupe::RedisPool`. |

### Maintenance

| Method | Description |
|--------|-------------|
| `DeDupe.keys` | Enumerator of every Redis key under the configured namespace. |
| `DeDupe.flush_all` | Delete every key under the namespace. Returns the count deleted. |

---

## `DeDupe::Config`

Container for configuration.

| Attribute | Type | Default | Purpose |
|-----------|------|---------|---------|
| `redis` | `Redis` or `ConnectionPool` | — (required) | Redis connection or pool |
| `namespace` | `String` | `"de-dupe"` | Prefix for every Redis key |
| `expires_in` | `Integer` (seconds) | `300` | Default TTL for locks |

---

## `DeDupe::Lock`

A single lock — one (key, id) pair with a TTL.

### Constructor

```ruby
DeDupe::Lock.new(lock_key:, lock_id:, ttl: nil)
```

- `lock_key` — string; full Redis key (typically built via `LockKey`).
- `lock_id` — string; member within the sorted set.
- `ttl` — seconds; falls back to `DeDupe.config.expires_in`.

### Methods

| Method | Returns | Description |
|--------|---------|-------------|
| `#acquire` / `#lock` | `Boolean` | `ZADD NX` with `now + ttl`. Returns `true` if newly acquired. |
| `#release` / `#unlock` | `Boolean` | `ZREM` the member. Returns `true` if a member was removed. |
| `#locked?` | `Boolean` | `true` if member exists with score > now. |
| `#with_lock { \|\| ... }` | block's value or `nil` | Acquire, yield, release on exit (including exceptions). |

---

## `DeDupe::Dataset`

A namespace that manages many lock ids under one key. Useful for batch workflows where you want to filter "which items are already being processed?"

### Constructor

```ruby
DeDupe::Dataset.new(lock_key, ttl: nil)
```

### Lock operations

| Method | Returns | Description |
|--------|---------|-------------|
| `#acquire(*ids)` / `#lock(*ids)` | `Boolean` | Flush expired, then `ZADD NX` all ids. Returns `true` if at least one was newly added. |
| `#release(*ids)` / `#unlock(*ids)` | `Boolean` | `ZREM` all ids. Returns `true` if anything was removed. |
| `#locked?(*ids)` | `Boolean` | `true` if any of the ids is locked and not expired. |
| `#locked_members(*ids)` | `Array<String>` | Subset of `ids` currently locked. |
| `#unlocked_members(*ids)` | `Array<String>` | Subset of `ids` not locked (or expired). |

### Inspection & maintenance

| Method | Returns | Description |
|--------|---------|-------------|
| `#size(flush_expired: true)` | `Integer` | Count of non-expired members. |
| `#members { \|id\| ... }` | `Enumerator` or return of block | Iterate non-expired members. |
| `#flush` | `Integer` | Delete the entire key. Returns count removed. |
| `#flush_expired_members` | `Integer` | `ZREMRANGEBYSCORE('-inf', '(now')`. Returns count pruned. |

---

## `DeDupe::LockKey`

Builds hierarchical Redis keys.

```ruby
DeDupe::LockKey.new('import', 'users').to_s
# => "de-dupe:import:users"
```

- Inserts the configured namespace as the first segment.
- Downcases string parts.
- Compacts out nil segments.

---

## `DeDupe::RedisPool`

Thin wrapper that unifies `Redis` and `ConnectionPool` behind a single `.with { |conn| ... }` interface. You rarely construct this yourself — `DeDupe.redis_pool` returns it.

---

## Return-value conventions

- `acquire` returns `true` on success, `false` (or `nil`, for the block form) if not acquired.
- `release` / `unlock` returns `true` if anything was removed, `false` otherwise.
- Query methods (`locked?`, `locked_members`, `unlocked_members`) are read-only and do not mutate state.
- `acquire` on `Dataset` eagerly prunes expired entries — `release` and queries do not.

## Redis commands used

| Operation | Command |
|-----------|---------|
| Acquire | `ZADD key NX score member` |
| Release | `ZREM key member` |
| Check one | `ZSCORE key member` |
| Check many | `ZMSCORE key member...` |
| Prune expired | `ZREMRANGEBYSCORE key -inf (now` |
| Count | `ZCARD key` |
| Iterate | `ZRANGEBYSCORE key (now +inf` |

All operations are O(log n) or better, with n = members in the sorted set.
