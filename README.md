# DeDupe

## Configuration

```ruby
DeDupe.configure do |config|
  config.redis = ConnectionPool.new(size: 10, timeout: 1) do
    Redis.new(url: ENV.fetch("REDIS_URL", "redis://0.0.0.0:6379"))
  end
  config.redis_namespace = "dedupe"
end
```
