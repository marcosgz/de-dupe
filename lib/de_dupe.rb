# frozen_string_literal: true

require "redis"
require "forwardable"
require "zeitwerk"

loader = Zeitwerk::Loader.for_gem
loader.inflector.inflect "version" => "VERSION"
loader.ignore("#{__dir__}/de-dupe.rb")
loader.ignore("#{__dir__}/dedupe.rb")
loader.log! if ENV["DEBUG"]
loader.setup

module DeDupe
  class Error < StandardError; end

  module_function

  def config
    @config ||= Config.new
  end

  def configure(&block)
    return unless block

    config.instance_eval(&block)
    @redis_pool = nil
    config
  end

  def acquire(*keys, **kwargs, &block)
    id = keys.pop
    if keys.empty?
      raise Error, <<~ERROR.strip
        You must provide the namespace + the identifier for the lock.

        Example:
        DeDupe.acquire("long-running-job", "1234567890", ttl: 50) do
          # code to execute
        end
      ERROR
    end

    namespace = LockKey.new(*keys).to_s
    lock = Lock.new(lock_key: namespace, lock_id: id, **kwargs)
    lock.with_lock(&block)
  end

  def redis_pool
    @redis_pool ||= RedisPool.new(config.redis)
  end

  def flush_all
    keys.inject(0) do |total, (key, cli)|
      total + cli.del(key)
    end
  end

  def keys
    Enumerator.new do |yielder|
      redis_pool.with do |cli|
        cli.keys("#{config.namespace}:*").each { |key| yielder.yield(key, cli) }
      end
    end
  end
end

loader.eager_load
