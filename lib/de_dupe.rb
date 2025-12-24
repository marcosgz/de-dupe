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

  def redis_pool
    @redis_pool ||= RedisPool.new(config.redis)
  end

  def clear_redis_pool!
    @redis_pool = nil
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
