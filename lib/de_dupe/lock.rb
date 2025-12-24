# frozen_string_literal: true

module DeDupe
  # Class Lock provides access to redis "sorted set" used to control unique jobs
  class Lock
    attr_reader :lock_key, :lock_id, :ttl

    class << self
      # Initialize a Lock object from hash
      #
      # @param value [Hash] Hash with lock properties
      # @return [DeDupe::Lock, nil]
      def coerce(value)
        return unless value.is_a?(Hash)

        lock_key = value[:lkey] || value["lkey"] || value[:lock_key] || value["lock_key"]
        lock_id = value[:lid] || value["lid"] || value[:lock_id] || value["lock_id"]
        ttl = value[:ttl] || value["ttl"]
        return if [lock_key, lock_id, ttl].any?(&:nil?)

        new(lock_key: lock_key, lock_id: lock_id, ttl: ttl)
      end

      # Remove expired locks from redis "sorted set"
      #
      # @param [String] lock_key It's the uniq string used to group similar jobs
      def flush_expired_members(lock_key, redis: nil)
        return unless lock_key

        caller = ->(redis) { redis.zremrangebyscore(lock_key, "-inf", "(#{now}") }

        if redis
          caller.call(redis)
        else
          DeDupe.redis_pool.with { |conn| caller.call(conn) }
        end
      end

      # Remove all locks from redis "sorted set"
      #
      # @param [String] lock_key It's the uniq string used to group similar jobs
      def flush(lock_key, redis: nil)
        return unless lock_key

        caller = ->(conn) { conn.del(lock_key) }

        if redis
          caller.call(redis)
        else
          DeDupe.redis_pool.with { |conn| caller.call(conn) }
        end
      end

      # Number of locks
      #
      # @param lock_key [String] It's the uniq string used to group similar jobs
      # @option [Number] from The begin of set. Default to 0
      # @option [Number] to The end of set. Default to the timestamp of 1 week from now
      # @return Number the amount of entries that within lock_key
      def count(lock_key, from: 0, to: nil, redis: nil)
        to ||= Time.now.to_f + DeDupe.config.expires_in.to_i
        caller = ->(conn) { conn.zcount(lock_key, from, to) }

        if redis
          caller.call(redis)
        else
          DeDupe.redis_pool.with { |conn| caller.call(conn) }
        end
      end
    end

    # @param :lock_key [String] It's the uniq string used to group similar jobs
    # @param :lock_id [String] The uniq job id
    # @param :ttl [Float] The timestamp related lifietime of the lock before being discarded.
    def initialize(lock_key:, lock_id:, ttl:)
      @lock_key = lock_key
      @lock_id = lock_id
      @ttl = ttl
    end

    def to_hash
      {
        "ttl" => ttl,
        "lkey" => lock_key&.to_s,
        "lid" => lock_id&.to_s
      }
    end

    # @return [Float] A float timestamp of current time
    def self.now
      Time.now.to_f
    end

    # Remove lock_id lock from redis
    # @return [Boolean] Returns true when it"s locked or false when there is no lock
    def unlock
      redis_pool.with do |conn|
        conn.zrem(lock_key, lock_id)
      end
    end

    # Adds lock_id lock to redis
    # @return [Boolean] Returns true when it"s a fresh lock or false when lock already exists
    def lock
      redis_pool.with do |conn|
        conn.zadd(lock_key, ttl, lock_id)
      end
    end

    # Check if the lock_id lock exist
    # @return [Boolean] true or false when lock exist or not
    def locked?
      locked = false

      redis_pool.with do |conn|
        timestamp = conn.zscore(lock_key, lock_id)
        return false unless timestamp

        locked = timestamp >= now
        self.class.flush_expired_members(lock_key, redis: conn)
      end

      locked
    end

    def eql?(other)
      return false unless other.is_a?(self.class)

      [lock_key, lock_id, ttl] == [other.lock_key, other.lock_id, other.ttl]
    end
    alias_method :==, :eql?

    protected

    def now
      self.class.now
    end

    def redis_pool
      DeDupe.redis_pool
    end
  end
end
