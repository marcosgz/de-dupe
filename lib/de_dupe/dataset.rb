# frozen_string_literal: true

module DeDupe
  # Distributed deduplication dataset using Redis Sorted Set.
  # Handle multiple entries within the same namespace.
  # The current timestamp is used as the score for the entries
  class Dataset
    attr_reader :lock_key, :ttl

    # @param lock_key [String] The key to store the dataset
    # @param ttl [Integer] The time to live for the dataset in seconds
    def initialize(lock_key, ttl: nil)
      @lock_key = lock_key.to_s
      @ttl = ttl || DeDupe.config.expires_in.to_i
    end

    def acquire(*ids)
      return false if ids.empty?

      flush_expired_members # Keep the dataset clean
      score = expiration_score
      redis_pool.with do |conn|
        conn.zadd(lock_key, ids.map { |id| [score, id] }, nx: true) > 0
      end
    end
    alias_method :lock, :acquire

    def release(*ids)
      return false if ids.empty?

      redis_pool.with do |conn|
        conn.zrem(lock_key, ids) > 0
      end
    end
    alias_method :unlock, :release

    def locked?(*ids)
      return false if ids.empty?

      redis_pool.with do |conn|
        scores = conn.zmscore(lock_key, *ids).compact
        return false if scores.empty?

        current_time = now
        scores.any? { |score| score.to_f >= current_time }
      end
    end

    def unlocked_members(*ids)
      return [] if ids.empty?

      redis_pool.with do |conn|
        scores = conn.zmscore(lock_key, *ids)
        return ids if scores.nil? || scores.empty?

        current_time = now
        scores.each_with_index
          .select { |score, idx| score.nil? || score.to_f <= current_time }
          .map { |score, idx| ids[idx] }
      end
    end

    def locked_members(*ids)
      return [] if ids.empty?

      redis_pool.with do |conn|
        scores = conn.zmscore(lock_key, *ids)
        return [] if scores.nil? || scores.empty?

        current_time = now
        scores.each_with_index
          .select { |score, idx| score && score.to_f >= current_time }
          .map { |score, idx| ids[idx] }
      end
    end

    def flush
      redis_pool.with do |conn|
        conn.del(lock_key) > 0
      end
    end

    def flush_expired_members
      redis_pool.with do |conn|
        conn.zremrangebyscore(lock_key, "-inf", "(#{now}") > 0
      end
    end

    private

    def expiration_score
      now(append_seconds: ttl)
    end

    def now(append_seconds: 0)
      Time.now.to_f + append_seconds
    end

    def redis_pool
      DeDupe.redis_pool
    end
  end
end
