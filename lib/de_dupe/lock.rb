# frozen_string_literal: true

module DeDupe
  # Distributed deduplication lock using Redis Sorted Set.
  # Allows multiple independent locks under the same namespace (lock_key).
  class Lock
    extend Forwardable
    def_delegators :dataset, :lock_key, :ttl

    attr_reader :dataset, :lock_id

    # @param lock_key [String] Namespace/group for similar jobs (e.g., "import:users")
    # @param lock_id  [String] Unique identifier within the namespace (e.g., digest)
    # @param ttl      [Integer, nil] Lock duration in seconds (defaults to config.expires_in)
    def initialize(lock_key:, lock_id:, ttl: nil)
      @lock_id = lock_id.to_s
      @dataset = Dataset.new(lock_key, ttl: ttl)
    end

    def acquire
      dataset.acquire(lock_id)
    end
    alias_method :lock, :acquire

    def release
      dataset.release(lock_id)
    end
    alias_method :unlock, :release

    def locked?
      redis_pool.with do |conn|
        score = conn.zscore(lock_key, lock_id)
        return false unless score

        score.to_f > now
      end
    end

    def with_lock(&block)
      return if locked?
      return unless acquire

      begin
        yield
      ensure
        release
      end
    end

    private

    def redis_pool
      DeDupe.redis_pool
    end

    def expiration_score
      now(append_seconds: ttl)
    end

    def now(append_seconds: 0)
      Time.now.to_f + append_seconds
    end
  end
end
