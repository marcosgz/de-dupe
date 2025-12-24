# frozen_string_literal: true

require "spec_helper"

RSpec.describe DeDupe::Lock, freeze_at: [2025, 12, 23, 22, 24, 40] do
  let(:ttl) { HOUR_IN_SECONDS }
  let(:lock_key) { DeDupe::LockKey.new("lock", "test").to_s }
  let(:lock_id) { "abc123" }
  let(:lock) { described_class.new(lock_key: lock_key, lock_id: lock_id, ttl: ttl) }

  before do
    DeDupe.redis_pool.with do |conn|
      conn.del(lock_key)
    end
  end

  after do
    DeDupe.redis_pool.with do |conn|
      conn.del(lock_key)
    end
  end

  describe "#initialize" do
    it "sets the lock_key through dataset" do
      expect(lock.lock_key).to eq(lock_key)
    end

    it "sets the lock_id" do
      expect(lock.lock_id).to eq(lock_id)
    end

    it "sets the ttl through dataset" do
      expect(lock.ttl).to eq(ttl)
    end

    it "uses config.expires_in when ttl is nil" do
      DeDupe.config.expires_in = 300
      lock = described_class.new(lock_key: lock_key, lock_id: lock_id, ttl: nil)
      expect(lock.ttl).to eq(300)
    end

    it "converts lock_id to string" do
      lock = described_class.new(lock_key: lock_key, lock_id: :symbol_id, ttl: ttl)
      expect(lock.lock_id).to eq("symbol_id")
    end
  end

  describe "#acquire" do
    it "returns true when lock is successfully acquired" do
      expect(lock.acquire).to be(true)
    end

    it "returns false when lock already exists" do
      expect(lock.acquire).to be(true)
      expect(lock.acquire).to be(false)
    end

    it "creates the lock in Redis" do
      DeDupe.redis_pool.with do |conn|
        expect(conn.zscore(lock_key, lock_id)).to be_nil
      end

      lock.acquire

      DeDupe.redis_pool.with do |conn|
        score = conn.zscore(lock_key, lock_id)
        expect(score).not_to be_nil
        expect(score).to be_within(0.1).of(Time.now.to_f + ttl)
      end
    end
  end

  describe "#lock" do
    it "is an alias for acquire" do
      expect(lock.lock).to be(true)
      expect(lock.lock).to be(false)
    end
  end

  describe "#release" do
    it "returns false when lock does not exist" do
      expect(lock.release).to be(false)
    end

    it "returns true when lock is successfully released" do
      lock.acquire
      expect(lock.release).to be(true)
    end

    it "removes the lock from Redis" do
      lock.acquire
      DeDupe.redis_pool.with do |conn|
        expect(conn.zscore(lock_key, lock_id)).not_to be_nil
      end

      lock.release

      DeDupe.redis_pool.with do |conn|
        expect(conn.zscore(lock_key, lock_id)).to be_nil
      end
    end
  end

  describe "#unlock" do
    it "is an alias for release" do
      lock.acquire
      expect(lock.unlock).to be(true)
      expect(lock.unlock).to be(false)
    end
  end

  describe "#locked?" do
    it "returns false when lock does not exist" do
      expect(lock.locked?).to be(false)
    end

    it "returns true when lock exists and is not expired" do
      lock.acquire
      expect(lock.locked?).to be(true)
    end

    it "returns false when lock is expired" do
      lock.acquire
      expect(lock.locked?).to be(true)

      travel_to = Time.at(Time.now.to_f + ttl + 1)
      Timecop.travel(travel_to) do
        expect(lock.locked?).to be(false)
      end
    end
  end

  describe "#with_lock" do
    context "when lock already exists" do
      it "does not execute the block" do
        lock.acquire
        block_executed = false

        result = lock.with_lock do
          block_executed = true
          "block result"
        end

        expect(block_executed).to be(false)
        expect(result).to be_nil
      end

      it "does not release the existing lock" do
        lock.acquire
        expect(lock.locked?).to be(true)

        lock.with_lock do
          "should not execute"
        end

        expect(lock.locked?).to be(true)
      end
    end

    context "when lock does not exist" do
      it "executes the block" do
        block_executed = false

        lock.with_lock do
          block_executed = true
        end

        expect(block_executed).to be(true)
      end

      it "returns the result of the block" do
        result = lock.with_lock do
          "block result"
        end

        expect(result).to eq("block result")
      end

      it "returns nil when block returns nil" do
        result = lock.with_lock do
          nil
        end

        expect(result).to be_nil
      end

      it "returns false when block returns false" do
        result = lock.with_lock do
          false
        end

        expect(result).to be(false)
      end

      it "acquires the lock before executing the block" do
        expect(lock.locked?).to be(false)

        lock.with_lock do
          expect(lock.locked?).to be(true)
        end
      end

      it "releases the lock after successful block execution" do
        expect(lock.locked?).to be(false)

        lock.with_lock do
          expect(lock.locked?).to be(true)
          "success"
        end

        expect(lock.locked?).to be(false)
      end

      it "releases the lock even when block raises an exception" do
        expect(lock.locked?).to be(false)

        expect do
          lock.with_lock do
            expect(lock.locked?).to be(true)
            raise StandardError, "test error"
          end
        end.to raise_error(StandardError, "test error")

        expect(lock.locked?).to be(false)
      end

      it "propagates exceptions from the block" do
        expect do
          lock.with_lock do
            raise ArgumentError, "custom error"
          end
        end.to raise_error(ArgumentError, "custom error")

        expect(lock.locked?).to be(false)
      end

      it "releases lock in ensure block even after exception handling" do
        unlock_call_count = 0
        original_release = lock.method(:release)

        allow(lock).to receive(:release) do
          unlock_call_count += 1
          original_release.call
        end

        expect do
          lock.with_lock do
            raise StandardError, "test error"
          end
        end.to raise_error(StandardError, "test error")

        expect(unlock_call_count).to eq(1)
        expect(lock.locked?).to be(false)
      end
    end
  end

  describe "integration scenarios" do
    context "when executing in parallel with same lock" do
      it "ensures only one task executes when using the same lock" do
        execution_count = 0
        execution_order = []
        mutex = Mutex.new

        # Create two locks with the same lock_key and lock_id
        lock1 = described_class.new(lock_key: lock_key, lock_id: lock_id, ttl: ttl)
        lock2 = described_class.new(lock_key: lock_key, lock_id: lock_id, ttl: ttl)

        threads = []

        # Start first thread
        threads << Thread.new do
          lock1.with_lock do
            mutex.synchronize do
              execution_count += 1
              execution_order << :first
            end
            sleep(0.1) # Simulate some work
            mutex.synchronize do
              execution_order << :first_done
            end
          end
        end

        # Small delay to ensure first thread starts
        sleep(0.01)

        # Start second thread
        threads << Thread.new do
          lock2.with_lock do
            mutex.synchronize do
              execution_count += 1
              execution_order << :second
            end
            sleep(0.1) # Simulate some work
            mutex.synchronize do
              execution_order << :second_done
            end
          end
        end

        # Wait for both threads to complete
        threads.each(&:join)

        # Only one execution should have occurred
        expect(execution_count).to eq(1)
        # First thread should execute, second should not
        expect(execution_order).to eq([:first, :first_done])
      end

      it "allows second task to execute after first completes" do
        execution_count = 0
        execution_order = []
        mutex = Mutex.new

        lock1 = described_class.new(lock_key: lock_key, lock_id: lock_id, ttl: ttl)
        lock2 = described_class.new(lock_key: lock_key, lock_id: lock_id, ttl: ttl)

        # First execution
        thread1 = Thread.new do
          lock1.with_lock do
            mutex.synchronize do
              execution_count += 1
              execution_order << :first
            end
            sleep(0.05) # Simulate some work
            mutex.synchronize do
              execution_order << :first_done
            end
          end
        end

        thread1.join

        # Small delay to ensure lock is released
        sleep(0.01)

        # Second execution should now be able to proceed
        thread2 = Thread.new do
          lock2.with_lock do
            mutex.synchronize do
              execution_count += 1
              execution_order << :second
            end
            sleep(0.05) # Simulate some work
            mutex.synchronize do
              execution_order << :second_done
            end
          end
        end

        thread2.join

        # Both executions should have occurred
        expect(execution_count).to eq(2)
        expect(execution_order).to eq([:first, :first_done, :second, :second_done])
      end
    end

    context "when executing sequentially" do
      it "executes -> wait -> executes again, ensuring lock is released" do
        execution_results = []

        # First execution
        result1 = lock.with_lock do
          execution_results << :first
          "result1"
        end

        expect(result1).to eq("result1")
        expect(execution_results).to eq([:first])
        expect(lock.locked?).to be(false)

        # Wait a bit
        sleep(0.05)

        # Second execution should work since lock was released
        result2 = lock.with_lock do
          execution_results << :second
          "result2"
        end

        expect(result2).to eq("result2")
        expect(execution_results).to eq([:first, :second])
        expect(lock.locked?).to be(false)
      end

      it "allows immediate re-execution after lock release" do
        results = []

        # Execute first time
        lock.with_lock do
          results << 1
        end

        # Immediately execute again (lock should be released)
        lock.with_lock do
          results << 2
        end

        expect(results).to eq([1, 2])
        expect(lock.locked?).to be(false)
      end
    end

    context "with TTL expiration" do
      it "allows re-acquisition after TTL expires" do
        lock.acquire
        expect(lock.locked?).to be(true)

        # Travel forward past TTL
        travel_to = Time.at(Time.now.to_f + ttl + 1)
        Timecop.travel(travel_to) do
          expect(lock.locked?).to be(false)

          # Should be able to acquire again
          expect(lock.acquire).to be(true)
          expect(lock.locked?).to be(true)
        end
      end
    end
  end
end
