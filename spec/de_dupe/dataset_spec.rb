# frozen_string_literal: true

require "spec_helper"

RSpec.describe DeDupe::Dataset, freeze_at: [2025, 12, 23, 22, 24, 40] do
  let(:ttl) { HOUR_IN_SECONDS }
  let(:lock_key) { DeDupe::LockKey.new("dataset", "test").to_s }
  let(:dataset) { described_class.new(lock_key, ttl: ttl) }
  let(:first_id) { "first_id" }
  let(:second_id) { "second_id" }
  let(:third_id) { "third_id" }

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
    it "sets the lock_key" do
      expect(dataset.lock_key).to eq(lock_key)
    end

    it "sets the ttl from parameter" do
      expect(dataset.ttl).to eq(ttl)
    end

    it "uses config.expires_in when ttl is nil" do
      DeDupe.config.expires_in = 300
      dataset = described_class.new(lock_key, ttl: nil)
      expect(dataset.ttl).to eq(300)
    end

    it "converts lock_key to string" do
      dataset = described_class.new(:symbol_key, ttl: ttl)
      expect(dataset.lock_key).to eq("symbol_key")
    end
  end

  describe "#acquire" do
    it "returns false when ids are empty" do
      expect(dataset.acquire).to be(false)
    end

    it "acquires a single id" do
      DeDupe.redis_pool.with do |conn|
        expect(conn.zcount(lock_key, "-inf", "+inf")).to eq(0)
        expect(dataset.acquire(first_id)).to be(true)
        expect(conn.zcount(lock_key, "-inf", "+inf")).to eq(1)
      end
    end

    it "acquires multiple ids at once" do
      DeDupe.redis_pool.with do |conn|
        expect(conn.zcount(lock_key, "-inf", "+inf")).to eq(0)
        expect(dataset.acquire(first_id, second_id, third_id)).to be(true)
        expect(conn.zcount(lock_key, "-inf", "+inf")).to eq(3)
      end
    end

    it "returns false when trying to acquire already locked ids" do
      expect(dataset.acquire(first_id)).to be(true)
      expect(dataset.acquire(first_id)).to be(false)
    end

    it "only acquires new ids, not already locked ones" do
      expect(dataset.acquire(first_id, second_id)).to be(true)
      expect(dataset.acquire(first_id, second_id)).to be(false)
      DeDupe.redis_pool.with do |conn|
        expect(conn.zcount(lock_key, "-inf", "+inf")).to eq(2)
      end
      expect(dataset.acquire(first_id, third_id)).to be(true)
      DeDupe.redis_pool.with do |conn|
        expect(conn.zcount(lock_key, "-inf", "+inf")).to eq(3)
      end
    end

    it "flushes expired members before acquiring" do
      # Create an expired entry manually
      expired_score = Time.now.to_f - 100
      DeDupe.redis_pool.with do |conn|
        conn.zadd(lock_key, expired_score, "expired_id")
        expect(conn.zcount(lock_key, "-inf", "+inf")).to eq(1)
      end

      dataset.acquire(first_id)
      DeDupe.redis_pool.with do |conn|
        expect(conn.zcount(lock_key, "-inf", "+inf")).to eq(1)
        expect(conn.zscore(lock_key, "expired_id")).to be_nil
        expect(conn.zscore(lock_key, first_id)).not_to be_nil
      end
    end

    it "sets expiration score based on ttl" do
      dataset.acquire(first_id)
      DeDupe.redis_pool.with do |conn|
        score = conn.zscore(lock_key, first_id)
        expected_score = Time.now.to_f + ttl
        expect(score).to be_within(0.1).of(expected_score)
      end
    end
  end

  describe "#lock" do
    it "is an alias for acquire" do
      expect(dataset.lock(first_id)).to be(true)
      expect(dataset.lock(first_id)).to be(false)
    end
  end

  describe "#release" do
    it "returns false when ids are empty" do
      expect(dataset.release).to be(false)
    end

    it "returns false when ids are not locked" do
      expect(dataset.release(first_id)).to be(false)
    end

    it "releases a single id" do
      dataset.acquire(first_id)
      DeDupe.redis_pool.with do |conn|
        expect(conn.zcount(lock_key, "-inf", "+inf")).to eq(1)
      end

      expect(dataset.release(first_id)).to be(true)
      DeDupe.redis_pool.with do |conn|
        expect(conn.zcount(lock_key, "-inf", "+inf")).to eq(0)
      end
    end

    it "releases multiple ids at once" do
      dataset.acquire(first_id, second_id, third_id)
      DeDupe.redis_pool.with do |conn|
        expect(conn.zcount(lock_key, "-inf", "+inf")).to eq(3)
      end

      expect(dataset.release(first_id, second_id)).to be(true)
      DeDupe.redis_pool.with do |conn|
        expect(conn.zcount(lock_key, "-inf", "+inf")).to eq(1)
        expect(conn.zscore(lock_key, third_id)).not_to be_nil
      end
    end

    it "only releases ids that exist" do
      dataset.acquire(first_id, second_id)
      expect(dataset.release(first_id, second_id, third_id)).to be(true)
      DeDupe.redis_pool.with do |conn|
        expect(conn.zcount(lock_key, "-inf", "+inf")).to eq(0)
      end
    end
  end

  describe "#unlock" do
    it "is an alias for release" do
      dataset.acquire(first_id)
      expect(dataset.unlock(first_id)).to be(true)
      expect(dataset.unlock(first_id)).to be(false)
    end
  end

  describe "#locked?" do
    it "returns false when ids are empty" do
      expect(dataset.locked?).to be(false)
    end

    it "returns false when id is not locked" do
      expect(dataset.locked?(first_id)).to be(false)
    end

    it "returns true when id is locked" do
      dataset.acquire(first_id)
      expect(dataset.locked?(first_id)).to be(true)
    end

    it "returns true when all ids are locked" do
      dataset.acquire(first_id, second_id)
      expect(dataset.locked?(first_id, second_id)).to be(true)
    end

    it "returns true when at least one id is locked" do
      dataset.acquire(first_id)
      expect(dataset.locked?(first_id, second_id)).to be(true)
    end

    it "returns false when id is expired" do
      dataset.acquire(first_id)
      expect(dataset.locked?(first_id)).to be(true)

      travel_to = Time.at(Time.now.to_f + ttl + 1)
      Timecop.travel(travel_to) do
        expect(dataset.locked?(first_id)).to be(false)
      end
    end

    it "returns false when some ids are expired" do
      dataset.acquire(first_id, second_id)
      expect(dataset.locked?(first_id, second_id)).to be(true)

      travel_to = Time.at(Time.now.to_f + ttl + 1)
      Timecop.travel(travel_to) do
        expect(dataset.locked?(first_id, second_id)).to be(false)
      end
    end
  end

  describe "#unlocked_members" do
    it "returns empty array when ids are empty" do
      expect(dataset.unlocked_members).to eq([])
    end

    it "returns all ids when none are locked" do
      expect(dataset.unlocked_members(first_id, second_id, third_id)).to contain_exactly(first_id, second_id, third_id)
    end

    it "returns only unlocked ids when some are locked" do
      dataset.acquire(first_id)
      expect(dataset.unlocked_members(first_id, second_id, third_id)).to contain_exactly(second_id, third_id)
    end

    it "returns empty array when all ids are locked" do
      dataset.acquire(first_id, second_id, third_id)
      expect(dataset.unlocked_members(first_id, second_id, third_id)).to eq([])
    end

    it "returns expired ids as unlocked" do
      dataset.acquire(first_id, second_id)
      expect(dataset.unlocked_members(first_id, second_id)).to eq([])

      travel_to = Time.at(Time.now.to_f + ttl + 1)
      Timecop.travel(travel_to) do
        expect(dataset.unlocked_members(first_id, second_id)).to contain_exactly(first_id, second_id)
      end
    end

    it "returns ids that were never locked along with expired ones" do
      dataset.acquire(first_id)
      # Release first_id to make it unlocked
      dataset.release(first_id)
      expect(dataset.unlocked_members(first_id, second_id, third_id)).to contain_exactly(first_id, second_id, third_id)
    end
  end

  describe "#locked_members" do
    it "returns empty array when ids are empty" do
      expect(dataset.locked_members).to eq([])
    end

    it "returns empty array when none are locked" do
      expect(dataset.locked_members(first_id, second_id, third_id)).to eq([])
    end

    it "returns only locked ids" do
      dataset.acquire(first_id, second_id)
      expect(dataset.locked_members(first_id, second_id, third_id)).to contain_exactly(first_id, second_id)
    end

    it "returns all ids when all are locked" do
      dataset.acquire(first_id, second_id, third_id)
      expect(dataset.locked_members(first_id, second_id, third_id)).to contain_exactly(first_id, second_id, third_id)
    end

    it "does not return expired ids" do
      dataset.acquire(first_id, second_id)
      expect(dataset.locked_members(first_id, second_id)).to contain_exactly(first_id, second_id)

      travel_to = Time.at(Time.now.to_f + ttl + 1)
      Timecop.travel(travel_to) do
        expect(dataset.locked_members(first_id, second_id)).to eq([])
      end
    end

    it "returns only non-expired locked ids" do
      dataset.acquire(first_id, second_id)
      new_dataset = described_class.new(lock_key, ttl: ttl + HOUR_IN_SECONDS)
      new_dataset.acquire(third_id)

      travel_to = Time.at(Time.now.to_f + ttl + 1)
      Timecop.travel(travel_to) do
        # first_id and second_id are expired, but third_id should still be locked with longer TTL
        expect(dataset.locked_members(first_id, second_id, third_id)).to contain_exactly(third_id)
        expect(new_dataset.locked_members(third_id)).to contain_exactly(third_id)
      end
    end
  end

  describe "#flush" do
    it "returns false when lock_key does not exist" do
      DeDupe.redis_pool.with do |conn|
        conn.del(lock_key)
        expect(dataset.flush).to be(false)
      end
    end

    it "removes all entries from the dataset" do
      dataset.acquire(first_id, second_id, third_id)
      DeDupe.redis_pool.with do |conn|
        expect(conn.zcount(lock_key, "-inf", "+inf")).to eq(3)
      end

      expect(dataset.flush).to be(true)
      DeDupe.redis_pool.with do |conn|
        expect(conn.zcount(lock_key, "-inf", "+inf")).to eq(0)
      end
    end

    it "returns true when entries are removed" do
      dataset.acquire(first_id)
      expect(dataset.flush).to be(true)
    end
  end

  describe "#size" do
    it "returns 0 when dataset is empty" do
      expect(dataset.size).to eq(0)
    end

    it "returns the number of entries in the dataset" do
      dataset.acquire(first_id)
      expect(dataset.size).to eq(1)

      dataset.acquire(second_id, third_id)
      expect(dataset.size).to eq(3)
    end

    it "flushes expired members by default" do
      dataset.acquire(first_id, second_id)
      expect(dataset.size).to eq(2)

      travel_to = Time.at(Time.now.to_f + ttl + 1)
      Timecop.travel(travel_to) do
        expect(dataset.size).to eq(0)
      end
    end

    it "does not flush expired members when flush_expired is false" do
      dataset.acquire(first_id, second_id)
      expect(dataset.size).to eq(2)

      travel_to = Time.at(Time.now.to_f + ttl + 1)
      Timecop.travel(travel_to) do
        expect(dataset.size(flush_expired: false)).to eq(2)
        expect(dataset.size(flush_expired: true)).to eq(0)
      end
    end

    it "only counts non-expired entries after flushing" do
      dataset.acquire(first_id, second_id)
      new_dataset = described_class.new(lock_key, ttl: ttl + HOUR_IN_SECONDS)
      new_dataset.acquire(third_id)

      expect(dataset.size).to eq(3)

      travel_to = Time.at(Time.now.to_f + ttl + 1)
      Timecop.travel(travel_to) do
        expect(dataset.size).to eq(1)
      end
    end
  end

  describe "#members" do
    it "yields nothing when dataset is empty" do
      yielded = []
      dataset.members { |member| yielded << member }
      expect(yielded).to eq([])
    end

    it "yields a single member" do
      dataset.acquire(first_id)
      yielded = []
      dataset.members { |member| yielded << member }
      expect(yielded).to contain_exactly(first_id)
    end

    it "yields all members in the dataset" do
      dataset.acquire(first_id, second_id, third_id)
      yielded = []
      dataset.members { |member| yielded << member }
      expect(yielded).to contain_exactly(first_id, second_id, third_id)
    end

    it "flushes expired members before iterating" do
      dataset.acquire(first_id, second_id)
      expired_score = Time.now.to_f - 100
      DeDupe.redis_pool.with do |conn|
        conn.zadd(lock_key, expired_score, "expired_id")
        expect(conn.zcount(lock_key, "-inf", "+inf")).to eq(3)
      end

      yielded = []
      dataset.members { |member| yielded << member }
      expect(yielded).to contain_exactly(first_id, second_id)
      expect(yielded).not_to include("expired_id")
    end

    it "does not yield expired members" do
      dataset.acquire(first_id, second_id)
      expect(dataset.size).to eq(2)

      travel_to = Time.at(Time.now.to_f + ttl + 1)
      Timecop.travel(travel_to) do
        yielded = []
        dataset.members { |member| yielded << member }
        expect(yielded).to eq([])
      end
    end

    it "yields only non-expired members when some are expired" do
      dataset.acquire(first_id)
      long_ttl_dataset = described_class.new(lock_key, ttl: ttl + HOUR_IN_SECONDS)
      long_ttl_dataset.acquire(second_id)

      travel_to = Time.at(Time.now.to_f + ttl + 1)
      Timecop.travel(travel_to) do
        yielded = []
        dataset.members { |member| yielded << member }
        expect(yielded).to contain_exactly(second_id)
      end
    end

    it "returns an enumerator when no block is given" do
      dataset.acquire(first_id, second_id)
      enumerator = dataset.members
      expect(enumerator).to be_a(Enumerator)
      expect(enumerator.to_a).to contain_exactly(first_id, second_id)
    end
  end

  describe "#flush_expired_members" do
    it "returns false when lock_key does not exist" do
      DeDupe.redis_pool.with do |conn|
        conn.del(lock_key)
        expect(dataset.flush_expired_members).to be(false)
      end
    end

    it "does not remove non-expired entries" do
      dataset.acquire(first_id, second_id)
      DeDupe.redis_pool.with do |conn|
        expect(conn.zcount(lock_key, "-inf", "+inf")).to eq(2)
      end

      dataset.flush_expired_members
      DeDupe.redis_pool.with do |conn|
        expect(conn.zcount(lock_key, "-inf", "+inf")).to eq(2)
      end
    end

    it "removes expired entries" do
      dataset.acquire(first_id, second_id)
      DeDupe.redis_pool.with do |conn|
        expect(conn.zcount(lock_key, "-inf", "+inf")).to eq(2)
      end

      travel_to = Time.at(Time.now.to_f + ttl + 1)
      Timecop.travel(travel_to) do
        expect(dataset.flush_expired_members).to be(true)
        DeDupe.redis_pool.with do |conn|
          expect(conn.zcount(lock_key, "-inf", "+inf")).to eq(0)
        end
      end
    end

    it "removes only expired entries, keeps active ones" do
      # Create entries with different expiration times
      dataset.acquire(first_id)
      long_ttl_dataset = described_class.new(lock_key, ttl: ttl + HOUR_IN_SECONDS)
      long_ttl_dataset.acquire(second_id)

      DeDupe.redis_pool.with do |conn|
        expect(conn.zcount(lock_key, "-inf", "+inf")).to eq(2)
      end

      travel_to = Time.at(Time.now.to_f + ttl + 1)
      Timecop.travel(travel_to) do
        dataset.flush_expired_members
        DeDupe.redis_pool.with do |conn|
          expect(conn.zcount(lock_key, "-inf", "+inf")).to eq(1)
          expect(conn.zscore(lock_key, first_id)).to be_nil
          expect(conn.zscore(lock_key, second_id)).not_to be_nil
        end
      end
    end

    it "is called automatically during acquire" do
      # Manually add an expired entry
      expired_score = Time.now.to_f - 100
      DeDupe.redis_pool.with do |conn|
        conn.zadd(lock_key, expired_score, "expired_id")
        expect(conn.zcount(lock_key, "-inf", "+inf")).to eq(1)
      end

      dataset.acquire(first_id)
      DeDupe.redis_pool.with do |conn|
        expect(conn.zscore(lock_key, "expired_id")).to be_nil
        expect(conn.zscore(lock_key, first_id)).not_to be_nil
      end
    end
  end

  describe "integration scenarios" do
    it "handles multiple acquire and release cycles" do
      expect(dataset.acquire(first_id, second_id)).to be(true)
      expect(dataset.locked?(first_id, second_id)).to be(true)
      expect(dataset.unlocked_members(first_id, second_id, third_id)).to contain_exactly(third_id)

      expect(dataset.release(first_id)).to be(true)
      expect(dataset.locked?(first_id, second_id)).to be(true)
      expect(dataset.locked_members(first_id, second_id)).to contain_exactly(second_id)
      expect(dataset.unlocked_members(first_id, second_id)).to contain_exactly(first_id)

      expect(dataset.acquire(first_id, third_id)).to be(true)
      expect(dataset.locked_members(first_id, second_id, third_id)).to contain_exactly(first_id, second_id, third_id)
    end

    it "handles TTL expiration correctly" do
      dataset.acquire(first_id, second_id)
      expect(dataset.locked?(first_id, second_id)).to be(true)

      # Travel forward in time past TTL
      travel_to = Time.at(Time.now.to_f + ttl + 1)
      Timecop.travel(travel_to) do
        expect(dataset.locked?(first_id, second_id)).to be(false)
        expect(dataset.unlocked_members(first_id, second_id)).to contain_exactly(first_id, second_id)
        expect(dataset.locked_members(first_id, second_id)).to eq([])

        # Can acquire again after expiration
        expect(dataset.acquire(first_id, second_id)).to be(true)
        expect(dataset.locked?(first_id, second_id)).to be(true)
      end
    end

    it "handles different TTLs for same lock_key" do
      dataset1 = described_class.new(lock_key, ttl: 60)
      dataset2 = described_class.new(lock_key, ttl: 300)

      dataset1.acquire(first_id)
      dataset2.acquire(second_id)

      # After 61 seconds, first_id should expire but second_id should still be locked
      travel_to = Time.at(Time.now.to_f + 61)
      Timecop.travel(travel_to) do
        expect(dataset1.locked?(first_id)).to be(false)
        expect(dataset2.locked?(second_id)).to be(true)
      end
    end
  end
end
