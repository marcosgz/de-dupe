# frozen_string_literal: true

require "spec_helper"

RSpec.describe DeDupe::Lock, freeze_at: [2025, 12, 23, 22, 24, 40] do
  let(:ttl) { Time.now.to_f + HOUR_IN_SECONDS }
  let(:lock_key) { %w[de-dupe test uniqueness-lock].join(":") }
  let(:lock_id) { "abc123" }
  let(:model) { described_class.new(lock_key: lock_key, ttl: ttl, lock_id: lock_id) }

  describe ".flush" do
    before do
      DeDupe.config.expires_in = 604_800
    end

    after do
      reset_config!
    end

    it "does not raise an error when the lock_key does not exist" do
      DeDupe.redis_pool.with do |conn|
        conn.del(lock_key)
        expect(described_class.count(lock_key, redis: conn)).to eq(0)
      end
      expect(described_class.count(lock_key)).to eq(0)
    end

    it "filters the range acording to the ttl" do
      described_class.new(lock_key: lock_key, ttl: ttl - 1, lock_id: "#{lock_id}a").lock
      described_class.new(lock_key: "#{lock_key}x", ttl: ttl, lock_id: "#{lock_id}b").lock
      described_class.new(lock_key: lock_key, ttl: ttl + 1, lock_id: "#{lock_id}c").lock

      expect(described_class.count(lock_key)).to eq(2)
      expect(described_class.count(lock_key, from: ttl + 1)).to eq(1)
      expect(described_class.count(lock_key, to: ttl - 1)).to eq(1)
      expect(described_class.count("#{lock_key}x", from: ttl, to: ttl)).to eq(1)
      expect(described_class.count(lock_key, from: ttl, to: ttl)).to eq(0)
    end
  end

  describe ".flush" do
    it "does not raise an error when the lock_key does not exist" do
      DeDupe.redis_pool.with do |conn|
        conn.del(lock_key)
        expect { described_class.flush(lock_key, redis: conn) }.not_to raise_error
      end
      expect { described_class.flush(lock_key) }.not_to raise_error
    end

    it "removes all locks without redis argument" do
      described_class.new(lock_key: lock_key, ttl: ttl, lock_id: "#{lock_id}1").lock
      described_class.new(lock_key: lock_key, ttl: ttl, lock_id: "#{lock_id}2").lock

      DeDupe.redis_pool.with do |conn|
        expect(conn.zcount(lock_key, 0, ttl + WEEK_IN_SECONDS)).to be >= 2
        expect { described_class.flush(lock_key) }.not_to raise_error
        expect(conn.zcount(lock_key, 0, ttl + WEEK_IN_SECONDS)).to eq(0)
      end
    end

    it "removes all locks using connection from arguments" do
      described_class.new(lock_key: lock_key, ttl: ttl, lock_id: "#{lock_id}1").lock
      described_class.new(lock_key: lock_key, ttl: ttl, lock_id: "#{lock_id}2").lock

      DeDupe.redis_pool.with do |conn|
        expect(conn.zcount(lock_key, 0, ttl + WEEK_IN_SECONDS)).to be >= 2
        expect { described_class.flush(lock_key, redis: conn) }.not_to raise_error
        expect(conn.zcount(lock_key, 0, ttl + WEEK_IN_SECONDS)).to eq(0)
      end
    end
  end

  describe ".flush_expired_members" do
    it "does not raise an error when the lock_key is nil" do
      DeDupe.redis_pool.with do |conn|
        conn.del(lock_key)
      end
      expect { described_class.flush_expired_members(lock_key) }.not_to raise_error
    end

    specify do
      expect { described_class.flush_expired_members(nil) }.not_to raise_error
    end

    specify do
      DeDupe.redis_pool.with do |conn|
        conn.del(lock_key)
        expect { described_class.flush_expired_members(lock_key, redis: conn) }.not_to raise_error
        expect { described_class.flush_expired_members(nil, redis: conn) }.not_to raise_error
      end
    end

    specify do
      DeDupe.redis_pool.with do |conn|
        lock_queue1 = described_class.new(lock_key: "#{lock_key}1", ttl: ttl, lock_id: lock_id).tap(&:lock)
        lock_queue2 = described_class.new(lock_key: "#{lock_key}2", ttl: ttl, lock_id: lock_id).tap(&:lock)
        expect(conn.zcount(lock_queue1.lock_key, 0, ttl)).to eq(1)
        expect(conn.zcount(lock_queue2.lock_key, 0, ttl)).to eq(1)

        described_class.flush_expired_members(lock_queue1.lock_key)
        expect(conn.zcount(lock_queue1.lock_key, 0, ttl)).to eq(1)
        expect(conn.zcount(lock_queue2.lock_key, 0, ttl)).to eq(1)

        travel_to = Time.at(ttl)
        Timecop.travel(travel_to) do
          described_class.flush_expired_members(lock_queue1.lock_key)
          expect(conn.zcount(lock_queue1.lock_key, 0, ttl)).to eq(0)
          expect(conn.zcount(lock_queue2.lock_key, 0, ttl)).to eq(1)
        end
      end
    end
  end

  describe "#coerce" do
    it "returns nil if the value is not a hash" do
      expect(described_class.coerce(nil)).to be_nil
      expect(described_class.coerce(false)).to be_nil
      expect(described_class.coerce(true)).to be_nil
      expect(described_class.coerce("")).to be_nil
      expect(described_class.coerce(1)).to be_nil
    end

    it "returns a new lock instance if the value is a hash" do
      expect(described_class.coerce(ttl: ttl, lkey: lock_key, lid: lock_id)).to eq(
        described_class.new(ttl: ttl, lock_key: lock_key, lock_id: lock_id)
      )
    end

    it "returns a new lock instance if the value is a hash" do
      expect(described_class.coerce("ttl" => ttl, "lkey" => lock_key, "lid" => lock_id)).to eq(
        described_class.new(ttl: ttl, lock_key: lock_key, lock_id: lock_id)
      )
    end

    it "returns nil if the value is a hash with nil values" do
      expect(described_class.coerce("ttl" => nil, "lkey" => lock_key, "lid" => lock_id)).to be_nil
      expect(described_class.coerce("ttl" => ttl, "lkey" => nil, "lid" => lock_id)).to be_nil
      expect(described_class.coerce("ttl" => ttl, "lkey" => lock_key, "lid" => nil)).to be_nil
    end
  end

  describe "#to_hash" do
    subject { model.to_hash }

    it "returns the lock as a hash" do
      expect(subject).to eq(
        "lkey" => lock_key.to_s,
        "ttl" => ttl,
        "lid" => lock_id.to_s
      )
    end
  end

  describe "#lock" do
    it "locks the lock_key" do
      DeDupe.redis_pool.with do |conn|
        conn.del(lock_key)
        expect(conn.zcount(lock_key, 0, ttl)).to eq(0)

        expect(model.lock).to be(true)
        expect(conn.zcount(lock_key, 0, ttl)).to eq(1)
        expect(model.lock).to be(false)
        expect(conn.zcount(lock_key, 0, ttl)).to eq(1)
      end
    end

    it "locks the lock_key with a new ttl" do
      DeDupe.redis_pool.with do |conn|
        conn.del(lock_key)
        expect(model.lock).to be(true)

        travel_to = Time.at(ttl)
        expect(conn.zcount(lock_key, 0, ttl)).to eq(1)

        Timecop.travel(travel_to) do
          new_ttl = ttl + HOUR_IN_SECONDS
          new_model = described_class.new(lock_key: model.lock_key, lock_id: model.lock_id, ttl: new_ttl)
          expect(new_model.lock).to be(false)
          expect(conn.zcount(lock_key, 0, new_ttl)).to eq(1)
          expect(conn.zcount(lock_key, 0, ttl)).to eq(0)
        end
      end
    end
  end

  describe "#unlock" do
    it "unlocks the lock_key" do
      DeDupe.redis_pool.with do |conn|
        conn.del(lock_key)
        expect(conn.zcount(lock_key, 0, ttl)).to eq(0)
        expect(model.unlock).to be(false)

        conn.zadd(lock_key, ttl, lock_id)
        expect(conn.zcount(lock_key, 0, ttl)).to eq(1)

        expect(model.unlock).to be(true)
        expect(conn.zcount(lock_key, 0, ttl)).to eq(0)
      end
    end
  end

  describe "#locked?" do
    it "returns true if the lock_key is locked" do
      DeDupe.redis_pool.with do |conn|
        conn.del(lock_key)
        expect(model.locked?).to be(false)

        conn.zadd(lock_key, ttl, lock_id)
        expect(model.locked?).to be(true)

        expect(model.unlock).to be(true)
        expect(model.locked?).to be(false)
      end
    end

    it "returns true if the lock_key is locked" do
      DeDupe.redis_pool.with do |conn|
        conn.del(lock_key)
        expect(model.locked?).to be(false)

        conn.zadd(lock_key, ttl, lock_id)
        expect(model.locked?).to be(true)

        travel_to = Time.at(ttl)
        expect(conn.zcount(lock_key, 0, travel_to.to_f)).to eq(1)
        Timecop.travel(travel_to) do
          expect(model.locked?).to be(false)
          expect(conn.zcount(lock_key, 0, travel_to.to_f)).to eq(0)
        end
      end
    end
  end
end
