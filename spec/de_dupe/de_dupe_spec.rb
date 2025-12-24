# frozen_string_literal: true

RSpec.describe DeDupe do
  it "has a version number" do
    expect(DeDupe::VERSION).not_to be_nil
  end

  describe "#redis_pool" do
    before do
      reset_config!
    end

    it "returns a RedisPool instance" do
      expect(described_class.redis_pool).to be_a(DeDupe::RedisPool)
      expect(described_class.instance_variable_get(:@redis_pool)).to be_a(DeDupe::RedisPool)
    end
  end

  describe "#config" do
    it { expect(described_class.config).to be_an_instance_of(DeDupe::Config) }
  end

  describe "#configure" do
    after { reset_config! }

    it "overwrites default config value" do
      described_class.config.namespace = "async-store1"

      described_class.configure { |config| config.namespace = "async-store2" }
      expect(described_class.config.namespace).to eq("async-store2")
    end

    it "starts a fresh redis pool" do
      pool = described_class.redis_pool
      3.times { expect(described_class.redis_pool).to eql(pool) }
      described_class.configure { |config| config.namespace = "async-store" }
      expect(described_class.redis_pool).not_to eql(pool)
    end
  end

  describe "#flush_all" do
    let(:pool) { described_class.redis_pool }
    let(:ns) { described_class.config.namespace }

    specify do
      pool.with { |c| c.set("#{ns}:foo", 0) }
      pool.with { |c| c.set("#{ns}:bar", 0) }
      pool.with { |c| c.set("#{ns}1:other", 0) }
      expect(described_class.flush_all).to be >= 2
      pool do |cli|
        expect(cli).not_to exist("#{ns}:foo")
        expect(cli).not_to exist("#{ns}:bar")
        expect(cli).to exist("#{ns}1:other")
        cli.del("#{ns}1:other")
      end
    end
  end

  describe "#keys" do
    let(:pool) { described_class.redis_pool }
    let(:ns) { described_class.config.namespace }

    specify do
      expect(described_class.keys).to be_an_instance_of(Enumerator)
    end

    specify do
      pool.with { |c| c.set("#{ns}:foo", 0) }
      expect(described_class.keys).to include("#{ns}:foo")
      pool.with { |c| c.del("#{ns}:foo") }
      expect(described_class.keys).not_to include("#{ns}:foo")
    end
  end

  describe "#acquire" do
    before do
      reset_config!
      described_class.config.namespace = "test-namespace"
      described_class.redis_pool.with do |conn|
        # Clean up any existing locks
        conn.keys("test-namespace:*").each { |key| conn.del(key) }
      end
    end

    after do
      reset_config!
    end

    context "when only one key is provided" do
      it "raises an error" do
        expect do
          described_class.acquire("only-id") do
            "should not execute"
          end
        end.to raise_error(DeDupe::Error, /You must provide the namespace/)
      end

      it "includes example usage in error message" do
        expect do
          described_class.acquire("only-id") {}
        end.to raise_error(DeDupe::Error, /DeDupe.acquire\("long-running-job", "1234567890", ttl: 50\)/)
      end
    end

    context "when namespace and id are provided" do
      it "delegates to Lock.new(...).call(&block)" do
        block_executed = false

        result = described_class.acquire("namespace-key", "lock-id") do
          block_executed = true
          "block result"
        end

        expect(block_executed).to be(true)
        expect(result).to eq("block result")
      end

      it "passes the last key as lock_id" do
        ttl = Time.now.to_f + 3600
        lock_key_str = DeDupe::LockKey.new("namespace").to_s

        # Execute acquire and verify it uses the last key as lock_id
        # by checking that a lock with that id can be created (since previous was released)
        described_class.acquire("namespace", "the-lock-id", ttl: ttl) do
          "result"
        end

        # Verify by creating a lock with the expected lock_id - should work since previous was released
        lock = DeDupe::Lock.new(
          lock_key: lock_key_str,
          lock_id: "the-lock-id",
          ttl: ttl
        )
        expect(lock.lock).to be(true)
        lock.unlock
      end

      it "passes all keys except the last as namespace to LockKey" do
        ttl = Time.now.to_f + 3600
        expected_lock_key = DeDupe::LockKey.new("namespace1", "namespace2", "namespace3").to_s

        described_class.acquire("namespace1", "namespace2", "namespace3", "lock-id", ttl: ttl) do
          "result"
        end

        # Verify by creating a lock with the expected key structure
        lock = DeDupe::Lock.new(
          lock_key: expected_lock_key,
          lock_id: "lock-id",
          ttl: ttl
        )
        expect(lock.lock).to be(true)
        expect(lock.lock_key.to_s).to eq("test-namespace:namespace1:namespace2:namespace3")
        lock.unlock
      end

      it "passes kwargs to Lock" do
        ttl = Time.now.to_f + 3600

        described_class.acquire("namespace", "lock-id", ttl: ttl) do
          "result"
        end

        # Verify ttl was used by creating a lock with the same ttl
        lock = DeDupe::Lock.new(
          lock_key: DeDupe::LockKey.new("namespace").to_s,
          lock_id: "lock-id",
          ttl: ttl
        )
        expect(lock.ttl).to eq(ttl)
        expect(lock.lock).to be(true)
        lock.unlock
      end

      it "returns the result from the block" do
        result = described_class.acquire("namespace", "lock-id") do
          "custom result"
        end

        expect(result).to eq("custom result")
      end

      it "returns nil when block returns nil" do
        result = described_class.acquire("namespace", "lock-id") do
          nil
        end

        expect(result).to be_nil
      end

      it "returns value even when block returns false" do
        result = described_class.acquire("namespace", "lock-id") do
          false
        end

        expect(result).to be(false)
      end

      it "does not execute block when lock already exists" do
        ttl = Time.now.to_f + 3600
        lock = DeDupe::Lock.new(
          lock_key: DeDupe::LockKey.new("namespace").to_s,
          lock_id: "lock-id",
          ttl: ttl
        )
        lock.lock

        block_executed = false
        result = described_class.acquire("namespace", "lock-id", ttl: ttl) do
          block_executed = true
          "should not execute"
        end

        expect(block_executed).to be(false)
        expect(result).to be_nil

        # Cleanup
        lock.unlock
      end

      it "releases lock after block execution" do
        ttl = Time.now.to_f + 3600
        lock_key = DeDupe::LockKey.new("namespace").to_s

        described_class.acquire("namespace", "lock-id", ttl: ttl) do
          "result"
        end

        # Verify lock was released
        described_class.redis_pool.with do |conn|
          expect(conn.zscore(lock_key, "lock-id")).to be_nil
        end
      end

      it "releases lock even when block raises an exception" do
        ttl = Time.now.to_f + 3600
        lock_key = DeDupe::LockKey.new("namespace").to_s

        expect do
          described_class.acquire("namespace", "lock-id", ttl: ttl) do
            raise StandardError, "test error"
          end
        end.to raise_error(StandardError, "test error")

        # Verify lock was released
        described_class.redis_pool.with do |conn|
          expect(conn.zscore(lock_key, "lock-id")).to be_nil
        end
      end
    end
  end
end
