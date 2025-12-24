# frozen_string_literal: true

RSpec.describe DeDupe do
  it "has a version number" do
    expect(DeDupe::VERSION).not_to be_nil
  end

  describe "#redis_pool" do
    after do
      described_class.config.redis = nil
      described_class.clear_redis_pool!
    end

    it "returns a RedisPool instance" do
      expect(described_class.redis_pool).to be_a(DeDupe::RedisPool)
      expect(described_class.instance_variable_get(:@redis_pool)).to be_a(DeDupe::RedisPool)
    end
  end

  describe "#clear_redis_pool!" do
    it "clears the redis pool" do
      expect(described_class.redis_pool).to be_a(DeDupe::RedisPool)
      described_class.clear_redis_pool!
      expect(described_class.instance_variable_get(:@redis_pool)).to be_nil
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
end
