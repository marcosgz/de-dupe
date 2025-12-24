# frozen_string_literal: true

require "spec_helper"

RSpec.describe DeDupe::LockKey do
  before do
    DeDupe.config.namespace = "test"
  end

  after do
    DeDupe.config.namespace = nil
  end

  describe ".to_s" do
    it "builds the lock key with the namespace and a single key" do
      expect(described_class.new("consumer-key").to_s).to eq("test:consumer-key")
    end

    it "builds the lock key with the namespace and multiple keys" do
      expect(described_class.new("group-name", "consumer-key").to_s).to eq("test:group-name:consumer-key")
    end
  end
end
