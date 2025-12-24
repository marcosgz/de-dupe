# frozen_string_literal: true

require "redis"
require "yaml"

module DeDupe
  class Config
    # Redis/ConnectionPool instance of a valid list of configs to build a new redis connection
    attr_accessor :redis

    # Namespace used to group group data stored by this package
    attr_accessor :namespace

    # The global TTL for the redis storage. Keep nil if you don't want to expire objects.
    attr_accessor :expires_in

    def initialize
      @redis = nil
      @namespace = "de-dupe"
      @expires_in = 5 * 60 # 5 minutes
    end
  end
end
