# frozen_string_literal: true

module DeDupe
  class LockKey
    SEPARATOR = ":"

    def initialize(*keys)
      @keys = keys.map { |k| k.to_s.strip.downcase }
    end

    def to_s
      [DeDupe.config.namespace, *keys].compact.join(SEPARATOR)
    end

    private

    attr_reader :keys
  end
end
