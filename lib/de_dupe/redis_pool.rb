# frozen_string_literal: true

module DeDupe
  class RedisPool
    extend Forwardable
    def_delegator :@connection, :with

    module ConnectionPoolLike
      def with
        yield self
      end
    end

    def initialize(connection)
      if connection.respond_to?(:with)
        @connection = connection
      else
        @connection = if connection.respond_to?(:client)
          connection
        else
          ::Redis.new(*[connection].compact)
        end
        @connection.extend(ConnectionPoolLike)
      end
    end
  end
end
