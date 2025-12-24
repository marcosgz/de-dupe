# frozen_string_literal: true

require "timecop"

module Hooks
  module Timecop
    def self.included(base)
      base.before do |example|
        ::Timecop.freeze(*example.metadata[:freeze_at]) if example.metadata[:freeze_at]
      end

      base.after do |example|
        ::Timecop.return if example.metadata[:freeze_at]
      end
    end
  end
end
