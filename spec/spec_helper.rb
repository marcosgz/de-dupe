# frozen_string_literal: true

require "de-dupe"
require "support/constants"
require "support/hooks/timecop"

RSpec.configure do |config|
  # Enable flags like --only-failures and --next-failure
  config.example_status_persistence_file_path = ".rspec_status"

  # Disable RSpec exposing methods globally on `Module` and `main`
  config.disable_monkey_patching!

  config.expect_with :rspec do |c|
    c.syntax = :expect
  end

  def reset_config!
    DeDupe.instance_variable_set(:@config, nil)
    DeDupe.instance_variable_set(:@redis_pool, nil)
  end
end
