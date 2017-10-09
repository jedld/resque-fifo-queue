require "bundler/setup"
require "resque/fifo/queue"

RSpec.configure do |config|
  # Enable flags like --only-failures and --next-failure
  config.example_status_persistence_file_path = ".rspec_status"

  config.expect_with :rspec do |c|
    c.syntax = :expect
  end

  config.before(:suite) do
    $redis = Redis.new
  end

  config.before(:each) do
    $redis.flushdb
  end
end
