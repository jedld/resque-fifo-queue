require 'resque'
require "resque/fifo/queue/version"
require 'redis-namespace'
require "resque/fifo/queue/manager"
require "redis"
require "redlock"
require 'xxhash'

module Resque
  module Fifo
    module Queue
    end
  end
end
