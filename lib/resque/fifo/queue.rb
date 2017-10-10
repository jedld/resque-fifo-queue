require 'resque'
require 'resque/plugins/fifo/worker'
require "resque/plugins/fifo/queue/version"
require "resque/plugins/fifo/queue/manager"
require "redis"
require "redlock"
require 'xxhash'

module Resque
  module Plugins
    module Fifo
      module Queue
      end
    end
  end
end
