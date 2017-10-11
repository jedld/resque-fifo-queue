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
      WORKER_QUEUE_NAMESPACE = "fifo-managed-queue"


      module Queue
      end
    end
  end
end
