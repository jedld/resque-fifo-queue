module Resque
  module Plugins
    module Fifo
      class Worker < Resque::Worker
        # Registers ourself as a worker. Useful when entering the worker
        # lifecycle on startup.
        def register_worker
          super

          manager = Resque::Plugins::Fifo::Queue::Manager.new
          if (!@queues.empty? && @queues.first.start_with?(manager.queue_prefix))
            puts "Fifo Startup - Updating worker list"
            manager.update_workers
          end
        end

        def unregister_worker
          super

          manager = Resque::Plugins::Fifo::Queue::Manager.new
          if (!@queues.empty? && @queues.first.start_with?(manager.queue_prefix))
            puts "Fifo Shutdown - Updating worker list"
            manager.update_workers
          end
        end
      end
    end
  end
end
