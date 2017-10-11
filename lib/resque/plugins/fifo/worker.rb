module Resque
  module Plugins
    module Fifo
      class Worker < Resque::Worker
        attr_accessor :main_queue_name

        def queues=(queues)
          queues = queues.empty? ? (ENV["QUEUES"] || ENV['QUEUE']).to_s.split(',') : queues
          main_queue_name = "#{manager.queue_prefix}-#{SecureRandom.hex(10)}"

          @queues = ([main_queue_name] + queues).map { |queue| queue.to_s.strip }
          unless ['*', '?', '{', '}', '[', ']'].any? {|char| @queues.join.include?(char) }
            @static_queues = @queues.flatten.uniq
          end
          validate_queues
        end

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

          if (!@queues.empty? && @queues.first.start_with?(manager.queue_prefix))
            puts "Fifo Shutdown - Updating worker list"
            manager.update_workers
          end
        end

        private

        def manager
          @manager ||=  Resque::Plugins::Fifo::Queue::Manager.new
        end
      end
    end
  end
end
