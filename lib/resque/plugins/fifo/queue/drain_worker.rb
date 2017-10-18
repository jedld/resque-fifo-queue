module Resque
  module Plugins
    module Fifo
      module Queue
        class DrainWorker
          include Resque::Plugins::UniqueJob
          @lock_after_execution_period = 30

          def self.perform
            Resque::Plugins::Fifo::Queue::Manager.new.update_workers
          end
        end
      end
    end
  end
end
