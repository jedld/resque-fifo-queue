module Resque
  module Plugins
    module Fifo
      module Queue
        class DrainWorker
          include Resque::Plugins::UniqueJob
          
          def self.perform
            Resque::Plugins::Fifo::Queue::Manager.new.update_workers
          end
        end
      end
    end
  end
end
