require 'resque/server'

module Resque
  module Plugins
    module Fifo
      module Server
        VIEW_PATH = File.join(File.dirname(__FILE__), 'server', 'views')

        module Helpers

        end

        class << self
          def registered(app)
            app.get '/fifo_queues' do
              @manager = Resque::Plugins::Fifo::Queue::Manager.new
              @queue_with_slices = @manager.dump_queues_with_slices
              erb(File.read(File.join(VIEW_PATH, 'fifo_queues.erb')))
            end

            app.post '/shared_finder' do
              @manager = Resque::Plugins::Fifo::Queue::Manager.new
              @queue_name = @manager.compute_queue_name(params[:key])
              @worker = @manager.worker_for_queue(@queue_name)
              erb(File.read(File.join(VIEW_PATH, 'shared_finder.erb')))
            end
            # We have little choice in using this funky name - Resque
            # already has a "Stats" tab, and it doesn't like
            # tab names with spaces in it (it translates the url as job%20stats)
            app.tabs << "FIFO_Queues"

            app.helpers(Helpers)
          end
        end
      end
    end
  end
end

Resque::Server.register Resque::Plugins::Fifo::Server
