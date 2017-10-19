require 'resque/server'

module Resque
  module Plugins
    module Fifo
      module Server
        VIEW_PATH = File.join(File.dirname(__FILE__), 'server', 'views')

        module Helpers
          def poll
            if @polling
              text = "Last Updated: #{Time.now.strftime("%H:%M:%S")}"
            else
              text = "<a href='#{u(request.path_info)}.poll' rel='poll'>Live Poll</a>"
            end
            "<p class='poll'>#{text}</p>"
          end

          def show_page(page, layout = true)
            response["Cache-Control"] = "max-age=0, private, must-revalidate"
            begin
              erb(File.read(File.join(VIEW_PATH, page)), {:layout => layout})
            rescue Errno::ECONNREFUSED
              erb :error, {:layout => false}, :error => "Can't connect to Redis! (#{Resque.redis_id})"
            end
          end
        end

        class << self
          def registered(app)
            app.get '/fifo_queues' do
              @refresh_requested = false
              @manager = Resque::Plugins::Fifo::Queue::Manager.new
              @queue_with_slices = @manager.dump_queues_with_slices
              @orphaned_queues = @manager.orphaned_queues
              show_page('fifo_queues.erb')
            end

            app.get '/fifo_queues.poll' do
              @polling = true
              @refresh_requested = false
              @manager = Resque::Plugins::Fifo::Queue::Manager.new
              @orphaned_queues = @manager.orphaned_queues
              @queue_with_slices = @manager.dump_queues_with_slices
              show_page('fifo_queues.erb', false)
            end

            app.post '/shared_finder' do
              @manager = Resque::Plugins::Fifo::Queue::Manager.new
              @queue_name = @manager.compute_queue_name(params[:key])
              @worker = @manager.worker_for_queue(@queue_name)
              erb(File.read(File.join(VIEW_PATH, 'shared_finder.erb')))
            end

            app.post '/request_update' do
              @manager = Resque::Plugins::Fifo::Queue::Manager.new
              @manager.request_refresh
              @refresh_requested = true
              @queue_with_slices = @manager.dump_queues_with_slices
              redirect '/resque/fifo_queues'
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
