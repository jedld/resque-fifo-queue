require 'set'

module Resque
  module Fifo
    module Queue
      class Manager
        DLM_TTL = 30000
        attr_accessor :queue_prefix

        def initialize(queue_prefix = 'fifo')
          @queue_prefix = queue_prefix
        end

        def enqueue(key, klass, args = {})
          redlock.lock!("fifo_queue_lock-#{@queue_prefix}", DLM_TTL) do |_lock_info|
            queue = compute_queue_name(key)
            Resque.push(queue, :class => klass.to_s, :args => args, fifo_key: key)
          end
        end

        def dump_dht
          slots = redis_client.lrange 'queue_dht', 0, -1
          slots.each_with_index.collect do |slot, index|
            slice, queue = slot.split('#')
            [slice.to_i, queue]
          end
        end

        def dump_queue_names
          dump_dht.collect { |item| item[1] }
        end

        def worker_for_queue(queue_name)
          Resque.workers.collect do |worker|
            w_queue_name = worker.queues.first
            return worker if w_queue_name == queue_name
          end.compact
          nil
        end

        def dump_queues
          query_available_queues.collect do |queue|
            [queue, Resque.peek(queue,0,0)]
          end.to_h
        end

        def dump_queues_sorted
          queues = dump_queues
          dht = dump_dht.collect do |item|
            _slice, queue_name = item
            queues[queue_name]
          end
        end

        def update_workers
          available_queues = query_available_queues
          # query removed workers
          slots = redis_client.lrange 'queue_dht', 0, -1
          current_queues = slots.map { |slot| slot.split('#')[1] }

          # no change don't update
          return if available_queues.sort == current_queues.sort

          redlock.lock!("fifo_queue_lock-#{@queue_prefix}", DLM_TTL) do |_lock_info|
            remove_list = slots.select do |slot|
              _slice, queue = slot.split('#')
              !available_queues.include?(queue)
            end

            remove_list.each do |slot|
              _slice, queue = slot.split('#')
              log "queue #{queue} removed."
              redlock.lock!("queue_lock-#{queue}", DLM_TTL) do |_lock_info|
                transfer_queues(queue, "#{@queue_prefix}-pending")
                redis_client.lrem 'queue_dht', -1, slot
              end
            end

            added_queues = available_queues.each do |queue|
              if !current_queues.include?(queue)
                insert_slot(queue)
                log "queue #{queue} was added."
              end
            end

            log("reinserting items from pending")

            reinsert_pending_items("#{@queue_prefix}-pending")
          end
        end

        private

        def log(message)
          # puts message
        end

        def insert_slot(queue)
          new_slice = XXhash.xxh32(rand(0..2**32).to_s) # generate random 32-bit integer
          insert_queue_to_slice new_slice, queue
        end

        def insert_queue_to_slice(slice, queue)
          queue_str = "#{slice}##{queue}"
          log "insert #{queue} -> #{slice}"
          slots = redis_client.lrange('queue_dht', 0, -1)

          if slots.empty?
            redis_client.rpush('queue_dht', queue_str)
            return
          end

          _b_slice, prev_queue = slots.last.split('#')
          slots.each do |slot|
            slot_slice, s_queue = slot.split('#')
            if slice < slot_slice.to_i
              redlock.lock!("queue_lock-#{prev_queue}", DLM_TTL) do |_lock_info|
                pause_queues([prev_queue]) do
                  redis_client.linsert('queue_dht', 'BEFORE', slot, queue_str)
                  transfer_queues(prev_queue, "#{@queue_prefix}-pending")
                end
              end
              return
            end

            prev_queue = s_queue
          end

          _slot_slice, s_queue = slots.last.split('#')
          pause_queues([s_queue]) do
            transfer_queues(s_queue, "#{@queue_prefix}-pending")
            redis_client.rpush('queue_dht', queue_str)
          end
        end

        def reinsert_pending_items(from_queue)
          redis_client.llen("queue:#{from_queue}").times do
            slot = redis_client.lpop "queue:#{from_queue}"
            queue_json = JSON.parse(slot)
            target_queue = compute_queue_name(queue_json['fifo_key'])
            log "#{queue_json['fifo_key']}: #{from_queue} -> #{target_queue}"
            redis_client.rpush("queue:#{target_queue}", slot)
          end
        end

        def pause_queues(queue_names = [], &block)
          begin
            queue_names.each do |queue_name|
              worker = worker_for_queue(queue_name)
              worker.pause_processing if worker
            end

            block.()
          ensure
            queue_names.each do |queue_name|
              worker = worker_for_queue(queue_name)
              worker.unpause_processing if worker
            end
          end
        end

        def transfer_queues(from_queue, to_queue)
          log "transfer: #{from_queue} -> #{to_queue}"
          redis_client.llen("queue:#{from_queue}").times do
            redis_client.rpoplpush("queue:#{from_queue}", "queue:#{to_queue}")
          end
        end

        def redis_client
          @redis ||= Resque.redis
        end

        def redlock
          Redlock::Client.new [redis_client]
        end

        def compute_index(key)
          XXhash.xxh32(key)
        end

        def compute_queue_name(key)
          index = compute_index(key)
          slots = redis_client.lrange 'queue_dht', 0, -1

          return "#{@queue_prefix}-pending" if slots.empty?

          slots.reverse.each do |slot|
            slice, queue = slot.split('#')
            if index > slice.to_i
              return queue
            end
          end

          _slice, queue_name = slots.last.split('#')

          queue_name
        end

        def query_available_queues
          Resque.workers.collect do |worker|
            queue_name = worker.queues.first
            queue_name.start_with?("#{@queue_prefix}-") ? queue_name : nil
          end.compact
        end
      end
    end
  end
end
