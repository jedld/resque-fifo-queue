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
          queue = compute_queue_name(key)
          Resque.push(queue, :class => klass.to_s, :args => args, fifo_key: key)
        end

        def dump_dht
          slots = redis_client.lrange 'queue_dht', 0, -1
          slots.each_with_index.collect do |slot, index|
            slice, queue = slot.split('#')
            [slice.to_i, queue]
          end
        end

        def dump_queues
          query_available_queues.collect do |queue|
            [queue, Resque.peek(queue,0,0)]
          end.to_h
        end

        def dump_queues_sorted
          queues = dump_queues
          dht = dump_dht.collect do |item|
            _slice, queue = item
            queues[queue]
          end
        end

        def update_workers
          redlock.lock!("action_queue_lock", DLM_TTL) do |lock_info|
            available_queues = query_available_queues
            # query removed workers
            slots = redis_client.lrange 'queue_dht', 0, -1
            current_queues = slots.map { |slot| slot.split('#')[1] }

            slots.each_with_index do |slot, index|
              slice, queue = slot.split('#')
              if !available_queues.include?(queue)
                log "queue #{queue} removed."
                transfer_queues(queue, 'pending')
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
          puts message
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
              transfer_queues(prev_queue, "#{@queue_prefix}-pending")
              redis_client.linsert('queue_dht', 'BEFORE', slot, queue_str)
              return
            end

            prev_queue = s_queue
          end

          _slot_slice, s_queue = slots.last.split('#')
          transfer_queues(s_queue, "#{@queue_prefix}-pending")
          redis_client.rpush('queue_dht', queue_str)
        end

        def reinsert_pending_items(from_queue)
          r = Resque.redis
          r.llen("queue:#{from_queue}").times do
            slot = r.lpop "queue:#{from_queue}"
            queue_json = JSON.parse(slot)
            target_queue = compute_queue_name(queue_json['fifo_key'])
            log "#{queue_json['fifo_key']}: #{from_queue} -> #{target_queue}"
            r.lpush("queue:#{target_queue}", slot)
          end
        end

        def transfer_queues(from_queue, to_queue)
          log "transfer: #{from_queue} -> #{to_queue}"
          r = Resque.redis
          r.llen("queue:#{from_queue}").times do
            r.rpoplpush("queue:#{from_queue}", "queue:#{to_queue}")
          end
        end

        def redis_client
          @redis ||= Redis::Namespace.new(:action_queue, :redis => $redis)
        end

        def redlock
          Redlock::Client.new [$redis]
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
