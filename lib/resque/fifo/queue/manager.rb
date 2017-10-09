module Resque
  module Fifo
    module Queue
      class Manager
        DLM_TTL = 30000
        attr_accessor :queue_prefix

        def initialize(queue_prefix = 'action')
          @queue_prefix = queue_prefix
        end

        def enqueue(key, klass, args = {})
          queue = compute_queue_name(key)
          Resque.push(queue, :class => klass.to_s, :args => args, fifo_key: key)
        end

        def dump_dht
          slots = redis_client.lrange 'queue_dht', 0, -1
          slots.each_with_index do |slot, index|
            slice, queue = slot.split('#')
            [index, slice, queue]
          end
        end

        def update_workers
          redlock.lock!("action_queue_lock", DLM_TTL) do |lock_info|
            available_queues = query_available_queues
            # query removed workers
            slots = redis_client.lrange 'queue_dht', 0, -1
            current_queues = slots.map { |slot| slot.split('#').second }

            slots.each_with_index do |slot, index|
              slice, queue = slot.split('#')
              if !available_queues.include?(queue)
                Rails.logger.info "queue #{queue} removed."
                transfer_queues(queue, 'pending')
                redis_client.lrem 'queue_dht', -1, slot
              end
            end
            slots = redis_client.lrange('queue_dht', 0, -1)
            added_queues = available_queues.each do |queue|
              if !current_queues.include?(queue)
                insert_slot(slots, queue)
                Rails.logger.info "queue #{queue} was added."
              end
            end

            Rails.logger.info("reinserting items from pending")

            reinsert_pending_items("#{@queue_prefix}-pending")
          end
        end

        private

        def insert_slot(slots, queue)
          new_slice = rand(0..2**32) # generate random 32-bit integer
          queue_str = "#{new_slice}##{queue}"
          slots.each do |slot|
            slice, queue = slot.split('#')
            if new_slice < slice
              redis_client.linsert('queue_dht', 'BEFORE', slot, queue_str)
              return
            end
          end
          redis_client.rpush('queue_dht', queue_str)
        end

        def reinsert_pending_items(from_queue)
          r = Resque.redis
          r.llen("queue:#{from_queue}").times do
            slot = r.lpop "queue:#{from_queue}"
            queue_json = JSON.parse(slot)
            target_queue = compute_queue_name(queue_json['fifo_key'])
            Rails.logger.info "#{from_queue} -> #{target_queue}"
            redis_client.lpush(target_queue, slot)
          end
        end

        def transfer_queues(from_queue, to_queue)
          r = Resque.redis
          r.llen("queue:#{from_queue}").times do
            r.rpoplpush("queue:#{from_queue}", "queue:#{to_queue}")
          end
        end

        def redis_client
          @redis ||= Redis::Namespace.new(:action_queue, :redis => $redis)
        end

        def redlock
          Redlock::Client.new [Rails.application.redis_host]
        end

        def compute_index(key)
          XXhash.xxh32(key)
        end

        def compute_queue_name(key)
          index = compute_index(key)
          slots = redis_client.lrange 'queue_dht', 0, -1

          return "#{@queue_prefix}-pending" if slots.empty?

          slots.each do |slot|
            slice, queue = slot.split('#')
            if index > slice.to_i
              return queue
            end
          end

          return slots.last.split('#').second
        end

        def query_available_queues
          Resque.workers.collect do |worker|
            queue_name = worker.queues.first
            queue_name.starts_with?("#{@queue_prefix}-") ? queue_name : nil
          end.compact
        end
      end
    end
  end
end
