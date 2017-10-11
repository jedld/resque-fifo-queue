Resque.send(:define_singleton_method, :queues) do
  data_store.queue_names.reject { |name| name.start_with?(Resque::Plugins::Fifo::WORKER_QUEUE_NAMESPACE) }
end

Resque.send(:define_singleton_method, :all_queues) do
  data_store.queue_names
end
