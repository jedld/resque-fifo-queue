require "spec_helper"
require "pry-byebug"

RSpec.describe Resque::Plugins::Fifo::Queue::DrainWorker do
  let(:manager) { Resque::Plugins::Fifo::Queue::Manager.new }

  around(:each) do |example|
    @timestamp = Time.now
    @timestamp_ts = @timestamp.to_i
    Timecop.freeze(@timestamp) do
      example.run
    end
  end

  describe "#perform" do
    before do
      srand(67809)

      allow_any_instance_of(Resque::Plugins::Fifo::Queue::Manager).to receive(:inline?).and_return(false)
      allow(Resque).to receive(:inline?).and_return(true)

      3.times do |i|
        worker = Resque::Plugins::Fifo::Worker.new
        worker.register_worker
      end

      3.times do |index|
        Resque::Plugins::Fifo::Queue::Manager.enqueue_to("key#{index}", TestJob, 1)
      end
    end

    it "runs the refresh" do
      described_class.perform
      expect(manager.dump_queues_sorted).to eq(
        [[{"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key2", "enqueue_ts" => @timestamp_ts}],
         [{"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key0", "enqueue_ts" => @timestamp_ts}],
         [{"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key1", "enqueue_ts" => @timestamp_ts}]]
      )
    end
  end
end
