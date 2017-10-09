require "spec_helper"

class TestJob
  @queue = :file_serve

  def self.perform(param)
  end
end

RSpec.describe Resque::Fifo::Queue::Manager do
  before :each do
    srand(67809)
  end

  it "has a version number" do
    expect(Resque::Fifo::Queue::VERSION).not_to be nil
  end

  let(:manager) { Resque::Fifo::Queue::Manager.new }

  context "#dump_dht" do
    it "dumps queue dictionary" do
      expect(manager.dump_dht).to eq []
    end
  end

  context "no workers" do
    it "sets to the pending queue" do
      manager.enqueue("key1", TestJob, {})
      expect(Resque.queues).to eq ["fifo-pending"]
      expect(Resque.peek('fifo-pending')).to eq(
        "args" => {},
        "class" => "TestJob",
        "fifo_key" => "key1",
      )
    end

    context ".update_workers" do
      it "does not do anything" do
        manager.update_workers
        expect(Resque.queues).to eq []
        expect(manager.dump_dht).to eq []
      end
    end
  end

  context "workers" do
    before do
      @queue_name = "fifo-#{SecureRandom.hex(10)}"
      @worker = Resque::Worker.new(@queue_name)
      @worker.register_worker
      manager.update_workers
    end

    it "has a worker and a queue" do
      expect(Resque.queues).to eq []
      expect(Resque.workers).to eq [@worker]
    end

    context ".update_workers" do
      it "creates a random queue" do
        expect(manager.dump_dht).to eq ["2656341485##{@queue_name}"]
      end

      context "multiple workers" do
        before do
          @queue_names = []
          @workers = []

          3.times do |i|
            rand_name = rand(0..2**32).to_s
            queue_name = "fifo-#{i}-#{Digest::MD5.hexdigest(rand_name)}"
            worker = Resque::Worker.new(queue_name)
            worker.register_worker
            @queue_names << queue_name
            @workers << worker
          end
          manager.update_workers
        end

        it "creates a random queue" do
          expected = [
            "419497541##{@queue_names[0]}",
            "2656341485##{@queue_name}",
            "2854829645##{@queue_names[2]}",
            "4097318294##{@queue_names[1]}",
            ]

          expect(manager.dump_dht).to eq expected
        end

        it "enqueueing assigns to one of the queues" do
          manager.enqueue("key1", TestJob, {})
          expect(manager.dump_queues).to eq({})
          expect(manager.dump_queues[@queue_name]).to eq(
            "args" => {},
            "class" => "TestJob",
            "fifo_key" => "key1"
          )
        end
      end
    end
  end
end
