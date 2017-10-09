require "spec_helper"

class TestJob
  @queue = :file_serve

  def self.perform(param)
  end
end

RSpec.describe Resque::Fifo::Queue::Manager do
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
      expect(Resque.queues).to eq ["action-pending"]
      expect(Resque.peek('action-pending')).to eq([
        "args" => {},
        "class" => "TestJob",
        "fifo_key" => "key1",
      ])
    end
  end
end
