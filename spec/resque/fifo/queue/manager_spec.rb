require "spec_helper"
require "pry-byebug"

class TestJob
  @queue = :file_serve

  def self.perform(param)
  end
end

RSpec.describe Resque::Plugins::Fifo::Queue::Manager do
  before do
    srand(67809)

    allow_any_instance_of(Resque::Plugins::Fifo::Queue::Manager).to receive(:inline?).and_return(false)
    allow(Resque).to receive(:inline?).and_return(true)
  end

  around(:each) do |example|
    @timestamp = Time.now
    @timestamp_ts = @timestamp.to_i
    Timecop.freeze(@timestamp) do
      example.run
    end
  end

  it "has a version number" do
    Resque::Plugin.lint(Resque::Plugins::Fifo)
    expect(Resque::Plugins::Fifo::Queue::VERSION).not_to be nil
  end

  let(:manager) { Resque::Plugins::Fifo::Queue::Manager.new }

  context "#dump_dht" do
    it "dumps queue dictionary" do
      expect(manager.dump_dht).to eq []
    end
  end

  context "no workers" do
    it "sets to the pending queue" do
      manager.enqueue("key1", TestJob, {})
      expect(Resque.all_queues).to eq [manager.pending_queue_name]
      expect(manager.peek_pending).to eq(
        [{"args" => [{}],
        "class" => "TestJob",
        "fifo_key" => "key1",
        "enqueue_ts" => @timestamp_ts}]
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
      rand_name = rand(0..2**32).to_s
      @worker = Resque::Plugins::Fifo::Worker.new
      @worker.register_worker
    end

    it "has a worker and a queue" do
      expect(Resque.queues).to eq []
      expect(Resque.workers).to eq [@worker]
    end

    context ".update_workers" do
      it "creates a random queue" do
        expect(manager.dump_dht).to eq [[621716388, @worker.main_queue_name]]
      end

      context "multiple workers" do
        before do
          @queue_names = []
          @workers = []

          3.times do |i|
            worker = Resque::Plugins::Fifo::Worker.new
            worker.register_worker
            @queue_names << worker.main_queue_name
            @workers << worker
          end
        end

        it "creates a random queue" do
          dht = manager.dump_dht
          expected = [
            [ 621716388, dht[0][1]],
            [ 1602258586, dht[1][1]],
            [ 3332007442, dht[2][1]],
            [ 3829916704, dht[3][1]],
          ]
          expect(dht).to eq expected
        end

        it "enqueueing assigns to one of the queues" do
          manager.enqueue("key1", TestJob, {})
          manager.enqueue("key1", TestJob, 1)

          manager.enqueue("key4", TestJob, 2)
          sorted_queues = manager.dump_queues_sorted

          expect(sorted_queues).to eq([
            [{"class"=>"TestJob", "args"=>[2], "fifo_key"=>"key4", "enqueue_ts" => @timestamp_ts}],
            [],
            [],
            [{"class"=>"TestJob", "args"=>[{}], "fifo_key"=>"key1", "enqueue_ts" => @timestamp_ts},
             {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key1", "enqueue_ts" => @timestamp_ts}]])
        end

        context "consistent hashing test" do
          before do
            30.times do |index|
              Resque::Plugins::Fifo::Queue::Manager.enqueue_to("key#{index}", TestJob, 1)
            end

            Resque::Plugins::Fifo::Queue::Manager.enqueue_to("key14", TestJob, 2)
          end

          context "queue insertion" do
            it "has queue contents" do
              expect(manager.dump_queues_sorted).to eq([
                  #Slice 621716388
                [{"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key4", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key5", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key6", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key9", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key12", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key13", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key25", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key29", "enqueue_ts" => @timestamp_ts}],

                #Slice 1602258586
                [{"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key0", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key2", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key3", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key7", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key8", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key10", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key15", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key17", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key18", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key19", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key21", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key24", "enqueue_ts" => @timestamp_ts}],

                #Slice 3332007442
                [{"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key11", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key16", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key20", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key26", "enqueue_ts" => @timestamp_ts}],

                #Slice 3829916704
                [{"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key1", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key14", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key22", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key23", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key27", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key28", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[2], "fifo_key"=>"key14", "enqueue_ts" => @timestamp_ts}],
                ])
            end

            it "inserting a queue in the middle" do
              allow_any_instance_of(Resque::Plugins::Fifo::Queue::Manager).to receive(:generate_new_slice).and_return(2602258586)

              worker = Resque::Plugins::Fifo::Worker.new
              worker.register_worker

              expect(manager.dump_queues_sorted).to eq([
                #Slice 621716388
                [{"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key4", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key5", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key6", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key9", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key12", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key13", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key25", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key29", "enqueue_ts" => @timestamp_ts}],

                #Slice 1602258586
                [{"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key2", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key3", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key10", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key15", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key17", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key18", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key19", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key24", "enqueue_ts" => @timestamp_ts}],

                #Slice 2602258586
                [{"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key0", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key7", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key8", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key21", "enqueue_ts" => @timestamp_ts}],

                #Slice 3332007442
                [{"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key11", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key16", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key20", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key26", "enqueue_ts" => @timestamp_ts}],

                #Slice 3829916704
                [{"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key1", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key14", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key22", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key23", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key27", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key28", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[2], "fifo_key"=>"key14", "enqueue_ts" => @timestamp_ts}]
              ])
            end

            it "inserting a queue in front" do
              allow_any_instance_of(Resque::Plugins::Fifo::Queue::Manager).to receive(:generate_new_slice).
                and_return(121716388)

              worker = Resque::Plugins::Fifo::Worker.new
              worker.register_worker

              expect(manager.dump_queues_sorted).to eq([
                #Slice 121716388
                [{"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key1", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key22", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key27", "enqueue_ts" => @timestamp_ts}],

                #Slice 621716388
                [{"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key4", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key5", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key6", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key9", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key12", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key13", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key25", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key29", "enqueue_ts" => @timestamp_ts}],

                #Slice 1602258586
                [{"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key0", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key2", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key3", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key7", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key8", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key10", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key15", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key17", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key18", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key19", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key21", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key24", "enqueue_ts" => @timestamp_ts}],

                #Slice 3332007442
                [{"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key11", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key16", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key20", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key26", "enqueue_ts" => @timestamp_ts}],

                #Slice 3829916704
                [{"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key14", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key23", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key28", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[2], "fifo_key"=>"key14", "enqueue_ts" => @timestamp_ts}],
                ])
            end

            it "inserting a queue at the back" do
              manager.clear_stats

              allow_any_instance_of(Resque::Plugins::Fifo::Queue::Manager).to receive(:generate_new_slice).
                and_return(7829916704)

              worker = Resque::Plugins::Fifo::Worker.new
              worker.register_worker

              expect(manager.dump_queues_sorted).to eq([
                #Slice 621716388
                [{"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key4", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key5", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key6", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key9", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key12", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key13", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key25", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key29", "enqueue_ts" => @timestamp_ts}],

                #Slice 1602258586
                [{"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key0", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key2", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key3", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key7", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key8", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key10", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key15", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key17", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key18", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key19", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key21", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key24", "enqueue_ts" => @timestamp_ts}],

                #Slice 3332007442
                [{"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key11", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key16", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key20", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key26", "enqueue_ts" => @timestamp_ts}],

                #Slice 3829916704
                [{"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key23", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key28", "enqueue_ts" => @timestamp_ts}],

                #Slice 7829916704
                [{"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key1", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key14", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key22", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key27", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[2], "fifo_key"=>"key14", "enqueue_ts" => @timestamp_ts}],
              ])

              expect(manager.all_stats).to eq({
                :avg_delay => 0,
                :avg_dht_recalc => 0.0,
                :dht_times_rehashed => "1",
                :max_delay => 0,
              })
            end
          end

          context "queue deletion" do
            it "reassigns queues" do
              queue_names = manager.dump_queue_names
              Resque.push("#{manager.queue_prefix}-orphaned-queue", {"class" => "TestJob", "args" =>[2], "fifo_key" => "orphan1", "enqueue_ts" => @timestamp_ts})
              expect(manager.orphaned_queues).to eq(["fifo-managed-queue-fifo-orphaned-queue"])
              worker = manager.worker_for_queue(queue_names[0])
              worker.unregister_worker
              manager.update_workers


              expect(manager.dump_queues_sorted).to eq([
                #Slice 1602258586
                [{"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key0", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key2", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key3", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key7", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key8", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key10", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key15", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key17", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key18", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key19", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key21", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key24", "enqueue_ts" => @timestamp_ts}],

                #Slice 3332007442
                [{"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key11", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key16", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key20", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key26", "enqueue_ts" => @timestamp_ts}],

                #Slice 3829916704
                [{"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key1", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key14", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key22", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key23", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key27", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key28", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[2], "fifo_key"=>"key14", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[2], "fifo_key"=>"orphan1", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key4", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key5", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key6", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key9", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key12", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key13", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key25", "enqueue_ts" => @timestamp_ts},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key29", "enqueue_ts" => @timestamp_ts}],
              ])

              expect(Resque.all_queues.sort).to eq(manager.dump_queue_names.sort)
            end
          end
        end
      end
    end
  end
end
