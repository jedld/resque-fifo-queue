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
        "fifo_key" => "key1",}]
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
            [{"class"=>"TestJob", "args"=>[2], "fifo_key"=>"key4"}],
            [],
            [],
            [{"class"=>"TestJob", "args"=>[{}], "fifo_key"=>"key1"},
             {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key1"}]])
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
                [{"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key4"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key5"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key6"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key9"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key12"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key13"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key25"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key29"}],

                #Slice 1602258586
                [{"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key0"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key2"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key3"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key7"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key8"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key10"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key15"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key17"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key18"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key19"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key21"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key24"}],

                #Slice 3332007442
                [{"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key11"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key16"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key20"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key26"}],

                #Slice 3829916704
                [{"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key1"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key14"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key22"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key23"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key27"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key28"},
                 {"class"=>"TestJob", "args"=>[2], "fifo_key"=>"key14"}],
                ])
            end

            it "inserting a queue in the middle" do
              allow_any_instance_of(Resque::Plugins::Fifo::Queue::Manager).to receive(:generate_new_slice).and_return(2602258586)

              worker = Resque::Plugins::Fifo::Worker.new
              worker.register_worker

              expect(manager.dump_queues_sorted).to eq([
                #Slice 621716388
                [{"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key4"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key5"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key6"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key9"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key12"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key13"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key25"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key29"}],

                #Slice 1602258586
                [{"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key2"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key3"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key10"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key15"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key17"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key18"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key19"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key24"}],

                #Slice 2602258586
                [{"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key0"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key7"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key8"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key21"}],

                #Slice 3332007442
                [{"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key11"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key16"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key20"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key26"}],

                #Slice 3829916704
                [{"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key1"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key14"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key22"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key23"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key27"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key28"},
                 {"class"=>"TestJob", "args"=>[2], "fifo_key"=>"key14"}]
              ])
            end

            it "inserting a queue in front" do
              allow_any_instance_of(Resque::Plugins::Fifo::Queue::Manager).to receive(:generate_new_slice).
                and_return(121716388)

              worker = Resque::Plugins::Fifo::Worker.new
              worker.register_worker

              expect(manager.dump_queues_sorted).to eq([
                #Slice 121716388
                [{"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key1"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key22"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key27"}],

                #Slice 621716388
                [{"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key4"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key5"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key6"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key9"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key12"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key13"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key25"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key29"}],

                #Slice 1602258586
                [{"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key0"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key2"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key3"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key7"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key8"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key10"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key15"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key17"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key18"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key19"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key21"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key24"}],

                #Slice 3332007442
                [{"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key11"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key16"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key20"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key26"}],

                #Slice 3829916704
                [{"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key14"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key23"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key28"},
                 {"class"=>"TestJob", "args"=>[2], "fifo_key"=>"key14"}],
                ])
            end

            it "inserting a queue at the back" do
              allow_any_instance_of(Resque::Plugins::Fifo::Queue::Manager).to receive(:generate_new_slice).
                and_return(7829916704)

              worker = Resque::Plugins::Fifo::Worker.new
              worker.register_worker

              expect(manager.dump_queues_sorted).to eq([
                #Slice 621716388
                [{"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key4"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key5"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key6"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key9"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key12"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key13"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key25"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key29"}],

                #Slice 1602258586
                [{"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key0"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key2"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key3"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key7"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key8"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key10"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key15"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key17"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key18"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key19"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key21"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key24"}],

                #Slice 3332007442
                [{"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key11"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key16"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key20"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key26"}],

                #Slice 3829916704
                [{"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key23"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key28"}],

                #Slice 7829916704
                [{"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key1"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key14"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key22"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key27"},
                 {"class"=>"TestJob", "args"=>[2], "fifo_key"=>"key14"}],
              ])
            end
          end

          context "queue deletion" do
            it "reassigns queues" do
              queue_names = manager.dump_queue_names

              worker = manager.worker_for_queue(queue_names[0])
              worker.unregister_worker
              manager.update_workers

              expect(manager.dump_queues_sorted).to eq([
                #Slice 1602258586
                [{"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key0"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key2"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key3"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key7"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key8"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key10"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key15"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key17"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key18"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key19"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key21"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key24"}],

                #Slice 3332007442
                [{"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key11"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key16"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key20"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key26"}],

                #Slice 3829916704
                [{"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key1"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key14"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key22"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key23"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key27"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key28"},
                 {"class"=>"TestJob", "args"=>[2], "fifo_key"=>"key14"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key4"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key5"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key6"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key9"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key12"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key13"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key25"},
                 {"class"=>"TestJob", "args"=>[1], "fifo_key"=>"key29"}],
              ])
            end
          end
        end
      end
    end
  end
end
