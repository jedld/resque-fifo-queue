require "spec_helper"

class TestJob
  @queue = :file_serve

  def self.perform(param)
  end
end

RSpec.describe Resque::Fifo::Queue::Manager do
  before do
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
      rand_name = rand(0..2**32).to_s
      @queue_name = "fifo-#{Digest::MD5.hexdigest(rand_name)}"
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
        expect(manager.dump_dht).to eq [[621716388, "fifo-d9f2e430334150d375cd1a6491b07235"]]
      end

      context "multiple workers" do
        before do
          @queue_names = []
          @workers = []
          srand(67809)

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
          dht = manager.dump_dht
          expected = [
            [ 48850411, dht[0][1]],
            [ 621716388, dht[1][1]],
            [ 1602258586, dht[2][1]],
            [ 3829916704, dht[3][1]],
          ]

          expect(manager.dump_dht).to eq expected
        end

        it "enqueueing assigns to one of the queues" do
          manager.enqueue("key1", TestJob, {})
          manager.enqueue("key1", TestJob, {args: 1})

          manager.enqueue("key4", TestJob, {args: 2})
          sorted_queues = manager.dump_queues_sorted

          expect(sorted_queues).to eq(
          [
            [{"class"=>"TestJob", "args"=>{}, "fifo_key"=>"key1"},
             {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key1"}],

            [{"class"=>"TestJob", "args"=>{"args"=>2}, "fifo_key"=>"key4"}],

            [],

            []])
        end

        context "consistent hashing test" do
          before do
            30.times do |index|
              manager.enqueue("key#{index}", TestJob, {args: 1})
            end

            manager.enqueue("key14", TestJob, {args: 2})

            dht = manager.dump_dht

            @queue_contents = (0..3).collect do |i|
              manager.dump_queues[dht[i][1]]
            end
          end

          context "queue insertion" do
            it "has queue contents" do
              expect(@queue_contents).to eq(
                [
                  # slice 48850411
                  [{"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key1"},
                  {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key22"},
                  {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key27"}],

                  # slice 621716388
                 [{"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key4"},
                  {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key5"},
                  {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key6"},
                  {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key9"},
                  {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key12"},
                  {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key13"},
                  {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key25"},
                  {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key29"}],

                  # slice 1602258586
                 [{"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key0"},
                  {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key2"},
                  {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key3"},
                  {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key7"},
                  {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key8"},
                  {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key10"},
                  {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key11"},
                  {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key15"},
                  {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key16"},
                  {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key17"},
                  {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key18"},
                  {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key19"},
                  {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key20"},
                  {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key21"},
                  {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key24"},
                  {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key26"}],

                  #slice 3829916704
                 [{"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key14"},
                  {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key23"},
                  {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key28"},
                  {"class"=>"TestJob", "args"=>{"args"=>2}, "fifo_key"=>"key14"}]]
                )
            end

            it "inserting a queue in the middle" do
              queue_name = "fifo-2602258586"
              worker = Resque::Worker.new(queue_name)
              worker.register_worker

              manager.send(:insert_queue_to_slice, 2602258586, queue_name)

              expect(Resque.peek("fifo-pending",0,0)).to eq(
                [{"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key0"},
                 {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key2"},
                 {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key3"},
                 {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key7"},
                 {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key8"},
                 {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key10"},
                 {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key11"},
                 {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key15"},
                 {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key16"},
                 {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key17"},
                 {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key18"},
                 {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key19"},
                 {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key20"},
                 {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key21"},
                 {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key24"},
                 {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key26"}]
              )

              manager.send(:reinsert_pending_items, "fifo-pending")

              expect(manager.dump_queues_sorted).to eq([
                [{"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key1"},
                 {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key22"},
                 {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key27"}],

                [{"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key4"},
                 {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key5"},
                 {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key6"},
                 {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key9"},
                 {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key12"},
                 {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key13"},
                 {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key25"},
                 {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key29"}],

                [{"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key2"},
                 {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key3"},
                 {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key10"},
                 {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key15"},
                 {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key17"},
                 {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key18"},
                 {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key19"},
                 {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key24"}],

                [{"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key0"},
                 {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key7"},
                 {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key8"},
                 {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key11"},
                 {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key16"},
                 {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key20"},
                 {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key21"},
                 {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key26"}],

                [{"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key14"},
                 {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key23"},
                 {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key28"},
                 {"class"=>"TestJob", "args"=>{"args"=>2}, "fifo_key"=>"key14"}]]
              )
            end

            it "inserting a queue in front" do
              queue_name = "fifo-18850411"
              worker = Resque::Worker.new(queue_name)
              worker.register_worker

              manager.send(:insert_queue_to_slice, 18850411, queue_name)
              manager.send(:reinsert_pending_items, "fifo-pending")

              expect(manager.dump_queues_sorted).to eq([
                [{"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key14"},
                 {"class"=>"TestJob", "args"=>{"args"=>2}, "fifo_key"=>"key14"}],

                [{"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key1"},
                 {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key22"},
                 {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key27"}],

                [{"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key4"},
                 {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key5"},
                 {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key6"},
                 {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key9"},
                 {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key12"},
                 {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key13"},
                 {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key25"},
                 {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key29"}],

                [{"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key0"},
                 {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key2"},
                 {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key3"},
                 {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key7"},
                 {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key8"},
                 {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key10"},
                 {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key11"},
                 {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key15"},
                 {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key16"},
                 {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key17"},
                 {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key18"},
                 {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key19"},
                 {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key20"},
                 {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key21"},
                 {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key24"},
                 {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key26"}],

                [{"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key23"},
                 {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key28"}]
                ])
            end

            it "inserting a queue at the back" do
              queue_name = "fifo-4829916704"
              worker = Resque::Worker.new(queue_name)
              worker.register_worker

              manager.send(:insert_queue_to_slice, 4829916704, queue_name)
              manager.send(:reinsert_pending_items, "fifo-pending")

              expect(manager.dump_queues_sorted).to eq([
                # slice 48850411
                [{"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key1"},
                {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key22"},
                {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key27"}],

                # slice 621716388
               [{"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key4"},
                {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key5"},
                {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key6"},
                {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key9"},
                {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key12"},
                {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key13"},
                {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key25"},
                {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key29"}],

                # slice 1602258586
               [{"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key0"},
                {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key2"},
                {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key3"},
                {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key7"},
                {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key8"},
                {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key10"},
                {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key11"},
                {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key15"},
                {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key16"},
                {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key17"},
                {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key18"},
                {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key19"},
                {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key20"},
                {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key21"},
                {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key24"},
                {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key26"}],

                # slice 3829916704
               [{"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key23"},
                {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key28"}],

               # slice 4829916704
               [{"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key14"},
                {"class"=>"TestJob", "args"=>{"args"=>2}, "fifo_key"=>"key14"}]
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
                [{"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key4"},
                 {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key5"},
                 {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key6"},
                 {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key9"},
                 {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key12"},
                 {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key13"},
                 {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key25"},
                 {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key29"}],

                [{"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key0"},
                 {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key2"},
                 {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key3"},
                 {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key7"},
                 {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key8"},
                 {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key10"},
                 {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key11"},
                 {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key15"},
                 {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key16"},
                 {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key17"},
                 {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key18"},
                 {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key19"},
                 {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key20"},
                 {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key21"},
                 {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key24"},
                 {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key26"}],

                [{"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key1"},
                 {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key22"},
                 {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key27"},
                 {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key14"},
                 {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key23"},
                 {"class"=>"TestJob", "args"=>{"args"=>1}, "fifo_key"=>"key28"},
                 {"class"=>"TestJob", "args"=>{"args"=>2}, "fifo_key"=>"key14"}]
              ])
            end
          end
        end
      end
    end
  end
end
