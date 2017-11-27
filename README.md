# Resque::Fifo::Queue

Implementation of a sharded First-in First-out queue using Redis and Resque.

This gem unables you to guarantee in-order job processing based on a shard key. Useful for business requirements that are race-condition prone or needs something processed in a streaming manner (jobs that require preservation of chronological order).

Sharding is automatically managed depending on the number of workers available. Durability is guaranteed with failover resharding using a consitent hash. Built on the reliability of resque and is pure ruby which simplifies deployment if you are already using resque with ruby on rails.

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'resque-fifo-queue'
```

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install resque-fifo-queue

## Usage

This adds a new task, to run fifo queues:

```bash
rake resque:fifo-worker
rake resque:fifo-workers
```

Available options are similar to rake resque:work

Workers will assign their own queue names which is automatically managed by the sharding algorithm.

Supports the same parameters as resque:work but creates additional queues behind the scenes
to support fifo queues.

fifo workers can also double duty as standard resque worker in order to simplify resource sharing:

```bash
QUEUE=high rake resque:fifo-worker
```

Aside from being a worker that processes jobs from the fifo queue it will process jobs from the high queue as well.

Sample Usage
------------

To start a job using a fifo strategy:

```ruby
class SampleJob
  def self.perform(*args)
    # run your resque job here
  end
end

shard_key = "user_00001"

# These async jobs will be guaranteed to run one after another in a single worker

Resque::Plugins::Fifo::Queue::Manager.enqueue_to(shard_key, SampleJob, "hello")
Resque::Plugins::Fifo::Queue::Manager.enqueue_to(shard_key, SampleJob, "hello1")

```

## Resque web extensions

This gem adds a FIFO_queue tab under resque web where you can see information about
workers and queues used. This can be accessed via:

http://localhost:3000/resque/fifo_queue

## Ensuring workers are updated

Since only one worker is assigned to a particular shard, it is important to make sure a worker is running otherwise jobs will
pile up for that shard. The shard table is updated everytime a worker is started or stopped properly (using kill -S QUITE). However
in exceptional cases (workers are forced killed for example), a shard may be left without a worker. In cases like this a scheduled task is necessary to make sure the worker list is constantly updated:

```yaml
# resque_schedule.yml
auto_refresh_fifo_queues:
  cron: '*/5 * * * * UTC'
  class: Resque::Plugins::Fifo::Queue::DrainWorker
  queue: fifo_refresh
  description: 'Check if fifo workers are still valid and update worker table'
```

Do make sure to add the resque-scheduler as well https://github.com/resque/resque-scheduler

## Related Projects

If you need something more performant and robust, Apache Kafka is still the way to go. Though you need a couple more libraries
to set it up with rails.

https://github.com/karafka/karafka

## Development

After checking out the repo, run `bin/setup` to install dependencies. Then, run `rake spec` to run the tests. You can also run `bin/console` for an interactive prompt that will allow you to experiment.

To install this gem onto your local machine, run `bundle exec rake install`. To release a new version, update the version number in `version.rb`, and then run `bundle exec rake release`, which will create a git tag for the version, push git commits and tags, and push the `.gem` file to [rubygems.org](https://rubygems.org).

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/[USERNAME]/resque-fifo-queue. This project is intended to be a safe, welcoming space for collaboration, and contributors are expected to adhere to the [Contributor Covenant](http://contributor-covenant.org) code of conduct.

## License

The gem is available as open source under the terms of the [MIT License](http://opensource.org/licenses/MIT).
