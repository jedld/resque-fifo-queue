# Resque::Fifo::Queue

Implementation of a sharded First-in First-out queue using Redis and Resque.

This gem unables you to guarantee in-order job processing based on a shared key. Useful for business requirements that are race-condition prone. Or for a set of related jobs that require preservation of chronological order.

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

```
rake resque:fifo-worker
rake resque:fifo-workers
```

Supports the same parameters as resque:work but creates additional queues behind the scenes
to support fifo queues.


Sample Usage
------------

To start a job using a fifo strategy:

```ruby
class SampleJob
  def self.perform(*args)
    # run your resque job here
  end
end

shared_key = "user_00001"
Resque::Plugins::Fifo::Queue::Manager.enqueue_to(shared_key, SampleJob, "hello")
```

## Resque web

This gem adds a FIFO_queue tab under resque web where you can see information about
workers and queues used. This can be accessed via:

http://localhost:3000/resque/fifo_queue

## Development

After checking out the repo, run `bin/setup` to install dependencies. Then, run `rake spec` to run the tests. You can also run `bin/console` for an interactive prompt that will allow you to experiment.

To install this gem onto your local machine, run `bundle exec rake install`. To release a new version, update the version number in `version.rb`, and then run `bundle exec rake release`, which will create a git tag for the version, push git commits and tags, and push the `.gem` file to [rubygems.org](https://rubygems.org).

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/[USERNAME]/resque-fifo-queue. This project is intended to be a safe, welcoming space for collaboration, and contributors are expected to adhere to the [Contributor Covenant](http://contributor-covenant.org) code of conduct.


## License

The gem is available as open source under the terms of the [MIT License](http://opensource.org/licenses/MIT).
