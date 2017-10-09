$LOAD_PATH.unshift File.dirname(__FILE__) + '/../../lib'

task "resque:fifo-worker" => :environment do
  prefix = ENV['PREFIX'] || 'fifo'
  worker = Resque::Worker.new("#{prefix}-#{SecureRandom.hex(10)}")
  worker.prepare
  worker.log "Starting worker #{self}"
  worker.work(ENV['INTERVAL'] || 5) # interval, will block
end

task "resque:fifo-workers" => :environment do
  threads = []

  if ENV['COUNT'].to_i < 1
    abort "set COUNT env var, e.g. $ COUNT=2 rake resque:workers"
  end

  ENV['COUNT'].to_i.times do
    threads << Thread.new do
      system "rake resque:fifo-worker"
    end
  end

  threads.each { |thread| thread.join }
end
