require 'sidekiq'
require 'sidekiq/util'
require 'sidekiq/api'
require 'forwardable'

class Sidekiq::LimitFetch
  autoload :UnitOfWork, 'sidekiq/limit_fetch/unit_of_work'

  require_relative 'limit_fetch/redis'
  require_relative 'limit_fetch/singleton'
  require_relative 'limit_fetch/queues'
  require_relative 'limit_fetch/global/semaphore'
  require_relative 'limit_fetch/global/selector'
  require_relative 'limit_fetch/global/monitor'
  require_relative 'extensions/queue'

  include Redis
  Sidekiq.options[:fetch] = self

  NAP_IF_NO_WORK = (ENV['NAP_IF_NO_WORK'] || 0).to_s.to_i

  def self.bulk_requeue(*args)
    Sidekiq::BasicFetch.bulk_requeue *args
  end

  def initialize(options)
    @queues = Queues.new options.merge(namespace: determine_namespace)
    Global::Monitor.start! @queues
  end

  def retrieve_work
    queue, message = fetch_message
    work = UnitOfWork.new queue, message if message
    if work == nil && NAP_IF_NO_WORK > 0
      # puts "Nothing to do, I take a nap.."
      sleep NAP_IF_NO_WORK
    end
    work
  end

  private

  def fetch_message
    queue, _ = redis_brpop *@queues.acquire, Sidekiq::Fetcher::TIMEOUT
  ensure
    @queues.release_except queue
  end

  def redis_brpop(*args)
    return if args.size < 2
    query = -> redis { redis.brpop *args }

    if busy_local_queues.any? {|queue| not args.include? queue.rname }
      nonblocking_redis(&query)
    else
      redis(&query)
    end
  end

  def busy_local_queues
    Sidekiq::Queue.instances.select(&:local_busy?)
  end
end
