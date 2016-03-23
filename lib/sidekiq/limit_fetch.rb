require 'forwardable'
require 'sidekiq'
require 'sidekiq/manager'
require 'sidekiq/api'

module Sidekiq::LimitFetch
  autoload :UnitOfWork, 'sidekiq/limit_fetch/unit_of_work'

  require_relative 'limit_fetch/instances'
  require_relative 'limit_fetch/queues'
  require_relative 'limit_fetch/global/semaphore'
  require_relative 'limit_fetch/global/selector'
  require_relative 'limit_fetch/global/monitor'
  require_relative 'extensions/queue'
  require_relative 'extensions/manager'

  extend self

  SLEEP_IF_NO_WORK = (ENV['SLEEP_IF_NO_WORK'] || 15).to_s.to_i

  def new(_)
    self
  end

  def retrieve_work
    queue, job = redis_brpop *Queues.acquire, Sidekiq::BasicFetch::TIMEOUT
    Queues.release_except(queue)
    if job
      UnitOfWork.new(queue, job)
    else
      if SLEEP_IF_NO_WORK > 0
        sleep SLEEP_IF_NO_WORK
      end

      nil
    end
  end

  def bulk_requeue(*args)
    Sidekiq::BasicFetch.bulk_requeue(*args)
  end

  private

  def redis_brpop(*args)
    return if args.size < 2
    Sidekiq.redis {|it| it.brpop *args }
  end
end
