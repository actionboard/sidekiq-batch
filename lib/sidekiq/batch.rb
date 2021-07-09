require 'securerandom'
require 'sidekiq'

require 'sidekiq/batch/callback'
require 'sidekiq/batch/middleware'
require 'sidekiq/batch/status'
require 'sidekiq/batch/version'

module Sidekiq
  class Batch
    class NoBlockGivenError < StandardError
    end

    BID_EXPIRE_TTL = 2_592_000

    attr_reader :bid, :description, :callback_queue, :created_at, :current_shard

    def initialize(existing_bid = nil)
      @bid            = existing_bid || SecureRandom.urlsafe_base64(10)
      @existing       = !(!existing_bid || existing_bid.empty?) # Basically existing_bid.present?
      @initialized    = false
      @created_at     = Time.now.utc.to_f
      @bidkey         = "BID-" + @bid.to_s
      @ready_to_queue = []
    end

    def description=(description)
      @description = description
      persist_bid_attr('description', description)
    end

    def callback_queue=(callback_queue)
      @callback_queue = callback_queue
      persist_bid_attr('callback_queue', callback_queue)
    end

    def current_shard=(current_shard)
      @current_shard = current_shard
      persist_bid_attr('current_shard', current_shard)
    end

    def on(event, callback, options = {})
      return unless %w(success complete).include?(event.to_s)
      callback_key = "#{@bidkey}-callbacks-#{event}"
      Sidekiq.redis do |r|
        r.multi do
          r.sadd(callback_key, JSON.unparse({
                                              callback: callback,
                                              opts:     options
                                            }))
          r.expire(callback_key, BID_EXPIRE_TTL)
        end
      end
    end

    def jobs
      raise NoBlockGivenError unless block_given?

      bid_data, Thread.current[:bid_data] = Thread.current[:bid_data], []

      begin
        if !@existing && !@initialized

          Sidekiq.redis do |r|
            r.multi do
              r.hset(@bidkey, "created_at", @created_at)
              r.expire(@bidkey, BID_EXPIRE_TTL)
            end
          end

          @initialized = true
        end

        @ready_to_queue = []

        begin
          parent                 = Thread.current[:batch]
          Thread.current[:batch] = self
          yield
        ensure
          Thread.current[:batch] = parent
        end

        return [] if @ready_to_queue.size == 0

        Sidekiq.redis do |r|
          r.multi do
            r.hincrby(@bidkey, "pending", @ready_to_queue.size)
            r.hincrby(@bidkey, "total", @ready_to_queue.size)
            r.expire(@bidkey, BID_EXPIRE_TTL)

            r.sadd(@bidkey + "-jids", @ready_to_queue)
            r.expire(@bidkey + "-jids", BID_EXPIRE_TTL)
          end
        end

        @ready_to_queue
      ensure
        Thread.current[:bid_data] = bid_data
      end
    end

    def increment_job_queue(jid)
      @ready_to_queue << jid
    end

    def invalidate_all
      Sidekiq.redis do |r|
        r.setex("invalidated-bid-#{bid}", BID_EXPIRE_TTL, 1)
      end
    end

    def valid?(batch = self)
      !Sidekiq.redis { |r| r.exists("invalidated-bid-#{batch.bid}") }
    end

    private

    def persist_bid_attr(attribute, value)
      Sidekiq.redis do |r|
        r.multi do
          r.hset(@bidkey, attribute, value)
          r.expire(@bidkey, BID_EXPIRE_TTL)
        end
      end
    end

    class << self
      def process_failed_job(bid, jid)
        Sidekiq.redis do |r|
          r.multi do
            r.sadd("BID-#{bid}-failed", jid)
            r.expire("BID-#{bid}-failed", BID_EXPIRE_TTL)
          end
        end
      end

      def process_successful_job(bid, jid)
        Sidekiq.redis do |r|
          r.multi do
            r.hincrby("BID-#{bid}", "pending", -1)
            r.srem("BID-#{bid}-failed", jid)
            r.sadd("BID-#{bid}-completed", jid)
            r.expire("BID-#{bid}", BID_EXPIRE_TTL)
            r.expire("BID-#{bid}-completed", BID_EXPIRE_TTL)
          end
        end

        batch_status = Status.new(bid)

        if batch_status.can_queue_callback?
          start_callback(:success, bid)
        end
      end

      def enqueue_callbacks(event, bid)
        batch_status = Status.new(bid)
        return 'Completed' if batch_status.completed?
        if batch_status.can_queue_callback?
          start_callback(event, bid)
        end
      end

      private

      def start_callback(event, bid)
        batch_key    = "BID-#{bid}"
        callback_key = "#{batch_key}-callbacks-#{event}"
        status       = Status.new(bid)
        return if status.completed?
        callbacks, queue, current_shard = Sidekiq.redis do |r|
          r.multi do
            r.smembers(callback_key)
            r.hget(batch_key, "callback_queue")
            r.hget(batch_key, "current_shard")
            r.set("#{batch_key}-callback_completed", 'true')
            r.expire("#{batch_key}-callback_completed", BID_EXPIRE_TTL)
          end
        end
        queue                           ||= "default"
        current_shard                   ||= "default"
        callback_args                   = callbacks.reduce([]) do |memo, jcb|
          cb = Sidekiq.load_json(jcb)
          memo << [cb['callback'], event, cb['opts'], bid]
        end

        Sidekiq.logger.debug { "Enqueue callback bid: #{bid} event: #{event} args: #{callback_args.inspect}" }
        cleanup_redis('bid')
        push_callbacks callback_args, queue, current_shard
      end

      def push_callbacks args, queue, current_shard
        Sidekiq::Client.push_bulk(
          'class' => Sidekiq::Batch::Callback::Worker,
          'args'  => args,
          'queue' => queue,
          'tags'  => [current_shard]
        ) unless args.empty?
      end

      def cleanup_redis(bid)
        Sidekiq.logger.debug { "Cleaning redis of batch #{bid}" }
        Sidekiq.redis do |r|
          r.del(
            "BID-#{bid}",
            "BID-#{bid}-callbacks-complete",
            "BID-#{bid}-completed",
            "BID-#{bid}-callbacks-success",
            "BID-#{bid}-failed",

            "BID-#{bid}-success",
            "BID-#{bid}-complete",
            "BID-#{bid}-jids",
            )
        end
      end
    end
  end
end
