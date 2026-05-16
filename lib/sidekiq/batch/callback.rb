module Sidekiq
  class Batch
    module Callback
      class Worker
        include Sidekiq::Job

        def perform(clazz, event, opts, bid)
          return unless %w(success complete).include?(event)
          clazz, method = clazz.split("#") if (clazz && clazz.class == String && clazz.include?("#"))
          method = "on_#{event}" if method.nil?
          status = Sidekiq::Batch::Status.new(bid)

          if clazz && object = Object.const_get(clazz)
            instance = object.new
            instance.send(method, status, opts) if instance.respond_to?(method)
          end
        end
      end

    end
  end
end
