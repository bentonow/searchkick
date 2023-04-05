module Searchkick
  class RecordIndexer
    attr_reader :record, :index

    def initialize(record)
      @record = record
      if @record.respond_to? 'tenant_searchkick_name'
        index_name = [@record.site_id, @record.class.searchkick_index.name].join('_')
        @index = Searchkick::Index.new(index_name, **record.class.searchkick_options)
      else
        @index = record.class.searchkick_index
      end
    end

    def reindex(method_name = nil, refresh: false, mode: nil)
      unless [:inline, true, nil, :async, :queue].include?(mode)
        raise ArgumentError, "Invalid value for mode"
      end

      mode ||= Searchkick.callbacks_value || index.options[:callbacks] || true

      case mode
      when :queue
        if method_name
          raise Searchkick::Error, "Partial reindex not supported with queue option"
        end

        # always pass routing in case record is deleted
        # before the queue job runs
        if record.respond_to?(:search_routing)
          routing = record.search_routing
        end

        # escape pipe with double pipe
        value = queue_escape(record.id.to_s)
        value = "#{value}|#{queue_escape(routing)}" if routing
        index.reindex_queue.push(value)
      when :async
        unless defined?(ActiveJob)
          raise Searchkick::Error, "Active Job not found"
        end

        # always pass routing in case record is deleted
        # before the async job runs
        if record.respond_to?(:search_routing)
          routing = record.search_routing
        end

        Searchkick::ReindexV2Job.perform_later(
          record.class.name,
          record.id.to_s,
          method_name ? method_name.to_s : nil,
          routing: routing
        )
      else # bulk, inline/true/nil
        reindex_record(method_name)

        index.refresh if refresh
      end
    end

    private

    def queue_escape(value)
      value.gsub("|", "||")
    end

    def reindex_record(method_name)
      if record.destroyed? || !record.persisted? || !record.should_index?
        begin
          index.remove(record)
        rescue Elasticsearch::Transport::Transport::Errors::NotFound
          # do nothing
        end
      else
        if method_name
          index.update_record(record, method_name)
        else
          index.store(record)
        end
      end
    end
  end
end
