module Searchkick
  class ProcessQueueJob < ActiveJob::Base
    queue_as { Searchkick.queue_name }

    def perform(class_name:, index_name: nil, inline: false, secondary_index_name: nil)
      model = class_name.constantize
      limit = model.searchkick_options[:batch_size] || 1000

      loop do
        record_ids = model.searchkick_index(name: index_name).reindex_queue.reserve(limit: limit)
        if record_ids.any?
          batch_options = {
            class_name: class_name,
            record_ids: record_ids,
            index_name: index_name
          }

          if inline
            # use new.perform to avoid excessive logging
            Searchkick::ProcessBatchJob.new.perform(**batch_options)
            if secondary_index_name
              batch_options[:index_name] = secondary_index_name
              Searchkick::ProcessBatchJob.new.perform(**batch_options)
            end
          else
            Searchkick::ProcessBatchJob.perform_later(**batch_options)
            if secondary_index_name
              batch_options[:index_name] = secondary_index_name
              Searchkick::ProcessBatchJob.perform_later(**batch_options)
            end
          end

          # TODO when moving to reliable queuing, mark as complete
        end
        break unless record_ids.size == limit
      end
    end
  end
end
