module VoterFile
  class DedupDriver < CSVDriver

    def initialize
      super
      @merge_duplicates_adapter = RecordDeduper
    end

    def merge_duplicates
      # the record deduper instance requires two working tables
      deduper = @merge_duplicates_adapter.new(create_working_table, create_working_table)
      yield deduper if block_given?

      deduper.apply_merge_orientation

      # find duplicates
      commands = deduper.match_commands
      commands.each do |sql|
        db_connection.execute(sql)
      end

      unless deduper.is_a? DedupAudit::DedupAuditMerger
        # ensure that a record is not matched to itself and that matched records are not merged twice
        reflexive_merge_count = get_count(deduper.reflexive_merge_count_sql)
        raise "#{reflexive_merge_count} records are mistakenly flagged as their own duplicate." unless reflexive_merge_count == 0
        symmetric_merge_count = get_count(deduper.symmetric_merge_count_sql)
        raise "#{symmetric_merge_count} records are mistakenly flagged as both a duplicate and original." unless symmetric_merge_count == 0
      end

      # perform merge and delete duplicate record
      deduper.dedup_commands.each do |sql|
        db_connection.execute(sql)
      end

      return deduper
    end

    class RecordDeduper < CSVDriver::RecordMerger
      include MergeAuditSql

      attr_accessor :merge_orientation

      def initialize(working_source_table, working_target_table)
        super
        @merge_orientation = []
      end

      # when deduping, the target and source is the same table.
      def source_table
        target_table
      end

      def set_merge_orientation(col_name, constraint)
        merge_orientation << [col_name, constraint]
      end

      def apply_merge_orientation
        if merge_orientation.empty?
          constrain_column target_table.primary_key, "$T < $S"
        else
          self.column_constraints += merge_orientation
        end
      end

      def dedup_commands
        [merge_duplicates_sql]
      end

      def merge_duplicates_sql
        primary_key = target_table.primary_key
        %Q{
          WITH duplicate_rows as (
            DELETE FROM #{source_table.name} s
              USING #{working_source_table.name} ws
              WHERE ws.#{TARGET_KEY_NAME} IS NOT NULL AND s.#{primary_key} = ws.#{primary_key}
              RETURNING ws.*
          ) UPDATE #{target_table.name} t
              SET ( #{update_columns.join(', ')} ) =
                ( #{update_values.join(', ')} )
              FROM duplicate_rows s
              WHERE s.#{TARGET_KEY_NAME} = t.#{primary_key}; }
      end
    end
  end
end