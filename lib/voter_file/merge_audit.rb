module VoterFile
  class MergeAudit < CSVDriver
    require 'voter_file/merge_audit_sql'

    attr_reader :audit_results

    def initialize(connection=nil)
      super(connection)
      @audit_results = []
      @audit_result_class = AuditResult
    end

    def merge_records
      @merge_records_adapter = AuditMerger
      merger = super
      audit_merge(merger)
      return merger
    end

    def audit_merge(merger)
      audit_result = AuditResult.new

      # total records in source table
      audit_result.total_source_records = get_count( merger.source_total_count_sql )

      # number that matched records
      merger.matched_count_commands.each do |match_group, sql|
        count = get_count(sql)
        percentage = count/audit_result.total_source_records.to_f
        audit_result.match_counts << [match_group, count, percentage]
      end

      audit_results << audit_result
    end

    class AuditResult
      attr_accessor :total_source_records, :match_counts
      def initialize
        @match_counts = []
      end

      def to_s
        report =  "total source records      : #{total_source_records} \n"
        match_counts.each do |count_stats|
          report << "#{count_stats[0]}: #{(100 *count_stats[2]).round(2)}% #{count_stats[1] } \n"
        end
        return report
      end
    end

    class AuditMerger < CSVDriver::FuzzyMerger
      include VoterFile::MergeAuditSql

      def merge_commands
        match_commands
      end

    end
  end
end
