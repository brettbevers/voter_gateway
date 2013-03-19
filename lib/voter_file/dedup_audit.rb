module VoterFile

  class DedupAudit < DedupDriver
    attr_reader :audit_results

    def initialize
      super
      @audit_results = []
      @merge_duplicates_adapter = DedupAuditMerger
    end

    def merge_duplicates
      deduper = super
      audit_merge(deduper)
      return deduper
    end

    def audit_merge(deduper)
      audit_result = DedupAuditResult.new

      # total records in source table
      audit_result.total_source_records = get_count( deduper.source_total_count_sql )

      # number that matched records
      deduper.matched_count_commands.each do |match_group, sql|
        count = get_count(sql)
        percentage = count/audit_result.total_source_records.to_f
        audit_result.match_counts << [match_group, count, percentage]
      end

      # count of records that are merged reflexively
      audit_result.reflexive_merges = get_count( deduper.reflexive_merge_count_sql )

      # count of records that are merged symmetrically
      audit_result.symmetric_merges = get_count( deduper.symmetric_merge_count_sql )

      audit_results << audit_result
    end

    class DedupAuditResult < MergeAudit::AuditResult
      attr_accessor :reflexive_merges, :symmetric_merges

      def to_s
        report = super
        unless reflexive_merges == 0
          report << "REFLEXIVE MERGE ERROR: #{reflexive_merges} records are flagged as their own duplicate.\n"
        end

        unless symmetric_merges == 0
          report << "SYMMETRIC MERGE ERROR: #{symmetric_merges} records are flagged as both a duplicate and original.\n"
        end

        return report
      end
    end

    class DedupAuditMerger < DedupDriver::RecordDeduper
      include MergeAuditSql

      def dedup_commands
        []
      end

    end
  end
end