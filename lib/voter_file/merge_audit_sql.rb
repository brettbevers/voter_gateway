module VoterFile
  module MergeAuditSql
    MATCH_AUDIT_NAME = "working_exact_match_group"
    TARGET_KEY_NAME = VoterFile::CSVDriver::RecordMatcher::TARGET_KEY_NAME

    def source_total_count_sql
      "SELECT COUNT(*) FROM #{working_source_table.name};"
    end

    def matched_count_sql(column_group, index)
      %Q{
        SELECT COUNT(*)
          FROM #{working_source_table.name} s
          WHERE s.#{MATCH_AUDIT_NAME} = #{index};}
    end

    def matched_count_commands
      exact_match_groups.each_with_index.map{|group, index| [group, matched_count_sql(group, index)] }
    end

    def symmetric_merge_count_sql
      primary_key = target_table.primary_key
      %Q{
        SELECT COUNT(*)
          FROM #{working_source_table.name} s1, #{working_source_table.name} s2
          WHERE s1.#{TARGET_KEY_NAME} = s2.#{primary_key}
            AND s2.#{TARGET_KEY_NAME} IS NOT NULL
            AND s1.#{primary_key} <> s2.#{primary_key}; }
    end

    def reflexive_merge_count_sql
      %Q{
        SELECT COUNT(*)
          FROM #{working_source_table.name} s1
          WHERE s1.#{TARGET_KEY_NAME} = s1.#{target_table.primary_key}; }
    end

    def create_working_source_table_sql
      sql = super
      sql + "\nALTER TABLE #{working_source_table.name} ADD COLUMN #{MATCH_AUDIT_NAME} INT;"
    end

    def find_exact_match_commands
      exact_match_groups.each_with_index.map{|group, index| find_exact_match_sql(group, index) }
    end

    def find_exact_match_sql(column_group, column_group_index)
      sql = super(column_group)
      sql.gsub("SET #{TARGET_KEY_NAME} = t.#{target_table.primary_key}",
               "SET #{TARGET_KEY_NAME} = t.#{target_table.primary_key}, #{MATCH_AUDIT_NAME} = #{column_group_index}")
    end
  end
end