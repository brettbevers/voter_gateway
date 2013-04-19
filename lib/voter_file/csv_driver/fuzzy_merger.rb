class VoterFile::CSVDriver::FuzzyMerger < VoterFile::CSVDriver::RecordMerger

  attr_accessor :fuzzy_match_columns, :working_target_table

  FUZZY_MATCH_LIMIT = 0.5

  def initialize(&block)
    super
    @fuzzy_match_columns = []
    @working_target_table = block.call
  end

  def fuzzy_match_column(*col_names)
    self.fuzzy_match_columns += col_names.map(&:to_sym)
  end

  def match_commands
    ([create_working_target_table_sql] + super + find_fuzzy_match_commands).compact
  end

  def create_working_target_table_sql
    name = working_target_table.name
    sql = %Q{
            DROP TABLE IF EXISTS #{name};
            CREATE TEMPORARY TABLE #{name} ( #{target_table.primary_key} #{target_table.primary_key_type} ); }

    select_columns = target_table.primary_key.to_s

    fuzzy_match_columns.each do |col|
      sql += "\nALTER TABLE #{name} ADD COLUMN #{col} TEXT;"
      sql += "\nCREATE INDEX ON #{name} USING gist (#{col} gist_trgm_ops);"
      select_columns += ", #{col}"
    end

    sql + "\nINSERT INTO #{name} ( SELECT #{select_columns} FROM #{target_table.name} );"
  end

  def find_exact_match_sql(column_group)
    %Q{
      WITH updated_rows as (
        #{super}
          RETURNING s.#{TARGET_KEY_NAME}
      ) DELETE FROM #{working_target_table.name} t
          USING updated_rows
          WHERE t.#{target_table.primary_key} = updated_rows.#{TARGET_KEY_NAME};}
  end

  def find_fuzzy_match_commands
    fuzzy_match_columns.map{|c| find_fuzzy_match_sql(c) }
  end

  def find_fuzzy_match_sql(column)
    match_conditions = (column_constraint_conditions ? "AND #{column_constraint_conditions}" : '')
    %Q{
      WITH updated_rows as (
        UPDATE #{working_source_table.name} s
          SET #{TARGET_KEY_NAME} =
            ( SELECT t.#{target_table.primary_key}
                FROM #{working_target_table.name} t
                WHERE ( s.#{column} <-> t.#{column} ) < #{FUZZY_MATCH_LIMIT}
                ORDER BY s.#{column} <-> t.#{column}
                LIMIT 1 )
          WHERE s.#{TARGET_KEY_NAME} IS NULL #{match_conditions}
          RETURNING s.#{TARGET_KEY_NAME}
      ) DELETE FROM #{working_target_table.name} t
          USING updated_rows
          WHERE t.#{target_table.primary_key} = updated_rows.#{TARGET_KEY_NAME}; }
  end
end