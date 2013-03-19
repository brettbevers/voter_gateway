class VoterFile::CSVDriver::RecordMerger < VoterFile::CSVDriver::RecordMatcher

  attr_accessor :column_map, :merge_expressions, :preserved_columns,
                :excluded_columns, :is_update_only, :is_insert_only

  def initialize(working_source_table, working_target_table)
    super
    @preserved_columns = []
    @excluded_columns = []
    @column_map = {}
    @merge_expressions = {}
  end

  def exclude_column(*col_names)
    self.excluded_columns += col_names.map(&:to_sym)
  end
  alias :exclude_columns :exclude_column

  def preserve_column(*col_name)
    self.preserved_columns += col_name.map(&:to_sym)
  end
  alias :preserve_columns :preserve_column

  def move_columns(col_map)
    col_map.each do |k,v|
      column_map[k.to_sym] = v.to_sym
    end
  end

  def merge_column_as(col_name, expression)
    merge_expressions[col_name.to_sym] = expression
  end

  def update_only
    self.is_update_only = true
  end

  def insert_only
    self.is_insert_only = true
  end

  def merge_commands
    ( match_commands +
    [ update_target_records_sql,
      insert_remaining_sql ]).compact
  end

  def update_target_records_sql
    return nil if is_insert_only

    %Q{
      WITH updated_rows as (
        UPDATE #{target_table.name} t
          SET ( #{update_columns.join(', ')} ) =
            ( #{update_values.join(', ')} )
          FROM #{working_source_table.name} s
          WHERE s.#{TARGET_KEY_NAME} = t.#{target_table.primary_key}
          RETURNING s.#{SOURCE_KEY_NAME}
      ) DELETE FROM #{working_source_table.name} s
          USING updated_rows
          WHERE s.#{SOURCE_KEY_NAME} = updated_rows.#{SOURCE_KEY_NAME}; }
  end

  def insert_remaining_sql
    return nil if is_update_only

    match_conditions = ''
    match_conditions = "AND #{column_constraint_conditions}" if column_constraint_conditions

    %Q{ INSERT INTO #{target_table.name} ( #{insert_columns.join(', ')} )
              SELECT #{insert_columns.join(', ')}
              FROM #{working_source_table.name} s
              WHERE s.#{TARGET_KEY_NAME} IS NULL #{match_conditions}; }
  end

  def update_columns
    column_map.values + merge_expressions.keys + correlated_columns
  end

  def update_values
    column_map.keys.map{|k| "t.#{k}"} + merge_expressions_values + correlated_columns.map{|c| "s.#{c}"}
  end

  def merge_expressions_values
    values = []
    merge_expressions.each do |key, value|
      values << value.gsub('$S', "s.#{key}").gsub('$T', "t.#{key}")
    end
    return values
  end

  def correlated_columns
    source_table.table_column_names - ( excluded_columns + preserved_columns + merge_expressions.keys + column_map.values.map(&:to_sym) )
  end

  def insert_columns
    source_table.table_column_names - excluded_columns
  end
end