class VoterFile::CSVDriver::RecordMerger < VoterFile::CSVDriver::RecordMatcher

  attr_accessor :column_map, :merge_expressions, :insert_expressions, :return_expressions, :preserved_columns,
                :excluded_columns, :is_update_only, :is_insert_only

  def initialize
    super
    @preserved_columns = []
    @excluded_columns = []
    @column_map = {}
    @merge_expressions = {}
    @insert_expressions = {}
    @return_expressions = {}
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
    col_map.each do |k, v|
      column_map[k.to_sym] = v.to_sym
    end
  end

  def merge_column_as(col_name, expression)
    merge_expressions[col_name.to_sym] = expression
  end

  def insert_column_as(col_name, expression)
    insert_expressions[col_name.to_sym] = expression
  end

  def return_value_to_source(return_expression, column)
    return_expressions[return_expression] = column
  end

  def update_only
    self.is_update_only = true
  end

  def insert_only
    self.is_insert_only = true
  end

  def merge_commands
    (match_commands +
        [update_target_records_sql,
         insert_remaining_sql]).compact
  end

  def update_target_records_sql
    return nil if is_insert_only

    match_conditions = ''
    match_conditions = "AND #{merge_constraint_conditions}" if merge_constraint_conditions

    update_sql = %Q{
      UPDATE #{target_table.name} t
        SET ( #{update_columns.join(', ')} ) =
          ( #{update_values.join(', ')} )
        FROM #{working_source_table.name} s
        WHERE s.#{TARGET_KEY_NAME} = t.#{target_table.primary_key} #{match_conditions} }

    unless return_expressions.empty?
      update_sql = %Q{
        WITH rows as (
          #{update_sql}
          RETURNING s.#{source_table.primary_key} #{returned_expressions_list}
        ) UPDATE #{source_table.name} t
            SET ( #{returned_columns_list} ) =
              ( #{returned_values_list} )
            FROM rows
            WHERE rows.#{source_table.primary_key} = t.#{source_table.primary_key}}
    end

    return update_sql
  end

  def insert_remaining_sql
    return nil if is_update_only

    match_conditions = ''
    match_conditions = "AND #{merge_constraint_conditions}" if merge_constraint_conditions

    insert_sql = %Q{
      INSERT INTO #{target_table.name} ( #{insert_columns.join(', ')} )
        SELECT #{insert_values.join(', ')}
        FROM #{working_source_table.name} s
        WHERE s.#{TARGET_KEY_NAME} IS NULL #{match_conditions} }

    unless return_expressions.empty?
      insert_sql = %Q{
        WITH rows as (
          #{insert_sql}
          RETURNING #{source_table.primary_key} #{insert_returned_expressions_list}
        ) UPDATE #{source_table.name} t
            SET ( #{returned_columns_list} ) =
              ( #{returned_values_list} )
            FROM rows
            WHERE rows.#{source_table.primary_key} = t.#{source_table.primary_key} }
    end
    return insert_sql
  end

  def merge_constraint_conditions
    constraints = column_constraints
    constraints.delete_if { |c| c[1].include? '$T' }
    return nil if constraints.empty?
    "( " + column_constraints.map { |c| c[1].gsub('$S', "s.#{c[0]}") }.join(" AND ") + " )"
  end

  def update_columns
    column_map.values + merge_expressions.keys + correlated_update_columns
  end

  def update_values
    column_map.keys.map { |k| "t.#{k}" } + merge_expressions_values + correlated_update_columns.map { |c| "s.#{c}" }
  end

  def merge_expressions_values
    values = []
    merge_expressions.each do |key, value|
      values << value.gsub('$S', "s.#{key}").gsub('$T', "t.#{key}")
    end
    return values
  end

  def correlated_update_columns
    source_table.table_column_names - (excluded_columns + preserved_columns + merge_expressions.keys + column_map.values.map(&:to_sym))
  end

  def insert_columns
    insert_expressions.keys + correlated_insert_columns
  end

  def correlated_insert_columns
    source_table.table_column_names - (excluded_columns + insert_expressions.keys)
  end

  def insert_values
    insert_expressions_values + correlated_insert_columns.map { |c| "s.#{c}"}
  end

  def insert_expressions_values
    values = []
    insert_expressions.each do |key, value|
      values << value.gsub('$S', "s.#{key}")
    end
    return values
  end

  def returned_expressions_list
    list = ''
    return_expressions.each_with_index do |pair, i|
      list << ", #{pair[0]} as col_#{i}"
    end
    return list
  end

  def insert_returned_expressions_list
    returned_expressions_list.gsub('t.', '').gsub('s.', '')
  end

  def returned_values_list
    (0...return_expressions.length).to_a.map { |v| "rows.col_#{v}" }.join(', ')
  end

  def returned_columns_list
    return_expressions.values.join(', ')
  end

end
