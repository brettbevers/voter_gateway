class VoterFile::CSVDriver::RecordMatcher

  attr_accessor :target_table, :source_table, :fuzzy_match_columns, :exact_match_groups,
                :column_constraints, :working_source_table, :working_target_table

  FUZZY_MATCH_LIMIT = 0.5
  SOURCE_KEY_NAME = "working_source_id"
  TARGET_KEY_NAME = "working_target_id"

  def initialize(working_source_table, working_target_table)
    @exact_match_groups = []
    @fuzzy_match_columns = []
    @column_constraints = []
    @working_source_table = working_source_table
    @working_target_table = working_target_table
  end

  def exact_match_group(*col_names)
    self.exact_match_groups << col_names.map(&:to_sym)
  end

  def fuzzy_match_column(*col_names)
    self.fuzzy_match_columns += col_names.map(&:to_sym)
  end

  def constrain_column(col_name, constraint)
    column_constraints << [col_name, constraint]
  end

  def match_commands
    raise "Target table must have a primary key" unless target_table.primary_key

    ([create_working_source_table_sql,
      create_working_target_table_sql ] +
      find_exact_match_commands +
      find_fuzzy_match_commands).compact
  end

  def create_working_source_table_sql
    name = working_source_table.name
    source_name = source_table.name
    %Q{
      DROP TABLE IF EXISTS #{name};
      CREATE TABLE #{name} ( LIKE #{source_name} );
      ALTER TABLE #{name} ADD COLUMN #{SOURCE_KEY_NAME} SERIAL;
      ALTER TABLE #{name} ADD COLUMN #{TARGET_KEY_NAME} #{target_table.primary_key_type};
      INSERT INTO #{name} ( SELECT * from #{source_name} );}
  end

  def create_working_target_table_sql
    name = working_target_table.name
    sql = %Q{
            DROP TABLE IF EXISTS #{name};
            CREATE TABLE #{name} ( #{target_table.primary_key} #{target_table.primary_key_type} ); }

    select_columns = target_table.primary_key.to_s

    fuzzy_match_columns.each do |col|
      sql += "\nALTER TABLE #{name} ADD COLUMN #{col} TEXT;"
      sql += "\nCREATE INDEX ON #{name} USING gist (#{col} gist_trgm_ops);"
      select_columns += ", #{col}"
    end

    sql + "\nINSERT INTO #{name} ( SELECT #{select_columns} FROM #{target_table.name} );"
  end

  def find_exact_match_commands
    exact_match_groups.map{|g| find_exact_match_sql(g) }
  end

  def find_exact_match_sql(column_group)
    match_conditions = exact_match_conditions(column_group)
    match_conditions += " AND #{column_constraint_conditions}" if column_constraint_conditions

    %Q{
      WITH updated_rows as (
        UPDATE #{working_source_table.name} s
          SET #{TARGET_KEY_NAME} = t.#{target_table.primary_key}
          FROM #{target_table.name} t
          WHERE s.#{TARGET_KEY_NAME} IS NULL AND #{match_conditions}
          RETURNING s.#{TARGET_KEY_NAME}
      ) DELETE FROM #{working_target_table.name} t
          USING updated_rows
          WHERE t.#{target_table.primary_key} = updated_rows.#{TARGET_KEY_NAME};}
  end

  def find_fuzzy_match_commands
    fuzzy_match_columns.map{|c| find_fuzzy_match_sql(c) }
  end

  # merge hit record from working table into target table, and then remove hit record from working table
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

  def exact_match_conditions(column_group)
    conditions = column_group.map do |c|
      "( s.#{c} = t.#{c} AND t.#{c} IS NOT NULL )"
    end
    "( #{conditions.join(" AND ")} )"
  end

  def column_constraint_conditions
    return nil if column_constraints.empty?
    "( " + column_constraints.map{|c| c[1].gsub('$S', "s.#{c[0]}").gsub('$T', "t.#{c[0]}") }.join(" AND ") + " )"
  end

end