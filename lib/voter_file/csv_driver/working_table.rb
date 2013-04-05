class VoterFile::CSVDriver::WorkingTable

  attr_accessor :name, :table_columns, :target_table, :default_data_type,
                :column_converters, :group_by_expressions, :column_constraints,
                :primary_key, :primary_key_type

  GEOGRAPHIC_SPACIAL_REFERENCE_SYSTEM_ID = 4326

  def initialize(name)
    @name = name
    @table_columns = []
    @column_converters = []
    @group_by_expressions = []
    @column_constraints = []
    @default_data_type = :TEXT
  end

  def set_primary_key(column, data_type)
    self.primary_key = column.to_sym
    self.primary_key_type = data_type.to_sym
  end

  def load_table_commands(source)
    [ create_table_sql,
      insert_from_sql(source.name) ]
  end

  def create_table_sql
    %Q{
      DROP TABLE IF EXISTS #{name};
      CREATE TABLE #{name} ( #{schema.join(", ")} );}
  end

  def insert_from_sql(table_name)
    sql = %Q{
            INSERT INTO #{name} ("#{mapped_column_names.join('", "')}")
            SELECT #{column_converters.join(", ")}
            FROM #{table_name}\n}
    unless column_constraints.empty?
      sql += "WHERE #{column_constraint_conditions}\n"
    end
    unless group_by_expressions.empty?
      sql += "GROUP BY #{group_by_expressions.join(', ')}"
    end
    return sql + ';'
  end

  def copy_column(column_name, options)
    map_column_from_table(options[:from].name, column_name, column_name, options[:key])
  end

  def map_column_from_table(source_table_name, source_col, target_col, matching_col, as_expression=nil)
    if as_expression
      value = as_expression.gsub('$S', "s.#{source_col}").gsub('$T', "t.#{target_col}")
    else
      value = "s.#{source_col}"
    end

    %Q{
      UPDATE #{self.name} AS t
      SET "#{target_col}" = #{value}
      FROM #{source_table_name} AS s
      WHERE t."#{matching_col}" = s."#{matching_col}";}
  end

      def prepare_for_geocoding_commands
        [add_necessary_geocoding_columns, mark_records_as_needing_geocoding, populate_location_geometry].compact
      end

  def mark_records_as_needing_geocoding
    add_column :needs_geocoding, type: :BOOLEAN
    %Q{
      ALTER TABLE #{name} ADD COLUMN needs_geocoding BOOLEAN;
      UPDATE #{name}
        SET needs_geocoding = (COALESCE(residential_address1, '') != ''
                                 AND (residential_lat IS NULL
                                        OR residential_lat = 0.0) );}
  end

  def populate_location_geometry
    add_column :location_geometry, type: "geometry(Geometry,4326)"
    %Q{
      ALTER TABLE #{name} ADD COLUMN location_geometry geometry(Geometry,4326);
      UPDATE #{name}
        SET location_geometry = ST_GeomFromText('POINT(' || residential_lng || ' ' || residential_lat || ')',
                                               #{GEOGRAPHIC_SPACIAL_REFERENCE_SYSTEM_ID})
        WHERE residential_lat IS NOT NULL AND residential_lng IS NOT NULL;}
  end

  def add_necessary_geocoding_columns
    sql = ''
    unless table_columns.map{|map| map[:name] }.include?(:residential_lat)
      sql << "ALTER TABLE #{name} ADD COLUMN residential_lat DECIMAL;"
    end

    unless table_columns.map{|map| map[:name] }.include?(:residential_lng)
      sql << "\nALTER TABLE #{name} ADD COLUMN residential_lng DECIMAL;"
    end

    sql.empty? ? nil : sql
  end

  def schema
    s = []
    if target_table
      s << "LIKE #{target_table.name}"
    else
      table_columns.each do |col|
        s << "\"#{col[:name]}\" #{col[:type]}"
      end
      return s
    end
  end

  def mapped_column_names
    table_columns.select{|c| c[:mapped]}.map{|c| c[:name].to_sym }
  end

  def table_column_names
    table_columns.map{|c| c[:name].to_sym }
  end

  def add_column(col_name, opts={})
    # parse options
    opts[:name] = col_name.to_sym
    opts[:type] ||= default_data_type

    # record column
    table_columns << opts
  end

  def map_column(col_name, opts)
    # parse options
    opts[:mapped] = true
    opts[:type] ||= default_data_type
    unless opts[:as]
      opts[:as] = "$::#{opts[:type]}"
    end

    # record table column
    add_column(col_name, opts)

    # record column converter
    if opts[:from]
      column_converters << opts[:as].gsub("$", "\"#{opts[:from]}\"")
    else
      column_converters << opts[:as]
    end
  end

  def copy_schema_from(database_table)
    self.target_table = database_table
  end

  def target_name
    target_table.name
  end

  def group_records_by(expression)
    self.group_by_expressions << expression
  end

  def drop
    "DROP TABLE IF EXISTS #{self.name};"
  end

  def constrain_column(col_name, constraint)
    column_constraints << [col_name.to_sym, constraint]
  end

  def column_constraint_conditions
    "( " + column_constraints.map{|c| c[1].gsub('$', "\"#{c[0]}\"") }.join(" AND ") + " )"
  end
end