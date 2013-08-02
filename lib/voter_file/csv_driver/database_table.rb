class VoterFile::CSVDriver::DatabaseTable

  attr_accessor :name, :table_column_names
  attr_reader :primary_key, :primary_key_type

  def initialize(name)
    @name = name
    @table_column_names = []
  end

  def set_primary_key(column, data_type)
    @primary_key = column.to_sym
    @primary_key_type = data_type.to_sym
  end

  def map_column_from_table(options)
    if options[:as_expression]
      value = options[:as_expression].gsub('$S', "s.#{options[:source_col]}").gsub('$T', "t.#{options[:target_col]}")
    else
      value = "s.#{options[:source_col]}"
    end

    where_clause = options[:filter] ? ' AND ' + options[:filter].map { |f| %Q{s."#{f[:column]}" #{f[:expression]}} }.join(' AND ') : ''

    %Q{
      UPDATE #{self.name} AS t
      SET "#{options[:target_col]}" = #{value}
      FROM #{options[:source_table_name]} AS s
      WHERE t."#{options[:matching_col]}" = s."#{options[:matching_col]}" #{where_clause};}
  end
end
