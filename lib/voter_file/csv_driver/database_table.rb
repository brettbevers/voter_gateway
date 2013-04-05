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
end