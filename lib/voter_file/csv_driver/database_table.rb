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

  def map_column_from_table(source_table_name, source_col, target_col, matching_col)
    %Q{
      UPDATE #{self.name} AS nv
      SET "#{target_col}" = v."#{source_col}"
      FROM #{source_table_name} AS v
      WHERE nv."#{matching_col}" = v."#{matching_col}";}
  end
end