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
end