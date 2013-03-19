class VoterFile::CSVDriver::CSVFile
  require 'csv'

  attr_accessor :original, :processed, :delimiter, :quote, :working_table, :working_files, :remote_host

  DEFAULT_DELIMITER = ','
  DEFAULT_QUOTE = '^'

  def initialize(original, working_table)
    @original = File.expand_path(original)
    @delimiter = DEFAULT_DELIMITER
    @quote = DEFAULT_QUOTE
    @working_table = working_table
    @working_files = []
  end

  def path
    processed || original
  end

  def headers
    if remote_host
      head = `ssh #{remote_host} 'head -n 1 #{path}'`
      head.gsub(/[\r\n]/, '').split(delimiter)
    else
      CSV.open(path, :col_sep => delimiter).shift
    end
  end

  def remove_expression(expr)
    grep_expression = 's/' + expr + '//g'
    stripped_file = "#{path}.stripped"
    command = "sed -E '#{grep_expression}' '#{path}' >'#{stripped_file}'; chmod 777 #{stripped_file}"
    command = "ssh #{remote_host} \"#{command}\"" if remote_host

    system(command)

    working_files << stripped_file
    self.processed = stripped_file
  end

  def remove_malformed_rows
    f = self.path
    delimiter_map = {'|' => '\|'}
    d = delimiter_map[delimiter] || delimiter
    corrected_file = "#{f}.corrected"
    c = (headers.count - 1).to_s
    regexp = '^[^' + d + ']*(' + d + '[^' + d + ']*){' + c + '}$'

    command = "egrep '#{regexp}' '#{f}' >'#{corrected_file}'; chmod 777 #{corrected_file}"
    command = "ssh #{remote_host} \"#{command}\"" if remote_host

    system(command)

    working_files << corrected_file
    self.processed = corrected_file
  end

  def load_file_commands
    [create_temp_table_sql,
     bulk_copy_into_working_table_sql]
  end

  # create temporary table for raw data using fields from csv  (all text types)
  def create_temp_table_sql
    raw_csv_schema = headers.map { |h| "#{h} TEXT" }.join(', ')
    %Q{
      DROP TABLE IF EXISTS #{working_table.name};
      CREATE TABLE #{working_table.name} (#{raw_csv_schema});}
  end

  # bulk copy csv into temporary table
  def bulk_copy_into_working_table_sql
    %Q{
      COPY #{working_table.name} FROM '#{path}'
        (FORMAT CSV,
          DELIMITER '#{delimiter}',
          HEADER true,
          ENCODING 'LATIN1',
          QUOTE '#{quote}');}
  end

  def name
    working_table.name
  end

  def close
    working_files.each do |file|
      command = "rm #{file}"
      command = "ssh #{remote_host} \"#{command}\"" if remote_host
      system(command)
    end
  end
end
