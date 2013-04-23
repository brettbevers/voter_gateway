class VoterFile::CSVDriver::CSVFile
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
      # TODO (sandrasi): in case of quoted values it may return invalid headers
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
    d = egrep_escape(delimiter)
    q = egrep_escape(quote)
    corrected_file = "#{f}.corrected"
    c = (headers.count - 1).to_s

    regexp = "^((#{q}[^#{q}]*#{q})|([^#{d}#{q}]*))(#{d}((#{q}[^#{q}]*#{q})|([^#{d}#{q}]*))){#{c}}$"

    command = "egrep '#{regexp}' '#{f}' >'#{corrected_file}'; chmod 777 #{corrected_file}"
    command = "ssh #{remote_host} \"#{command}\"" if remote_host
    system(command)

    working_files << corrected_file
    self.processed = corrected_file
  end

  def egrep_escape(char)
    posix_ext_regex_meta_chars = %w{. ^ $ | \\ ? * + [ ( ) { } }
    if posix_ext_regex_meta_chars.include? char
      '\\' + char
    elsif char == "'"
      "'\\''"
    else
      char
    end
  end
  private :egrep_escape

  def load_file_commands(custom_headers = [])
    [create_temp_table_sql(custom_headers),
     bulk_copy_into_working_table_sql(custom_headers)]
  end

  # create temporary table for raw data using fields from csv  (all text types)
  def create_temp_table_sql(custom_headers)
    if (custom_headers.empty?)
      raw_csv_schema = headers.map { |h| "\"#{h}\" TEXT" }.join(', ')
    else
      raw_csv_schema = custom_headers.map { |h| "\"#{h}\" TEXT" }.join(', ')
    end
    %Q{
      DROP TABLE IF EXISTS #{working_table.name};
      CREATE TEMPORARY TABLE #{working_table.name} (#{raw_csv_schema});}
  end

  # bulk copy csv into temporary table
  def bulk_copy_into_working_table_sql(custom_headers)
    %Q{
      COPY #{working_table.name} FROM '#{path}'
        (FORMAT CSV,
          DELIMITER '#{delimiter}',
          HEADER #{custom_headers.empty?},
          ENCODING 'LATIN1',
          QUOTE '#{quote == "'" ? "''" : quote}');}
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
