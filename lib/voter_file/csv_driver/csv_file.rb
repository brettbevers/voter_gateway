class CSV
  # Monkey patch Ruby's built in CSV class and fix the regular expression which is used internally to escape special regexp characters
  def escape_re(str)
    @fixed_re_chars = /#{%"[-\\[\\]\\.^$?*+{}()|# \r\n\t\f\v]".encode(@encoding)}/ if @fixed_re_chars.nil?
    str.gsub(@fixed_re_chars) { |c| @re_esc + c }
  end
end

class VoterFile::CSVDriver::CSVFile
  attr_accessor :delimiter, :quote
  attr_reader :original, :working_table, :working_files

  DEFAULT_DELIMITER = ','
  DEFAULT_QUOTE = '^'
  BATCH_SIZE = 100000

  def initialize(original, working_table, connection = nil)
    @connection = connection
    @original = File.expand_path(original)
    @delimiter = DEFAULT_DELIMITER
    @quote = DEFAULT_QUOTE
    @working_table = working_table
    @working_files = []
  end

  def path
    @processed || original
  end

  def remove_expression(expr)
    grep_expression = 's/' + expr + '//g'
    stripped_file = "#{path}.stripped"

    system("sed -E '#{grep_expression}' '#{path}' > '#{stripped_file}'")

    working_files << stripped_file
    @processed = stripped_file
  end

  def remove_malformed_rows
    corrected_file = "#{path}.corrected"
    CSV.open(corrected_file, 'wb', col_sep: delimiter, quote_char: quote) do |corrected_csv|
      csv = CSV.open(path, col_sep: delimiter, quote_char: quote, headers: :first_row, return_headers: true)
      while row = next_row(csv)
        corrected_csv << row unless row.headers.size != csv.headers.size
      end
      csv.close
    end
    working_files << corrected_file
    @processed = corrected_file
  end

  def next_row(csv)
    row = nil
    while row.nil? && !csv.eof?
      begin
        row = csv.shift
      rescue CSV::MalformedCSVError
        # ignore malformed rows
      end
    end
    row
  end

  def load_file_commands(&block)
    if mapped_column_names.empty?
      load_csv_in_bulk &block
    else
      load_csv_by_row &block
    end
  end

  def load_csv_by_row
    yield working_table.create_table_sql
    CSV.open(path, col_sep: delimiter, quote_char: quote, headers: :first_row, skip_blanks: true) do |csv|
      stream_data_from(csv)
    end
  end

  def load_csv_in_bulk
    yield create_temp_table_sql
    yield bulk_copy_into_table_sql(path)
  end

  def stream_data_from(csv)
    VoterFile::PostgresCopy.copy(working_table.name, mapped_column_names, @connection) do |writer|
      until csv.eof?
        writer.write(*convert_row(csv.shift))
      end
    end
  end

  def convert_row(row)
    converted_row = []
    column_converters.each do |converter|
      converted_row << converter.call(row)
    end
    return converted_row
  end

  def column_converters
    @column_converters ||= working_table.column_converters
  end

  def mapped_column_names
    @mapped_column_names ||= working_table.mapped_column_names.map(&:to_s)
  end

  def name
    @name ||= working_table.name
  end

  def map_column(col_name, opts={}, &block)
    opts[:as] = case opts[:as]
                  when Proc
                    opts[:as]
                  when nil
                    from_string = opts[:from].to_s
                    block || ->(row) { row[from_string] }
                  else
                    value = opts[:as]
                    ->(row) { value }
                end
    working_table.send(:map_column, col_name, opts)
  end

  def set_primary_key(*args)
    working_table.send(:set_primary_key, *args)
  end

  def default_data_type=(*args)
    working_table.send(:default_data_type=, *args)
  end

  def constrain_column(*args)
    working_table.send(:constrain_column, *args)
  end

  def close
    working_files.each { |file| File.unlink(file) }
  end

  def bulk_copy_into_table_sql(csv_path)
    return <<-SQL
    COPY #{working_table.name} FROM '#{csv_path}'
      (FORMAT CSV,
        DELIMITER '#{delimiter == "'" ? "''" : delimiter}',
        HEADER TRUE,
        ENCODING 'LATIN1',
        QUOTE $quote_character$#{quote == "'" ? "''" : quote}$quote_character$);
    SQL
  end

  def headers
    return @headers if @headers
    CSV.open(path, col_sep: delimiter, quote_char: quote, headers: :first_row, return_headers: true) do |csv|
      @headers = csv.shift.headers
    end
    @headers
  end

  def raw_csv_schema
    headers.map{ |h| "\"#{h}\" TEXT" }.join(', ')
  end

  def create_temp_table_sql
    return <<-SQL
    DROP TABLE IF EXISTS #{working_table.name};
    CREATE TEMPORARY TABLE #{working_table.name} (#{raw_csv_schema});
    SQL
  end
end
