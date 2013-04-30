class VoterFile::CSVDriver::CSVFile

  attr_accessor :original, :delimiter, :quote, :working_table, :working_files, :custom_headers

  DEFAULT_DELIMITER = ','
  DEFAULT_QUOTE = "\x00"

  def initialize(original, working_table, custom_headers = [])
    @original = File.expand_path(original)
    @processed = nil
    @delimiter = DEFAULT_DELIMITER
    @quote = DEFAULT_QUOTE
    @working_table = working_table
    @working_files = []
    @custom_headers = custom_headers
    @field_converters = {}
  end

  def path
    @processed || original
  end

  def remove_expression(expr)
    grep_expression = 's/' + expr + '//g'
    stripped_file = "#{path}.stripped"

    system("sed -E '#{grep_expression}' '#{path}' > '#{stripped_file}'; chmod 777 #{stripped_file}")

    working_files << stripped_file
    @processed = stripped_file
  end

  def remove_malformed_rows
    corrected_file = "#{path}.corrected"
    CSV.open(corrected_file, 'wb', col_sep: delimiter, quote_char: quote) do |corrected_csv|
      csv = CSV.open(path, col_sep: delimiter, quote_char: quote, :headers => @custom_headers.empty? ? :first_row : @custom_headers, return_headers: true)
      begin
        row = csv.shift
        until row.nil?
          corrected_csv << row unless row.headers.include?(nil)
          row = csv.shift
        end
      rescue CSV::MalformedCSVError
        # ignore malformed rows
      end
      csv.close
    end
    working_files << corrected_file
    @processed = corrected_file
  end

  def load_file_commands
    [create_temp_table_sql]
  end

  def field(name, options = {})
    @field_converters[name.to_sym] = options[:as] ||  lambda { |value| value }
  end

  def import_rows
    csv = CSV.open(path, col_sep: delimiter, quote_char: quote, :headers => @custom_headers.empty? ? :first_row : @custom_headers, return_headers: false)
    row = csv.shift
    until row.nil?
      values = []
      csv.headers.each do |h|
        value = row[h]
        if @field_converters.has_key?(h.to_sym)
          values << ((@field_converters[h.to_sym].is_a? Proc) ? @field_converters[h.to_sym][value] : @field_converters[h.to_sym])
        else
          values << value
        end
      end

      yield "INSERT INTO #{name} VALUES ('#{values.map{ |v| v.gsub("'", "''") unless v.nil? }.join("', '")}')"

      row = csv.shift
    end
  ensure
    csv.close unless csv.nil?
  end

  def name
    working_table.name
  end

  def close
    working_files.each do |file|
      system("rm #{file}")
    end
  end

  private

    def create_temp_table_sql
      raw_csv_schema = headers.map { |h| %Q{"#{h}" TEXT} }.join(', ')
      %Q{
        DROP TABLE IF EXISTS #{working_table.name};
        CREATE TEMPORARY TABLE #{working_table.name} (#{raw_csv_schema});
      }
    end

    def headers
      csv = CSV.open(path, col_sep: delimiter, quote_char: quote, :headers => @custom_headers.empty? ? :first_row : @custom_headers, return_headers: true)
      csv.shift
      result = csv.headers
      result
    ensure
      csv.close unless csv.nil?
    end
end
