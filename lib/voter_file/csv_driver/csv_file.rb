class VoterFile::CSVDriver::CSVFile

  attr_accessor :original, :delimiter, :quote, :working_table, :working_files, :custom_headers

  DEFAULT_DELIMITER = ','
  DEFAULT_QUOTE = '`'

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
      row = next_row(csv)
      until row.nil?
        corrected_csv << row unless row.headers.size != csv.headers.size
        row = next_row(csv)
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
  private :next_row

  def load_file_commands
    [create_temp_table_sql]
  end

  def field(name, options = {})
    converter = {}
    converter[:type] = options[:type] if options[:type]
    converter[:using_field_values] = options[:using_field_values] if options[:using_field_values]
    converter[:as] = options[:as] || (converter[:using_field_values] ? lambda { |value, other_field_values| value } : lambda { |value| value })
    @field_converters[name.to_sym] = converter
  end

  def import_rows(options = {:import_method => :bulk})
    if options.has_key?(:import_method) && options[:import_method].to_sym == :by_row
      begin
        bulk_size = options[:bulk_insert_size] || 1
        bulk_values = []
        csv = CSV.open(path, col_sep: delimiter, quote_char: quote, :headers => @custom_headers.empty? ? :first_row : @custom_headers, return_headers: false)
        row = csv.shift
        until row.nil?
          values = []
          headers = csv.headers
          headers.each_index do |idx|
            value = row[idx]
            converter = @field_converters[headers[idx].to_sym]
            if converter
              type = converter[:type]
              if converter[:using_field_values]
                if converter[:using_field_values].respond_to?(:each)
                  other_field_values = []
                  converter[:using_field_values].each do |v|
                    other_field_values << row[v]
                  end
                  value = converter[:as][value, other_field_values]
                else
                  value = converter[:as][value, row[converter[:using_field_values]]]
                end
              else
                value = converter[:as][value]
              end
            end
            values << {value: value, type: type}
          end

          bulk_values << values

          if (bulk_values.size == bulk_size) || csv.eof?
            sql_insert_values = bulk_values.map do |bv|
              mapped_values = bv.map do |value_with_type|
                if value_with_type[:value]
                  mapped_value = "'#{value_with_type[:value].gsub("'", "''")}'"
                else
                  mapped_value = 'NULL'
                end
                mapped_value = "#{mapped_value}::#{value_with_type[:type]}" if value_with_type[:type]
                mapped_value
              end
              "(#{mapped_values.join(', ')})"
            end
            yield "INSERT INTO #{name} VALUES #{sql_insert_values.join(', ')}"
            bulk_values = []
          end

          row = csv.shift
        end
      ensure
        csv.close unless csv.nil?
      end
    else
      yield bulk_copy_into_table_sql
    end
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
      raw_csv_schema = headers.map { |h| %Q{"#{h}" #{(@field_converters[h.to_sym][:type] if @field_converters[h.to_sym]) || 'TEXT'}} }.join(', ')
      %Q{
        DROP TABLE IF EXISTS #{working_table.name};
        CREATE TEMPORARY TABLE #{working_table.name} (#{raw_csv_schema});
      }
    end

    def bulk_copy_into_table_sql
      %Q{
        COPY #{working_table.name} FROM '#{path}'
          (FORMAT CSV,
            DELIMITER '#{delimiter == "'" ? "''" : delimiter}',
            HEADER #{@custom_headers.empty?},
            ENCODING 'LATIN1',
            QUOTE '#{quote == "'" ? "''" : quote}');
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
