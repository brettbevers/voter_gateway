module VoterFile
  class CSVAudit < CSVDriver

    attr_accessor :path, :delimiter, :quote, :remote_host, :keys

    def initialize(path, opts={})
      super()
      @path = path
      @delimiter = opts[:delimiter] || VoterFile::CSVDriver::CSVFile::DEFAULT_DELIMITER
      @quote = opts[:quote] || VoterFile::CSVDriver::CSVFile::DEFAULT_QUOTE
      @remote_host = opts[:remote_host]
      @keys = opts[:keys] || []
    end

    def perform
      begin
        return job
      ensure
        clean_up!
      end
    end

    def job
      result = AuditResult.new
      file = load_file(path) do |f|
        f.delimiter = delimiter
        f.quote = quote
        f.remote_host = remote_host
      end

      sql = file.total_count_sql
      result.total_count = get_count(sql)

      keys.each do |key|
        sql = file.duplicate_records_sql(key)
        result.keys[key] = get_count(sql)
      end

      headers = file.headers
      result.headers = headers
      headers.each do |header|
        sql = file.coverage_for_header_sql(header)
        count = get_count(sql)
        percentage = count/result.total_count.to_f
        result.coverage[header] = [count, percentage]
      end

      result.malformed_count = file.malformed_count
      result
    end

    def load_file(path)
      # the CSVFile instance requires one working table
      file = AuditFile.new(path, create_working_table)
      working_files << file

      yield file if block_given?

      file.remove_malformed_rows

      commands = file.load_file_commands
      commands.each do |sql|
        db_connection.execute(sql)
      end

      return file
    end

    class AuditResult
      attr_accessor :total_count, :malformed_count, :coverage, :headers, :keys

      def initialize
        @coverage = {}
        @keys = {}
      end

      def to_s
        report = "CSV Audit Report"
        report << "\n\nValid rows: #{total_count}"
        report << "\nInvalid rows: #{malformed_count}"

        report << "\n\nKeys:"
        keys.each do |key, stat|
          s = "\n\t[#{key}]"
          if stat == 0
            s << " UNIQUE KEY"
          else
            s << " DUPLICATES (NOT UNIQUE)"
          end
          report << s
        end

        report << "\n\nCoverage:"
        coverage.each do |header, stats|
          report << "\n\t#{header}:".ljust(30) + " #{(stats[1]*100).round(2)}% (#{stats[0]})\n"
        end

        return report
      end
    end

    class AuditFile < CSVDriver::CSVFile
      def duplicate_records_sql(key)
        %Q{
          SELECT #{key}, COUNT(*)
            FROM #{working_table.name}
            GROUP BY #{key}
            HAVING COUNT(*) > 1
            LIMIT 1;}
      end

      def coverage_for_header_sql(header)
        %Q{
          SELECT COUNT(*) FROM #{working_table.name}
            WHERE "#{header}" IS NOT NULL AND trim("#{header}") <> '';}
      end

      def total_count_sql
        "SELECT COUNT(*) FROM #{working_table.name};"
      end

      def malformed_count
        command = "wc -l #{original}"
        command = "ssh #{remote_host} \"#{command}\"" if remote_host
        original_file_length = `#{command}`.to_i

        command = "wc -l #{path}"
        command = "ssh #{remote_host} \"#{command}\"" if remote_host
        corrected_file_length = `#{command}`.to_i

        return original_file_length - corrected_file_length
      end
    end
  end
end
