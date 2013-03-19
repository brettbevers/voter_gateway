require 'ostruct'

module VoterFile
  class DatabaseAudit < CSVDriver
    attr_accessor :state

    def initialize(state)
      super()
      @state = state
    end

    def perform
      @result = AuditResult.new

      table_name = "#{state.downcase}_voters"

      @voters_table = load_table(table_name)

      sql = @voters_table.total_records_sql
      @result.total_records = get_count(sql)

      %w{state_file_id county_file_id phone email born_at age gender demo residential_address1 residential_lat residential_lng registered_at party is_perm_absentee}.
      each do |column|
        sql = @voters_table.coverage_for_column_sql(column)
        count = get_count(sql)
        percentage = count/@result.total_records.to_f
        @result.coverage[column] = [count, percentage]
      end

      sql = @voters_table.get_distribution_for_column_sql('registered_at', limit: 20)
      @result.registration_stats['top_20'] = db_connection.execute(sql).values

      [[:is_perm_absentee, :perm_absentee_stats],
       [:party, :party_stats],
       [:demo, :ethnicity_stats],
       [:gender, :gender_stats],
       [:reg_status, :registration_stats],
       [:born_at, :dob_stats],
       [:age, :age_stats]].
      each do |args|
        record_distribution_for_column_in_hash(*args)
      end

      sql = @voters_table.count_needs_geocoding_sql
      db_connection.execute(sql).values.each do |row|
        @result.needs_geocoding_stats[row[0]] = row[1]
      end

      return @result
    end

    def record_distribution_for_column_in_hash(column, hash_name)
      sql = @voters_table.get_distribution_for_column_sql(column)
      db_connection.execute(sql).values.each do |item|
        hash = @result.send(hash_name.to_sym)
        hash[item[0]] = item[1].to_i
      end
    end
    private :record_distribution_for_column_in_hash

    def load_table_from_db(source)
      raise "Relation '#{source}' does not exist" unless db_connection.table_exists? source
      table = AuditDatabaseTable.new(source)
      yield table if block_given?
      return table
    end

    class AuditDatabaseTable < CSVDriver::DatabaseTable
      def coverage_for_column_sql(column)
        "SELECT count(*) FROM #{name} WHERE #{column} IS NOT NULL AND trim(#{column}::TEXT) <> '';"
      end

      def total_records_sql
        "SELECT count(*) FROM #{name};"
      end

      def count_needs_geocoding_sql
        %Q{SELECT (COALESCE(trim(residential_address1), '') != ''
                  AND (residential_lat IS NULL
                    OR residential_lat = 0.0
                    OR residential_zip4 IS NULL) ) AS needs_geocoding,
                  count(*)
          FROM #{name}
          GROUP BY needs_geocoding;}
      end

      def get_distribution_for_column_sql(column, opts={})
        sql = %Q{
          SELECT #{column}, count(*) AS count
            FROM #{name}
            GROUP BY #{column}
            ORDER BY count DESC
        }
        if (limit = opts[:limit])
          sql << "\nLIMIT #{limit}"
        end
        sql + ';'
      end
    end

    class AuditResult
      attr_accessor :total_records, :coverage, :age_stats, :gender_stats, :ethnicity_stats,
                    :registration_stats, :party_stats, :perm_absentee_stats, :needs_geocoding_stats, :dob_stats

      def initialize
        @coverage = {}
        @age_stats = {}
        @gender_stats = {}
        @ethnicity_stats = {}
        @registration_stats = {}
        @party_stats = {}
        @perm_absentee_stats = {}
        @needs_geocoding_stats = {}
        @dob_stats = {}
      end
    end
  end
end