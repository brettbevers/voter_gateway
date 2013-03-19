module VoterFile
  class CSVDriver
    attr_accessor :loaded_extensions, :working_table_infix, :working_tables, :working_files

    WORKING_TABLE_PREFIX = "tmp_working_table"

    SUPPORTED_EXTENSIONS = {
      :nb_parse_election_name =>
%Q{
  DROP FUNCTION IF EXISTS nb_parse_election_name(elec_date text, elec_type text);
  CREATE FUNCTION nb_parse_election_name(elec_date text, elec_type text) RETURNS TEXT AS $$
  DECLARE
    year TEXT := EXTRACT(YEAR FROM elec_date::DATE)::TEXT;
    type TEXT := lower(elec_type);
  BEGIN
    IF type = 'pr' THEN
      RETURN year || '_primary';
    ELSIF type = 'ge' THEN
      RETURN year || '_general';
    ELSIF type = 'pp' THEN
      RETURN year || '_presidential_primary';
    ELSIF type = 'sp' THEN
      RETURN year || '_special';
    ELSE
      RETURN year || '_' || type;
    END IF;
  END
  $$ LANGUAGE plpgsql;
},
    :nb_coerce_to_date_or_null =>
%Q{
  DROP FUNCTION IF EXISTS nb_coerce_to_date_or_null(date text);
  CREATE FUNCTION nb_coerce_to_date_or_null(date text) RETURNS DATE AS $$
  BEGIN
      BEGIN
          RETURN date::DATE;
      EXCEPTION WHEN OTHERS THEN
          RETURN NULL;
      END;
  END
  $$ LANGUAGE plpgsql;
}}

    def initialize
      @db_connection = ActiveRecord::Base.connection unless Rails.env == 'test'
      @merge_records_adapter = RecordMerger
      @working_tables = []
      @working_files = []
      @loaded_extensions = []

      # random string to ensure that working tables have unique name across driver instances
      @working_table_infix = (0...4).map{ ('a'..'z').to_a[rand(26)] }.join
    end

    def db_connection
      init_extensions
      @db_connection
    end
    private :db_connection

    def init_extensions
      loaded_extensions.each do |extension|
        @db_connection.execute SUPPORTED_EXTENSIONS[extension]
      end
    end
    private :init_extensions

    def create_working_table
      w = WorkingTable.new("#{WORKING_TABLE_PREFIX}_#{working_table_infix}_#{@working_tables.size}")
      working_tables << w

      return w
    end
    private :create_working_table

    def clean_up!
      working_tables.each do |table|
        db_connection.execute table.drop
      end

      working_files.each do |file|
        file.close
      end
    end

    def load_file(path)
      # the CSVFile instance requires one working table
      file = CSVFile.new(path, create_working_table)
      working_files << file

      yield file if block_given?

      commands = file.load_file_commands
      commands.each do |sql|
        db_connection.execute(sql)
      end

      return file
    end

    def load_table(source, &block)
      case source
      when String
        load_table_from_db(source, &block)
      else
        load_table_from_source(source, &block)
      end
    end

    def load_table_from_source(source)
      table = create_working_table
      yield table if block_given?

      commands = table.load_table_commands(source)
      commands.each do |sql|
        db_connection.execute(sql)
      end

      return table
    end
    private :load_table_from_source

    def load_table_from_db(source)
      model = Class.new(ActiveRecord::Base) do
        self.table_name = source
      end

      raise "Relation '#{source}' does not exist" unless db_connection.table_exists? source
      table = DatabaseTable.new(source)
      table.table_column_names = model.column_names.map(&:to_sym)

      yield table if block_given?
      return table
    end

    def prepare_table_for_geocoding(table)
      commands = table.prepare_for_geocoding_commands
      commands.each do |sql|
        db_connection.execute(sql)
      end
    end

    def merge_records
      # the record merger instance requires two working tables
      merger = @merge_records_adapter.new(create_working_table, create_working_table)
      yield merger if block_given?

      commands = merger.merge_commands
      commands.each do |sql|
        db_connection.execute(sql)
      end

      return merger
    end

    def load_extension(name)
      unless SUPPORTED_EXTENSIONS.include? name.to_sym
        raise NameError, "#{name} is not a supported extension."
      end
      self.loaded_extensions << name.to_sym
    end

    def copy_column(col_name, opts)
      target_table = opts.delete(:to)
      sql = target_table.copy_column(col_name, opts)
      db_connection.execute(sql)
    end

    def get_count(sql)
      result = db_connection.execute(sql)
      result.field_values("count").first.to_i
    end
  end
end
