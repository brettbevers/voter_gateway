require File.expand_path(File.dirname(__FILE__) + '/spec_helper')

describe VoterFile::CSVDriver::CSVFile do

  let(:test_file_path) { Tempfile.new('test').path }
  let(:working_table) { stub(name: 'working_table') }
  let(:subject) { VoterFile::CSVDriver::CSVFile.new(test_file_path, working_table) }

  after(:all) do
    File.delete(test_file_path)
  end

  after(:each) do
    subject.close
  end

  describe '#initialize' do
    its(:original) { should == test_file_path }
    its(:delimiter) { should == ',' }
    its(:quote) { should == '^' }
    its(:working_table) { should == working_table }
    its(:working_files) { should be_empty }
    its(:custom_headers) { should be_empty }
  end

  describe '#path' do
    its(:path) { should == test_file_path }

    it 'returns path of file, preferring processed to original' do
      subject.instance_variable_set(:@processed, '/other/path')
      subject.path.should == '/other/path'
    end
  end

  describe '#remove_expression' do
    it 'returns path to processed file' do
      File.open(test_file_path, 'w') { |f| f << 'header 1,header 2,header 3' }
      subject.remove_expression '\^'
      subject.path.should == "#{test_file_path}.stripped"
    end

    it 'saves the stripped file in the working files' do
      File.open(test_file_path, 'w') { |f| f << "^header 1^,^header 2^,^header 3^\ndata 1,data 2,data 3\n" }
      subject.remove_expression '\^'
      subject.working_files.should include("#{test_file_path}.stripped")
    end

    it 'creates a corrected file in which the character is removed' do
      File.open(test_file_path, 'w') { |f| f << "^header 1^,^header 2^,^header 3^\ndata 1,data 2,data 3\n" }
      subject.remove_expression '\^'
      File.open(subject.path, 'r').read.should == "header 1,header 2,header 3\ndata 1,data 2,data 3\n"
    end
  end

  describe '#remove_malformed_rows' do
    it 'returns path to processed file' do
      File.open(test_file_path, 'w') { |f| f << 'header 1,header 2,header 3' }
      subject.remove_malformed_rows
      subject.path.should == "#{test_file_path}.corrected"
    end

    it 'saves the corrected file in the working files' do
      File.open(test_file_path, 'w') { |f| f << 'header 1,header 2,header 3' }
      subject.remove_malformed_rows
      subject.working_files.should include("#{test_file_path}.corrected")
    end

    it 'creates a corrected file from which rows with extra fields are removed' do
      File.open(test_file_path, 'w') { |f| f << "header 1,header 2,header 3\ndata 1,data 2,data 3\nd1,d2,d3,d4\n" }
      subject.remove_malformed_rows
      File.open(subject.path, 'r').read.should == "header 1,header 2,header 3\ndata 1,data 2,data 3\n"
    end

    it 'creates a corrected file from which malformed rows are removed' do
      File.open(test_file_path, 'w') { |f| f << "header 1,header 2,header 3\ndata 1,data 2,data 3\nd^1,d^2,d3\ndata 4,data 5,data 6\n" }
      subject.remove_malformed_rows
      File.open(subject.path, 'r').read.should == "header 1,header 2,header 3\ndata 1,data 2,data 3\ndata 4,data 5,data 6\n"
    end

    it 'ignores the delimiter inside quoted fields' do
      File.open(test_file_path, 'w') { |f| f << "'header 1','header 2','header 3'\n'data, 1','data, 2','data, 3'" }
      subject.delimiter = ","
      subject.quote = "'"
      subject.remove_malformed_rows
      File.open(subject.path, 'r').read.should == "header 1,header 2,header 3\n'data, 1','data, 2','data, 3'\n"
    end

    it 'matches on mixed quoted and unquoted fields' do
      File.open(test_file_path, 'w') { |f| f << "'header 1',header 2,'header 3'\ndata 1,'data, 2',data 3" }
      subject.delimiter = ","
      subject.quote = "'"
      subject.remove_malformed_rows
      File.open(subject.path, 'r').read.should == "header 1,header 2,header 3\ndata 1,'data, 2',data 3\n"
    end
  end

  describe '#load_file_commands' do
    it 'returns the sql to create a temporary table' do
      File.open(test_file_path, 'w') { |f| f << "header 1,header 2,header 3\n" }
      expected_sql = 'DROP TABLE IF EXISTS working_table; CREATE TEMPORARY TABLE working_table ("header 1" TEXT, "header 2" TEXT, "header 3" TEXT);'
      actual_sql = subject.load_file_commands[0].gsub(/\s+/, ' ').strip

      actual_sql.should == expected_sql
    end

    it 'returns the sql to create a temporary table with specified column types' do
      File.open(test_file_path, 'w') { |f| f << "header 1,header 2,header 3\n" }
      expected_sql = 'DROP TABLE IF EXISTS working_table; CREATE TEMPORARY TABLE working_table ("header 1" INT, "header 2" TEXT, "header 3" TEXT);'
      subject.field 'header 1', :type => :INT
      subject.field 'header 2'

      actual_sql = subject.load_file_commands[0].gsub(/\s+/, ' ').strip

      actual_sql.should == expected_sql
    end
  end

  describe '#import_rows' do
    it 'uses the postgresql bulk csv import by default' do
      expected_sql = [
          %Q{COPY working_table FROM '#{test_file_path}' (FORMAT CSV, DELIMITER ',', HEADER true, ENCODING 'LATIN1', QUOTE '^');}.gsub(/\s+/, ' ').strip
      ]
      actual_sql = []

      subject.import_rows { |sql| actual_sql << sql.gsub(/\s+/, ' ').strip }

      actual_sql.should == expected_sql
    end

    it 'does not take the header from the csv when bulk importing from postgresql and custom headers are defined' do
      expected_sql = [
          %Q{COPY working_table FROM '#{test_file_path}' (FORMAT CSV, DELIMITER ',', HEADER false, ENCODING 'LATIN1', QUOTE '^');}.gsub(/\s+/, ' ').strip
      ]
      actual_sql = []

      subject.custom_headers = %w{header1 header2 header3}
      subject.import_rows { |sql| actual_sql << sql.gsub(/\s+/, ' ').strip }

      actual_sql.should == expected_sql
    end

    it 'returns the sql to insert each field in each row if no converters defined' do
      File.open(test_file_path, 'w') do |f|
        f << "header 1,header 2,header 3\n"
        f << "row 1 value 1,row 1 value 2,row 1 value 3\n"
        f << "row 2 value 2,row 2 value 2,row 2 value 3\n"
      end

      expected_sql = [
          "INSERT INTO working_table VALUES ('row 1 value 1', 'row 1 value 2', 'row 1 value 3')",
          "INSERT INTO working_table VALUES ('row 2 value 2', 'row 2 value 2', 'row 2 value 3')"
      ]
      actual_sql = []

      subject.import_rows(:import_method => :by_row) { |sql| actual_sql << sql }

      actual_sql.should == expected_sql
    end

    it 'escapes the quotes in the returned sql' do
      File.open(test_file_path, 'w') do |f|
        f << "header 1,header 2,header 3\n"
        f << "value with 'quotes',value 2,value 3\n"
      end

      expected_sql = ["INSERT INTO working_table VALUES ('value with ''quotes''', 'value 2', 'value 3')"]
      actual_sql = []

      subject.import_rows(:import_method => :by_row) { |sql| actual_sql << sql }

      actual_sql.should == expected_sql
    end

    it 'returns the sql to insert fields using a conversion block' do
      File.open(test_file_path, 'w') do |f|
        f << "header 1,header 2,header 3\n"
        f << "value 1,value 2,value 3\n"
      end

      expected_sql = ["INSERT INTO working_table VALUES ('1 eulav', 'value 2', 'value 3')"]
      actual_sql = []

      subject.field 'header 1', as: lambda { |v| v.reverse }
      subject.import_rows(:import_method => :by_row) { |sql| actual_sql << sql }

      actual_sql.should == expected_sql
    end

    it 'returns the sql to insert fields using a conversion block and another single field from the csv defined by its name' do
      File.open(test_file_path, 'w') do |f|
        f << "header 1,header 2,header 3\n"
        f << "value 1,value 2,value 3\n"
      end

      expected_sql = ["INSERT INTO working_table VALUES ('value 1 and value 2', 'value 2', 'value 3')"]
      actual_sql = []

      subject.field 'header 1', as: lambda { |v, other_field| "#{v} and #{other_field}" }, using_field_values: 'header 2'
      subject.import_rows(:import_method => :by_row) { |sql| actual_sql << sql }

      actual_sql.should == expected_sql
    end

    it 'returns the sql to insert fields using a conversion block and multiple fields from the csv defined by their names' do
      File.open(test_file_path, 'w') do |f|
        f << "header 1,header 2,header 3\n"
        f << "value 1,value 2,value 3\n"
      end

      expected_sql = ["INSERT INTO working_table VALUES ('value 1 and value 2 and value 3', 'value 2', 'value 3')"]
      actual_sql = []

      subject.field 'header 1', as: lambda { |v, other_fields| "#{v} and #{other_fields.join(' and ')}" }, using_field_values: ['header 2', 'header 3']
      subject.import_rows(:import_method => :by_row) { |sql| actual_sql << sql }

      actual_sql.should == expected_sql
    end

    it 'returns the sql to insert fields using a conversion block and another single field from the csv defined by its index' do
      File.open(test_file_path, 'w') do |f|
        f << "header 1,header 2,header 3\n"
        f << "value 1,value 2,value 3\n"
      end

      expected_sql = ["INSERT INTO working_table VALUES ('value 1 and value 2', 'value 2', 'value 3')"]
      actual_sql = []

      subject.field 'header 1', as: lambda { |v, other_field| "#{v} and #{other_field}" }, using_field_values: 1
      subject.import_rows(:import_method => :by_row) { |sql| actual_sql << sql }

      actual_sql.should == expected_sql
    end

    it 'ignores the other field values if no conversion block is defined' do
      File.open(test_file_path, 'w') do |f|
        f << "header 1,header 2,header 3\n"
        f << "value 1,value 2,value 3\n"
      end

      expected_sql = ["INSERT INTO working_table VALUES ('value 1', 'value 2', 'value 3')"]
      actual_sql = []

      subject.field 'header 1', using_field_values: ['header 2', 'header 3']
      subject.import_rows(:import_method => :by_row) { |sql| actual_sql << sql }

      actual_sql.should == expected_sql
    end

    it 'returns the sql to insert extra fields' do
      File.open(test_file_path, 'w') do |f|
        f << "value 1,value 2,value 3\n"
      end

      expected_sql = ["INSERT INTO working_table VALUES ('value 1', 'value 2', 'value 3', 'value for the extra column')"]
      actual_sql = []

      subject.custom_headers = %w{header1 header2 header3 header4}
      subject.field 'header4', as: lambda { |v| 'value for the extra column' }
      subject.import_rows(:import_method => :by_row) { |sql| actual_sql << sql }

      actual_sql.should == expected_sql
    end

    it 'returns the insert sql with type conversion' do
      File.open(test_file_path, 'w') do |f|
        f << "header 1,header 2,header 3\n"
        f << "value 1,,value 3\n"
      end

      expected_sql = ["INSERT INTO working_table VALUES ('value 1'::CITEXT, NULL::INT, 'value 3')"]
      actual_sql = []

      subject.field 'header 1', :type => :CITEXT
      subject.field 'header 2', :type => :INT
      subject.import_rows(:import_method => :by_row) { |sql| actual_sql << sql }

      actual_sql.should == expected_sql
    end

    it 'returns the sql to bulk insert rows' do
      File.open(test_file_path, 'w') do |f|
        f << "header 1,header 2,header 3\n"
        f << "row 1 value 1,row 1 value 2,row 1 value 3\n"
        f << "row 2 value 2,row 2 value 2,row 2 value 3\n"
      end

      expected_sql = ["INSERT INTO working_table VALUES ('row 1 value 1', 'row 1 value 2', 'row 1 value 3'), ('row 2 value 2', 'row 2 value 2', 'row 2 value 3')"]
      actual_sql = []

      subject.import_rows(:import_method => :by_row, bulk_insert_size: 2) { |sql| actual_sql << sql }

      actual_sql.should == expected_sql
    end
  end
end
