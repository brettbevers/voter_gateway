require File.expand_path(File.dirname(__FILE__) + '/spec_helper')

describe VoterFile::CSVDriver do

  let(:subject) { VoterFile::CSVDriver.new }
  let!(:test_file_path) { Tempfile.new("test").path }
  let(:test_table_name) { "test_table" }

  def subject_should_execute_sql(sql)
    db_connection = stub
    subject.should_receive(:db_connection).ordered.and_return(db_connection)
    db_connection.should_receive(:execute).with(sql)
  end

  describe "#clean_up!" do
    it "should drop all working tables" do
      table1, table2, drop_table1, drop_table2 = stub, stub, stub, stub
      subject.instance_variable_set(:@working_tables, [table1, table2])
      table1.should_receive(:drop).and_return(drop_table1)
      table2.should_receive(:drop).and_return(drop_table2)
      subject_should_execute_sql(drop_table1)
      subject_should_execute_sql(drop_table2)
      subject.send(:clean_up!)
    end
  end

  describe "#new_working_table" do

    its(:create_working_table) { should be_a VoterFile::CSVDriver::WorkingTable }

    it "should add a new working table to the working tables list" do
      working_table = subject.send(:create_working_table)
      working_tables = subject.instance_variable_get(:@working_tables)
      working_tables.count.should == 1
      working_tables.should include working_table
    end

  end

  describe "#load_file" do

    it "yields the file located at the input path" do
      csv_file, sql = stub, stub
      VoterFile::CSVDriver::CSVFile.stub(:new => csv_file)
      ActiveRecord::Base.stub(:transaction).and_yield()

      subject_should_execute_sql(sql)
      csv_file.should_receive(:load_file_commands).and_return([sql])
      csv_file.should_receive(:import_rows)

      file = subject.load_file(test_file_path) do |file|
        file.should == csv_file
      end
      file.should == csv_file
    end

    it "uses the custom headers when loading the file" do
      csv_file, working_table, create_table_sql, insert_row_sql = stub, stub, stub, stub

      subject.stub(create_working_table: working_table)
      VoterFile::CSVDriver::CSVFile.should_receive(:new).with(test_file_path, working_table, %w{header1 header2 header3}).and_return(csv_file)
      ActiveRecord::Base.stub(:transaction).and_yield()
      csv_file.should_receive(:load_file_commands).and_return([create_table_sql])
      subject_should_execute_sql(create_table_sql)
      csv_file.should_receive(:import_rows).and_yield(insert_row_sql)
      subject_should_execute_sql(insert_row_sql)

      subject.load_file(test_file_path, %w{header1 header2 header3})
    end
  end

  describe "#load_table" do

    let(:csv_file) { VoterFile::CSVDriver::CSVFile.new(test_file_path, stub) }
    let(:working_table) { VoterFile::CSVDriver::WorkingTable.new(stub, stub) }
    let(:proc) { lambda { true } }

    it "calls load_table_from_source when given a CSVFile input" do
      subject.should_receive(:load_table_from_source).with(csv_file, &proc).and_return(nil)
      subject.load_table(csv_file, &proc)
    end

    it "calls load_table_from_source when given a CSVFile input" do
      subject.should_receive(:load_table_from_source).with(csv_file, &proc).and_return(nil)
      subject.load_table(csv_file, &proc)
    end

    it "calls load_table_from_db otherwise" do
      subject.should_receive(:load_table_from_db).with(test_table_name, &proc)
      subject.load_table(test_table_name, &proc)
    end

  end

  describe "#load_table_from_db" do

    it "returns a DatabaseTable for the input table" do
      ar_model = stub(:column_names => ['col1'])
      Class.stub(:new => ar_model)

      db_connection = stub(:table_exists? => true)
      subject.stub(:db_connection).and_return(db_connection)

      db_table = subject.load_table_from_db(test_table_name) {}
      db_table.should be_a VoterFile::CSVDriver::DatabaseTable

      Class.rspec_reset
    end

    it "raises an exception if the input table doesn't exist" do
      db_connection = stub(:table_exists? => false)
      subject.stub(:db_connection).and_return(db_connection)

      expect { subject.load_table_from_db(test_table_name) }.to raise_error
    end

  end

  describe "#load_table_from_source" do

    it "executes SQL for loading a table" do
      working_table, sql, source = stub, stub, stub
      subject.stub(:create_working_table => working_table)

      working_table.should_receive(:load_table_commands).with(source).and_return([sql])
      subject_should_execute_sql(sql)

      subject.send(:load_table_from_source, source)
    end

  end

  describe "#prepare_table_for_geocoding" do
    it "calls the correct methods for generating a SQL command" do
      table, sql = stub, stub
      table.should_receive(:prepare_for_geocoding_commands).and_return([sql])
      subject_should_execute_sql(sql)
      subject.prepare_table_for_geocoding(table)
    end
  end

  describe "#merge_records" do

    it "executes the merging script provided by the merger" do
      merger = stub(:target_table => stub(:primary_key => :col_1))
      VoterFile::CSVDriver::RecordMerger.stub(:new => merger)
      sql1 = stub

      merger.should_receive(:merge_commands).and_return([sql1])
      subject_should_execute_sql(sql1)

      result = subject.merge_records do |m|
        m.should == merger
      end
      result.should == merger
    end

  end

  describe "#load_extension" do
    it "should raise an error if and only if the extension is not supported" do
      expect { subject.load_extension :nonexistent_aggregator }.to raise_error
      expect { subject.load_extension :nb_parse_election_name }.to_not raise_error
    end

    it "should add the specified aggregator to the 'loaded_extensions' attribute" do
      subject.loaded_extensions.should_receive(:<<)
      subject.load_extension :nb_parse_election_name
    end
  end

  describe "#exec_sql" do
    it "should execute the given sql command" do
      subject_should_execute_sql("SELECT 'test'")
      subject.exec_sql("SELECT 'test'")
    end
  end

  describe "#db_connection" do
    it "should return the database connection" do
      db_connection = stub
      subject.instance_variable_set :@db_connection, db_connection
      subject.send(:db_connection).should == db_connection
    end

    it "should load extensions" do
      subject.should_receive :init_extensions
      subject.send(:db_connection)
    end
  end

  describe "#init_extensions" do
    it "executes each extension definition" do
      db_connection = stub
      subject.instance_variable_set :@db_connection, db_connection
      subject.loaded_extensions << :nb_parse_election_name
      db_connection.should_receive(:execute).with(VoterFile::CSVDriver::SUPPORTED_EXTENSIONS[:nb_parse_election_name])
      subject.send(:init_extensions)
    end
  end

  describe "#copy_column" do
    it "should tell the target table to copy the given column" do
      target_table, source_table, sql = stub, stub, stub
      target_table.should_receive(:copy_column).with(:col_1, {from: source_table, key: :id}).and_return(sql)
      subject_should_execute_sql(sql)
      subject.copy_column :col_1, to: target_table, from: source_table, key: :id
    end
  end
end
