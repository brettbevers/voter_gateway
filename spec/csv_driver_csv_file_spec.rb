require File.expand_path(File.dirname(__FILE__) + '/spec_helper')

describe VoterFile::CSVDriver::CSVFile do
  let(:test_file_path) { Tempfile.new("test").path }
  let(:working_table) { stub(:name => 'working_table') }
  let(:subject) { VoterFile::CSVDriver::CSVFile.new(test_file_path, working_table) }

  describe "#initialize" do
    its(:delimiter) { should == ',' }
    its(:quote) { should == '^' }
  end

  describe "#path" do
    its(:path) { should == test_file_path }

    it "returns path of file, preferring processed to original" do
      subject.processed = "/other/path"
      subject.path.should == "/other/path"
    end
  end

  describe "#headers" do
    it "returns" do
      File.open(test_file_path, 'w') { |f| f << "header 1,header 2,header 3\ndata 1,data 2,data 3" }
      subject.headers.should == ['header 1', 'header 2', 'header 3']
    end
  end

  describe "#remove_expression" do
    it "creates a corrected file in which the character is removed" do
      File.open(test_file_path, 'w') { |f| f << "^header 1^,^header 2^,^header 3^\ndata 1,data 2,data 3\n" }
      subject.remove_expression '\^'
      File.open(subject.path, 'r').read.should == "header 1,header 2,header 3\ndata 1,data 2,data 3\n"
    end
  end

  describe "#remove_malformed_rows" do
    it "creates a corrected file in which malformed rows are removed" do
      File.open(test_file_path, 'w') { |f| f << "header 1,header 2,header 3\ndata 1,data 2,data 3\nd1,d2,d3,d4\n" }
      subject.remove_malformed_rows
      File.open(subject.path, 'r').read.should == "header 1,header 2,header 3\ndata 1,data 2,data 3\n"
    end

    it "ignores the delimiter inside quoted fields" do
      File.open(test_file_path, 'w') { |f| f << "'header 1','header 2','header 3'\n'data, 1','data, 2','data, 3'" }
      subject.delimiter = ","
      subject.quote = "'"
      subject.remove_malformed_rows
      File.open(subject.path, 'r').read.should == "'header 1','header 2','header 3'\n'data, 1','data, 2','data, 3'\n"
    end

    it "matches on mixed quoted and unquoted fields" do
      File.open(test_file_path, 'w') { |f| f << "'header 1',header 2,'header 3'\ndata 1,'data, 2',data 3" }
      subject.delimiter = ","
      subject.quote = "'"
      subject.remove_malformed_rows
      File.open(subject.path, 'r').read.should == "'header 1',header 2,'header 3'\ndata 1,'data, 2',data 3\n"
    end
  end

  describe "#load_file_commands" do

    it "calls the correct functions for assembling a SQL command" do
      sql1, sql2 = stub, stub
      subject.should_receive(:create_temp_table_sql).with([]).ordered.and_return(sql1)
      subject.should_receive(:bulk_copy_into_working_table_sql).ordered.and_return(sql2)

      subject.load_file_commands([]).should == [sql1, sql2]
    end

  end

  describe "#create_temp_table_sql" do

    it "returns sql to create a temporary table with headers from the csv" do
      subject.should_receive(:headers).and_return(["header1", "header2", "header3"])
      temp_table_sql = subject.create_temp_table_sql([])

      temp_table_sql.should include "DROP TABLE IF EXISTS working_table"
      temp_table_sql.should include "CREATE TABLE working_table (\"header1\" TEXT, \"header2\" TEXT, \"header3\" TEXT)"
    end

    it "returns sql to create a temporary table with custom headers" do
      temp_table_sql = subject.create_temp_table_sql(["header1", "header2", "header3"])

      temp_table_sql.should include "DROP TABLE IF EXISTS working_table"
      temp_table_sql.should include "CREATE TABLE working_table (\"header1\" TEXT, \"header2\" TEXT, \"header3\" TEXT)"
    end

  end

  describe "#bulk_copy_into_working_table_sql" do
    it "outputs SQL for copying into a table from a CSV file" do
      subject.stub( :delimiter => ",",
                    :quote => "\"",
                    :path => "/fake_path")

      sql = subject.bulk_copy_into_working_table_sql

      sql.should include "COPY working_table FROM '/fake_path'"
      sql.should include "FORMAT CSV"
      sql.should include "DELIMITER ','"
      sql.should include "HEADER true"
      sql.should include "ENCODING 'LATIN1'"
      sql.should include "QUOTE '\"'"
    end
  end

end
