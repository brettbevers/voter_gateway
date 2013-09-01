require File.expand_path(File.dirname(__FILE__) + '/spec_helper')

describe VoterFile::CSVDriver::CSVFile do

  let(:test_file_path) do
    # stash the object so GC doesn't reclaim the file
    @tmp = Tempfile.new('test')
    @tmp.path
  end
  let(:working_table) { stub(name: 'working_table', mapped_column_names: [], column_converters: []) }
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
      actual_sql = ''
      subject.load_file_commands { |sql| actual_sql << sql }

      actual_sql.should include 'DROP TABLE IF EXISTS working_table;'
      actual_sql.should include 'CREATE TEMPORARY TABLE working_table ("header 1" TEXT, "header 2" TEXT, "header 3" TEXT);'
    end

    it 'returns the sql to create a temporary table with specified column types' do
      create_table_sql = stub
      working_table.should_receive(:mapped_column_names).and_return(['header_1'])
      working_table.should_receive(:create_table_sql).and_return(create_table_sql)
      subject.load_file_commands { |sql| sql.should == create_table_sql }
    end
  end

  describe '#map_column' do
    it 'passes arguments on to the working_table' do
      working_table.should_receive(:map_column) do |col_name, options|
        col_name.should == 'header_1'
        options[:type].should == :INT
        options[:as].should be_a Proc
      end

      subject.map_column 'header_1', :type => :INT
    end
  end
end
