require File.expand_path(File.dirname(__FILE__) + '/spec_helper')

describe VoterFile::CSVDriver::DatabaseTable do

  let(:subject) { VoterFile::CSVDriver::DatabaseTable.new('database_table') }

  describe "#set_primary_key" do

    it "sets both primary_key and primary_key_type" do
      subject.set_primary_key "col_1", "INT"
      subject.primary_key.should == :col_1
      subject.primary_key_type.should == :INT
    end

    it "should create an instance with the given name" do
      subject.name.should == 'database_table'
    end

  end
end