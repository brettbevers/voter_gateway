require File.expand_path(File.dirname(__FILE__) + '/spec_helper')

def subject_should_execute_sql(sql)
  db_connection = stub
  subject.should_receive(:db_connection).ordered.and_return(db_connection)
  db_connection.should_receive(:execute).with(sql)
end

describe VoterFile::MergeAudit do

  let(:subject) { VoterFile::MergeAudit.new }

  describe "#merge_records" do
    it "matches records and then performs audit" do
      merger = stub(:target_table => stub(:primary_key => :col_1))
      VoterFile::MergeAudit::AuditMerger.stub(:new => merger)
      sql1 = stub

      merger.should_receive(:merge_commands).and_return([sql1])
      subject_should_execute_sql(sql1)

      subject.should_receive(:audit_merge).with(merger)

      result = subject.merge_records do |m|
        m.should == merger
      end
      result.should == merger
    end
  end
end

describe VoterFile::MergeAudit::AuditMerger do

  let!(:working_source_table) { stub(:name => 'source_table') }
  let!(:working_target_table) { stub(:name => 'target_table') }
  let!(:subject) { VoterFile::MergeAudit::AuditMerger.new { working_source_table } }

  before do
    subject.stub(:target_table => stub(:name => "target_table", :primary_key => :column1, :primary_key_type => :INT),
                 :source_table => stub(:name => "source_table"),
                 :working_source_table => stub(:name => 'working_source_table'),
                 :working_target_table => stub(:name => 'working_target_table'),
                 :column_constraints => [["col_1", "$S IS NOT NULL"], ["col_2", "$S > 2"]])
  end

  describe "#find_exact_match_sql" do
    let(:sql) { subject.find_exact_match_sql(["column2", "column3"], 0) }
    it "generates the right SQL lines" do
      sql.should include "UPDATE working_source_table"
      sql.should include "SET #{VoterFile::CSVDriver::RecordMerger::TARGET_KEY_NAME} = t.column1, #{VoterFile::MergeAudit::AuditMerger::MATCH_AUDIT_NAME} = 0"
      sql.should include "FROM target_table t"
      sql.should include "WHERE s.#{VoterFile::CSVDriver::RecordMerger::TARGET_KEY_NAME} IS NULL AND ( ( s.column2 = t.column2 AND t.column2 IS NOT NULL ) "
      sql.should include " AND ( s.col_1 IS NOT NULL AND s.col_2 > 2 )"
      sql.should include " AND ( s.column3 = t.column3 AND t.column3 IS NOT NULL ) )"
    end
  end

  describe "#create_working_source_table_sql" do
    let(:sql) { subject.create_working_source_table_sql }
    it "returns SQL to initialize a working table for use in merging" do
      sql.should include "DROP TABLE IF EXISTS working_source_table;"
      sql.should include "CREATE TABLE working_source_table ( LIKE source_table );"
      sql.should include "ALTER TABLE working_source_table ADD COLUMN working_source_id SERIAL;"
      sql.should include "ALTER TABLE working_source_table ADD COLUMN #{VoterFile::CSVDriver::RecordMerger::TARGET_KEY_NAME} INT;"
      sql.should include "INSERT INTO working_source_table ( SELECT * from source_table );"
      sql.should include "ALTER TABLE working_source_table ADD COLUMN #{VoterFile::MergeAudit::AuditMerger::MATCH_AUDIT_NAME} INT;"
    end
  end

  describe "#merge_commands" do
    it "executes the merging script provided by the merger" do
      sql1, sql3 = stub, stub

      subject.should_receive(:create_working_source_table_sql).ordered.and_return(sql1)
      subject.should_receive(:find_exact_match_commands).ordered.and_return([sql3])

      subject.merge_commands.should == [sql1, sql3]
    end
  end

end