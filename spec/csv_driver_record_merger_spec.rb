require File.expand_path(File.dirname(__FILE__) + '/spec_helper')

describe VoterFile::CSVDriver::RecordMerger do
  let!(:working_source_table) { stub(:name => 'source_table') }
  let!(:working_target_table) { stub(:name => 'target_table') }
  let!(:subject) { VoterFile::CSVDriver::RecordMerger.new(working_source_table, working_target_table) }
  let(:column_name) { :column_name }

  its(:is_update_only) { should be_nil }
  its(:is_insert_only) { should be_nil }

  context "fixed correlated_columns" do

    before do
      subject.stub(:column_map => {column1: "column2"})
      subject.stub(:correlated_update_columns => [:column3])
      subject.stub(:correlated_insert_columns => [:column4, :column3])
      subject.stub(:merge_expressions => {column4: "$S + 100"})
      subject.stub(:insert_expressions => {foo: "s.column2 * 2"})
    end

    describe "#exact_match_group" do

      it "adds the column name to exact_match_groups" do
        subject.exact_match_groups.should_receive(:<<).with([:col_1, :col_2])
        subject.exact_match_group "col_1", "col_2"
      end

    end

    describe "#fuzzy_match_column" do
      it "adds the column name to fuzzy_match_columns" do
        subject.fuzzy_match_columns.should_receive(:+).with([:col_1, :col_2])
        subject.fuzzy_match_column "col_1", "col_2"
      end
    end

    describe "#exclude_column" do

      it "adds the column name to excluded_columns" do
        subject.excluded_columns.should_receive(:+).with([column_name])
        subject.exclude_column(column_name)
      end

    end

    describe "#exclude_column" do

      it "adds eaach column name to excluded_columns" do
        subject.excluded_columns.should_receive(:+).with([:col_1, :col_2])
        subject.exclude_column "col_1", "col_2"
      end

    end

    describe "#preserve_column" do

      it "should add the column name to preserved_columns" do
        subject.preserved_columns.should_receive(:+).with([:col_1, :col_2])
        subject.preserve_column "col_1", "col_2"
      end

    end

    describe "#move_columns" do

      it "merges its inputs with the column_map attribute" do
        column_map = {"column_1" => "column_2"}
        subject.column_map.should_receive(:[]=).with(:column_1, :column_2)

        subject.move_columns(column_map)
      end

    end

    describe "#update_only" do
      it "sets is_update_only flag to true" do
        subject.update_only
        subject.is_update_only.should be_true
      end
    end

    describe "#insert_only" do
      it "sets is_insert_only flag to true" do
        subject.insert_only
        subject.is_insert_only.should be_true
      end
    end

    describe "#update_columns" do
      it "returns the columns that will be updated" do
        update_columns = subject.update_columns

        update_columns.should == ["column2", :column4, :column3]
      end
    end

    describe "#update_values" do
      it "returns a list of SQL expressions that will be used for updates" do
        subject.stub(:target_table => stub(:name => "target_table"))
        subject.stub(:working_source_table => stub(:name => "working_table"))

        update_values = subject.update_values

        update_values.should == ["t.column1", "s.column4 + 100", "s.column3"]
      end
    end

    describe "#insert_columns" do
      it "returns the columns that will be inserted" do
        insert_columns = subject.insert_columns

        insert_columns.should == [:foo, :column4, :column3]
      end
    end

    describe "#insert_values" do
      it "returns a list of SQL expressions that will be used for inserts" do
        subject.stub(:target_table => stub(:name => "target_table"))

        insert_values = subject.insert_values

        insert_values.should == ["s.column2 * 2", "s.column4", "s.column3"]
      end
    end

  end

  describe "#correlated_update_columns" do

    it "returns list of columns that should be updated directly from the source file" do
      subject.stub(:source_table => stub(:table_column_names => [:column1, :column2, :column3, :column4]))
      subject.stub(:excluded_columns => [:column2], :preserved_columns => [:column3], :merge_expressions => {:column4 => 'lower(source_table.$)'})

      correlated_columns = subject.correlated_update_columns

      correlated_columns.should == [:column1]
    end

  end

  describe "#correlated_insert_columns" do
    it "returns the source table's columns that are not excluded" do
      subject.stub(:source_table => stub(:table_column_names => [:column1, :column2, :column3]))
      subject.stub(:excluded_columns => [:column2], :preserved_columns => [:column3], :insert_expressions => {:column3 => "lower($S)"})
      insert_columns = subject.correlated_insert_columns

      insert_columns.should == [:column1]
    end
  end

  describe "#match_conditions" do

    it "returns a SQL condition for matching columns" do
      subject.stub(:target_table => stub(:name => "target_table"))
      subject.stub(:working_table => stub(:name => "working_table"))

      match_conditions = subject.exact_match_conditions(["column1", "column2"])

      match_conditions.should ==
          "( ( s.column1 = t.column1 AND t.column1 IS NOT NULL ) AND ( s.column2 = t.column2 AND t.column2 IS NOT NULL ) )"
    end

  end

  context "merge SQL" do

    let!(:target_key_name) { VoterFile::CSVDriver::RecordMerger::TARGET_KEY_NAME }

    before do
      subject.stub(:target_table => stub(:name => "target_table", :primary_key => :column1, :primary_key_type => :INT),
                   :source_table => stub(:name => "source_table"),
                   :working_source_table => stub(:name => 'working_source_table'),
                   :working_target_table => stub(:name => 'working_target_table'),
                   :update_columns => ["column1", "column2"],
                   :insert_columns => ["column3", "column4"],
                   :update_values => ["value1", "value2"],
                   :column_constraints => [["col_1", "$S IS NOT NULL"], ["col_2", "$S > 2"], ["col_3", "$T = $S"]])
    end

    describe "#find_exact_match_sql" do
      let(:sql) { subject.find_exact_match_sql(["column2", "column3"]) }
      it "generates the right SQL lines" do
        sql.should include "UPDATE working_source_table"
        sql.should include "SET #{target_key_name} = t.column1"
        sql.should include "FROM target_table t"
        sql.should include "WHERE s.#{target_key_name} IS NULL AND ( ( s.column2 = t.column2 AND t.column2 IS NOT NULL )"
        sql.should include " AND ( s.col_1 IS NOT NULL AND s.col_2 > 2 AND t.col_3 = s.col_3 )"
        sql.should include " AND ( s.column3 = t.column3 AND t.column3 IS NOT NULL ) )"
      end

      it "should handle the case where there are no column constraints" do
        subject.stub(:column_constraints => [])
        sql.should include "UPDATE working_source_table"
        sql.should include "SET #{target_key_name} = t.column1"
        sql.should include "FROM target_table t"
        sql.should include "WHERE s.#{target_key_name} IS NULL AND ( ( s.column2 = t.column2 AND t.column2 IS NOT NULL )"
        sql.should include " AND ( s.column3 = t.column3 AND t.column3 IS NOT NULL ) )"
      end
    end

    describe "#update_target_records_sql" do
      let(:sql) { subject.update_target_records_sql }
      it "generates the right SQL lines" do
        sql.should include "UPDATE target_table"
        sql.should include "SET ( column1, column2 ) ="
        sql.should include "( value1, value2 )"
        sql.should include "FROM working_source_table s"
        sql.should include "WHERE s.#{target_key_name} = t.column1"
      end

      it "returns nil when insert only" do
        subject.stub(:is_insert_only => true)
        sql.should be_nil
      end
    end

    describe "#find_fuzzy_match_sql" do
      it "returns sql for trigram similarity search" do
        sql = subject.find_fuzzy_match_sql("column4")
        sql.should include "UPDATE working_source_table"
        sql.should include "SET #{target_key_name} ="
        sql.should include "( SELECT t.column1"
        sql.should include "FROM working_target_table t"
        sql.should include "WHERE ( s.column4 <-> t.column4 ) < 0.5"
        sql.should include "ORDER BY s.column4 <-> t.column4"
        sql.should include "LIMIT 1 )"
        sql.should include "WHERE s.#{target_key_name} IS NULL"
        sql.should include " AND ( s.col_1 IS NOT NULL AND s.col_2 > 2 AND t.col_3 = s.col_3 )"
      end
    end

    describe "#insert_remaining_sql" do
      let(:sql) { subject.insert_remaining_sql }
      it "returns SQL for inserting remaining rows from the working_table" do
        sql.should include "INSERT INTO target_table ( column3, column4 )"
        sql.should include "SELECT column3, column4"
        sql.should include "FROM working_source_table"
        sql.should include "WHERE s.working_target_id IS NULL AND ( s.col_1 IS NOT NULL AND s.col_2 > 2 )"
      end

      it "returns nil when update only" do
        subject.stub(:is_update_only => true)
        sql.should be_nil
      end
    end


    describe "#create_working_source_table_sql" do
      let(:sql) { subject.create_working_source_table_sql }
      it "returns SQL to initialize a working table for use in merging" do
        sql.should include "DROP TABLE IF EXISTS working_source_table;"
        sql.should include "CREATE TABLE working_source_table ( LIKE source_table );"
        sql.should include "ALTER TABLE working_source_table ADD COLUMN working_source_id SERIAL;"
        sql.should include "ALTER TABLE working_source_table ADD COLUMN #{target_key_name} INT;"
        sql.should include "INSERT INTO working_source_table ( SELECT * from source_table );"
      end
    end

    describe "#create_working_target_table_sql" do
      it "returns SQL to initialize a working table for use in merging" do
        subject.stub(:fuzzy_match_columns => ['column2'])
        sql = subject.create_working_target_table_sql
        sql.should include "DROP TABLE IF EXISTS working_target_table;"
        sql.should include "CREATE TABLE working_target_table ( column1 INT );"
        sql.should include "ALTER TABLE working_target_table ADD COLUMN column2 TEXT;"
        sql.should include "INSERT INTO working_target_table ( SELECT column1, column2 FROM target_table );"
      end
    end
  end

  describe "#constrain_column" do

    it "should record the constraint in column_constraints" do
      subject.column_constraints.should_receive(:<<).with(["col_1", "$S IS NOT NULL"])
      subject.constrain_column "col_1", "$S IS NOT NULL"
    end

  end

  describe "#column_constraint_conditions" do
    it "returns SQL conditions that constrain column values columns" do
      subject.stub(:column_constraints => [["col_1", "$S IS NOT NULL"], ["col_2", "$T > 2"]],
                   :working_table => stub(:name => "working_table"))
      subject.column_constraint_conditions.should == "( s.col_1 IS NOT NULL AND t.col_2 > 2 )"
    end

    it "returns nil if there are no constraints" do
      subject.stub(:column_constraints => [])
      subject.column_constraint_conditions.should be_nil
    end
  end

  describe "#merge_column_as" do
    it "adds a merge expression" do
      subject.merge_expressions.should_receive(:[]=).with(:col_1, "lower(col_1)")
      subject.merge_column_as("col_1", "lower(col_1)")
    end
  end

  describe "#merge_expressions_values" do

    it "substitutes the column for '$' in each merge expression" do
      subject.stub(:merge_expressions => {:col_1 => 'a', :col_2 => 'b + $S'},
                   :working_source_table => stub(:name => 'working_table'),
                   :target_table => stub(:name => 'target_table'))
      subject.merge_expressions_values.should == ['a', 'b + s.col_2']
    end
  end

  describe "#merge_commands" do

    before do
      subject.stub_chain(:target_table, :primary_key).and_return(true)
    end

    it "executes the merging script provided by the merger" do
      sql1, sql2, sql3, sql4, sql5, sql6 = stub, stub, stub, stub, stub, stub

      subject.should_receive(:create_working_source_table_sql).ordered.and_return(sql1)
      subject.should_receive(:create_working_target_table_sql).ordered.and_return(sql2)
      subject.should_receive(:find_exact_match_commands).ordered.and_return([sql3])
      subject.should_receive(:find_fuzzy_match_commands).ordered.and_return([sql4])
      subject.should_receive(:update_target_records_sql).ordered.and_return(sql5)
      subject.should_receive(:insert_remaining_sql).ordered.and_return(sql6)

      subject.merge_commands.should == [sql1, sql2, sql3, sql4, sql5, sql6]
    end

    it "strips nil values from the returned array" do
      sql1 = stub

      subject.should_receive(:create_working_source_table_sql).ordered.and_return(sql1)
      subject.should_receive(:create_working_target_table_sql).ordered.and_return(nil)
      subject.should_receive(:find_exact_match_commands).ordered.and_return([nil])
      subject.should_receive(:find_fuzzy_match_commands).ordered.and_return([nil])
      subject.should_receive(:update_target_records_sql).ordered.and_return(nil)
      subject.should_receive(:insert_remaining_sql).ordered.and_return(nil)

      subject.merge_commands.should == [sql1]
    end
  end

end