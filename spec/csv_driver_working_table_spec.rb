require File.expand_path(File.dirname(__FILE__) + '/spec_helper')

describe VoterFile::CSVDriver::WorkingTable do
  let(:test_table_name) { "test_table" }
  let(:subject) { VoterFile::CSVDriver::WorkingTable.new(test_table_name) }
  let(:target_table) { stub(:name => 'database_table') }

  describe "#add_column" do
    it "specifies a column" do
      subject.add_column "col_1", :type => :INT
      subject.table_columns.should == [{name: :col_1, type: :INT}]
    end

    it "adds sets the type to :TEXT if no type is specified" do
      subject.add_column "col_1"
      subject.table_columns.should == [{name: :col_1, type: :TEXT}]
    end
  end

  describe "#schema" do
    it "returns an array of sql schema declarations" do
      subject.add_column "col_1", :type => :INT
      subject.add_column "col_2", :type => :TEXT
      subject.schema.should == ["\"col_1\" INT", "\"col_2\" TEXT"]
    end

    it "refers to the target_table if one is set" do
      subject.target_table = target_table
      subject.schema.should == ["LIKE database_table"]
    end

    it "ignores declared table columns if a target_table is set" do
      subject.add_column "col_1", :type => :INT
      subject.target_table = target_table
      subject.schema.should == ["LIKE database_table"]
    end
  end

  describe "#mapped_column_names" do
    it "returns an array of columns that are flagged as 'mapped'" do
      subject.add_column "col_1", :type => :INT
      subject.add_column "col_2", :type => :TEXT, :mapped => true
      subject.mapped_column_names.should == [:col_2]
    end
  end

  describe "#table_column_names" do
    it "returns an array of all column nanes" do
      subject.add_column "col_1", :type => :INT
      subject.add_column "col_2", :type => :TEXT, :mapped => true
      subject.table_column_names.should == [:col_1, :col_2]
    end
  end

  describe "#copy_schema_for" do
    it "should set the target_table" do
      subject.copy_schema_from target_table
      subject.target_table.should == target_table
    end
  end

  describe "#map_column" do
    it "should add the column and flags it as :mapped" do
      subject.should_receive(:add_column) do |name, opts|
        name.should == "col_1"
        opts[:mapped].should be_true
      end
      subject.map_column "col_1", :type => :INT
    end

    it "should add the default type if none is provided" do
      subject.should_receive(:add_column) do |name, opts|
        name.should == "col_1"
        opts[:type].should == :TEXT
      end
      subject.map_column "col_1", :as => "Hello World!"
    end

    it "should add an 'as' option if there is a type specified" do
      subject.should_receive(:add_column) do |name, opts|
        name.should == "col_1"
        opts[:as].should == "$S::INT"
      end
      subject.map_column "col_1", :type => :INT
    end

    it "should substitute the 'from' column for '$S' in the resulting converter" do
      subject.map_column "col_1", :from => "count", :type => :INT, :as => '$S + 1'
      subject.column_converters.first.should == "\"count\" + 1"
    end

    it "should use 'as' option if no 'from' is specified" do
      subject.map_column "col_1", :as => "Hello World!", :type => :INT
      subject.column_converters.first.should == "Hello World!"
    end

  end

  describe "target_name" do
    it "should return the target table's name" do
      subject.target_table = target_table
      subject.target_name.should == target_table.name
    end
  end

  describe "#create_table_sql" do
    it "return sql for creating table with specified schema" do
      subject.create_table_sql.should match /DROP TABLE IF EXISTS #{test_table_name}/i
      subject.create_table_sql.should match /CREATE TEMPORARY TABLE #{test_table_name}/i
    end
  end

  describe "#insert_from_sql" do
    before do
      subject.should_receive(:mapped_column_names).and_return ["col_1", "col_2"]
      subject.should_receive(:column_converters).and_return ["\"col_1\"::TEXT", "\"col_2\"::TEXT"]
    end

    let(:sql) { subject.insert_from_sql "raw_csv_tmp" }
    it "should construct the insert command in SQL" do
      sql.should match /INSERT INTO #{test_table_name}.*"col_1", "col_2"/i
      sql.should match /SELECT "col_1"::TEXT, "col_2"::TEXT/
      sql.should match /FROM raw_csv_tmp/
      sql.should_not match /GROUP BY/i
    end

    it "should add a 'group by' clause if a grouping is specified" do
      subject.should_receive(:group_by_expressions).at_least(1).times.and_return ["col_1"]
      sql.should match /GROUP BY col_1/
    end

    it "should add a 'where' clause if column constraints are specified" do
      subject.should_receive(:column_constraints).
        at_least(1).times.and_return([[:col_1, "$S IS NOT NULL"], [:col_2, "$S > 1"]])
      sql.should include "WHERE ( \"col_1\" IS NOT NULL AND \"col_2\" > 1 )"
    end
  end

  describe "#copy_column_from_table_to_table" do
    it "should return correct SQL for copying a column" do
      source_table_name = "source_table"
      source_col = "source_col"
      target_col = "target_col"
      matching_col = "matching_col"

      sql = subject.map_column_from_table(source_table_name,
                                                    source_col,
                                                    target_col,
                                                    matching_col)

      sql.should include "UPDATE test_table AS t"
      sql.should include "SET \"target_col\" = s.\"source_col\""
      sql.should include "FROM source_table AS s"
      sql.should include "WHERE t.\"matching_col\" = s.\"matching_col\""
    end

    it "should return correct SQL for copying a column by matching on multiple keys" do
      source_table_name = "source_table"
      source_col = "source_col"
      target_col = "target_col"
      matching_cols = %w{matching_col_1 matching_col_2}

      sql = subject.map_column_from_table(source_table_name,
                                          source_col,
                                          target_col,
                                          matching_cols)

      sql.should include "UPDATE test_table AS t"
      sql.should include "SET \"target_col\" = s.\"source_col\""
      sql.should include "FROM source_table AS s"
      sql.should include "WHERE t.\"matching_col_1\" = s.\"matching_col_1\" AND t.\"matching_col_2\" = s.\"matching_col_2\""
    end
  end

  describe "#mark_records_as_needing_geocoding" do
    it "should return SQL for updating the needs_geocoding column to true" do
      subject.stub(:table_name => "table_name")

      sql = subject.mark_records_as_needing_geocoding
      sql.should include 'ALTER TABLE test_table ADD COLUMN needs_geocoding BOOLEAN;'
      sql.should include "SET needs_geocoding = (COALESCE(residential_address1, '') != ''"
      sql.should include "AND (residential_lat IS NULL"
      sql.should include "OR residential_lat = 0.0)"
    end

    it "should add the needs_geocoding column to the table_columns" do
      subject.should_receive(:add_column).with(:needs_geocoding, {type: :BOOLEAN})
      subject.mark_records_as_needing_geocoding
    end
  end

  describe "#populate_location_geometry" do
    it "should return SQL for settign the location_geometry column" do
      subject.stub(:table_name => "table_name")

      sql = subject.populate_location_geometry

      sql.should include "ALTER TABLE test_table ADD COLUMN location_geometry geometry(Geometry,4326);"
      sql.should include "UPDATE test_table"
      sql.should include "SET location_geometry = ST_GeomFromText('POINT(' || residential_lng || ' ' || residential_lat || ')'"
      sql.should include "4326)"
      sql.should include "WHERE residential_lat IS NOT NULL AND residential_lng IS NOT NULL"
    end
  end

  describe "#group_records_by" do
    it "should add expression to 'group_by_expressions'" do
      subject.group_by_expressions.should_receive(:<<).with("col_1")
      subject.group_records_by "col_1"
    end
  end

  its(:drop) { should match /DROP TABLE IF EXISTS test_table;/ }

  describe "#constrain_column" do
    it "should add the column and constraint to column_constraints" do
      subject.column_constraints.should_receive(:<<).with([:col_1, "$S IS NOT NULL"])
      subject.constrain_column 'col_1', '$S IS NOT NULL'
    end
  end

  describe "#column_constraint_conditions" do
    it "should return the conjunction of column constraints" do
      subject.stub(:column_constraints => [[:col_1, "$S IS NOT NULL"], [:col_2, "$S > 1"]])
      subject.column_constraint_conditions.should == "( \"col_1\" IS NOT NULL AND \"col_2\" > 1 )"
    end
  end
end
