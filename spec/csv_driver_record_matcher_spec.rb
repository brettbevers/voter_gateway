require File.expand_path(File.dirname(__FILE__) + '/spec_helper')

describe VoterFile::CSVDriver::RecordMatcher do
  let!(:working_source_table) { stub(:name => 'working') }
  let!(:source_table) { stub(:name => 'source') }
  let!(:target_table) { stub(:name => 'target', :primary_key => :id) }
  let!(:subject) { VoterFile::CSVDriver::RecordMatcher.new }

  before do
    subject.working_source_table = working_source_table
    subject.source_table = source_table
    subject.target_table = target_table
  end

  describe "#find_exact_matches" do
    it "yields a new RecordMatcher with the same working, source and target tables" do
      subject.find_exact_matches do |m|
        m.should be_a VoterFile::CSVDriver::RecordMatcher
        m.working_source_table.should == working_source_table
        m.source_table.should == source_table
        m.target_table.should == target_table
      end
    end

    it "adds the yielded RecordMatcher to the exact_match_groups array" do
      yielded_matcher = nil
      subject.find_exact_matches do |m|
        yielded_matcher = m
      end
      subject.exact_match_groups.should == [yielded_matcher]
    end

    it "returns the yielded RecordMatcher" do
      yielded = nil
      returned = subject.find_exact_matches do |m|
        yielded = m
      end
      returned.should == yielded
    end
  end

  describe "#find_exact_match_commands" do
    let(:list) { [ [:foo, :bar], [:goo, :tar] ] }

    it "generates a list of sql commands from lists of column names" do
      subject.exact_match_groups = list
      commands = subject.find_exact_match_commands
      commands.size.should == 2
      commands.each_with_index do |command, index|
        command.should be_a String
        command.should include "UPDATE working s"
        list[index].each {|x| command.should include "s.#{x} = t.#{x}" }
      end
    end

    it "generates a list of sql commands from lists of column names and RecordMatcher instances" do
      subject.exact_match_groups = list.dup
      subject.find_exact_matches {|m| m.exact_match_groups = [[:blue, :car]] }
      commands = subject.find_exact_match_commands
      list << [:blue, :car]
      commands.size.should == 3
      commands.each_with_index do |command, index|
        command.should be_a String
        command.should include "UPDATE working s"
        command.should include 'FROM target t'
        list[index].each {|x| command.should include "s.#{x} = t.#{x}" }
      end
    end
  end

  describe "joining tables" do
    let!(:join_table_1) { stub(:name => 'join1') }
    let!(:join_table_2) { stub(:name => 'join2') }

    before do
      subject.join_table join_table_1, "t.foreign_key_1 = join_table_1.id"
      subject.join_table join_table_2, "t.foreign_key_2 = join_table_2.id"
    end

    it "adds the table and condition to join_clauses" do
      subject.join_clauses.should == [ [join_table_1, "t.foreign_key_1 = join_table_1.id"],
                                       [join_table_2, "t.foreign_key_2 = join_table_2.id"] ]
    end

    it "allows the generation of join sql" do
      subject.join_sql.should ==
        "JOIN join1 ON ( t.foreign_key_1 = join_table_1.id ) JOIN join2 ON ( t.foreign_key_2 = join_table_2.id )"
    end

    it "inserts join sql in find_exact_match_commands" do
      subject.exact_match_group :foo, :bar
      subject.find_exact_match_commands.first.should include "FROM target t JOIN join1 ON ( t.foreign_key_1 = join_table_1.id ) JOIN join2 ON ( t.foreign_key_2 = join_table_2.id )"
    end
  end


end