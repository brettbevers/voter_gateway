require File.expand_path(File.dirname(__FILE__) + '/spec_helper')

class TestJob < VoterFile::ImportJob::Base

  def initialize
  end

  def connection
  end
end

describe VoterFile::ImportJob::Base do

  let(:subject) { TestJob.new }

  let!(:driver) { stub(:clean_up! => true, :audit_results => true) }

  before do
    subject.stub(:job)
    VoterFile::CSVDriver.stub(:new => driver)
  end

  describe "#perform" do

    it "calls job and passes driver instance" do
      subject.should_receive(:job).with(driver)
      subject.perform
    end

    it "calls clean_up! on driver instance" do
      driver.should_receive(:clean_up!)
      subject.perform
    end
  end

  describe "#perform_audit" do
    it "initializes a CSVDriverAudit instance" do
      VoterFile::MergeAudit.should_receive(:new).and_return(driver)
      subject.should_receive(:job).with(driver)
      result = stub
      driver.should_receive(:audit_results).and_return(result)
      driver.should_receive(:clean_up!)
      subject.perform_audit.should == result
    end
  end
end
