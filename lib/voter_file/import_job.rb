module VoterFile

  module ImportJob
    def initialize(*args)
      raise "Must override '#initialize' in subclass"
    end

    def job(*args)
      raise "Must override '#job' in subclass"
    end

    def perform
      driver = CSVDriver.new
      begin
        job(driver)
      ensure
        driver.clean_up!
      end
    end

    def perform_audit
      driver = MergeAudit.new
      begin
        job(driver)
      ensure
        results = driver.audit_results
        driver.clean_up!
      end
      return results
    end

    class Base
      include ImportJob
    end
  end
end