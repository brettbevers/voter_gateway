module VoterFile

  module DedupJob

    def job(*args)
      raise "Must override '#job' in subclass"
    end

    def perform
      driver = DedupDriver.new
      begin
        job(driver)
      ensure
        driver.clean_up!
      end
    end

    def perform_audit
      driver = DedupAudit.new
      begin
        job(driver)
      ensure
        results = driver.audit_results
        driver.clean_up!
      end
      return results
    end

    class Base
      include DedupJob
    end
  end
end