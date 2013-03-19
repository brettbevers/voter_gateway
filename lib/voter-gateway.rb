require 'active_record'
require 'csv'

require 'voter_file/csv_driver'

require 'voter_file/csv_driver/csv_file'
require 'voter_file/csv_driver/database_table'
require 'voter_file/csv_driver/record_matcher'
require 'voter_file/csv_driver/record_merger'
require 'voter_file/csv_driver/working_table'

require 'voter_file/merge_audit_sql'
require 'voter_file/merge_audit'

require 'voter_file/import_job'
require 'voter_file/dedup_driver'
require 'voter_file/dedup_job'
require 'voter_file/csv_audit'
require 'voter_file/database_audit'
require 'voter_file/dedup_audit'

