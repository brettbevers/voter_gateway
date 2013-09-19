module VoterFile
  module PostgresCopy
    class Error < StandardError; end

    class Writer
      def initialize(raw_conn)
        @conn = raw_conn
      end

      def write(*array)
        line = array.map do |val|
          if val.nil?
            "\\N"
          elsif val == '\\'
            val = "\\N"
          elsif val.is_a?(DateTime)
            val.to_s(:db)
          else
            val.to_s
          end
        end.join("\t")
        line << "\n"
        until @conn.put_copy_data(line)
          # wait for connection to be writable
          sleep 0.1
        end
      end
    end

    #copy all events from the other adapter into my database
    def self.copy(table_name, attributes, conn, truncate = false, &block)
      raw_conn = conn.raw_connection

      if truncate
        sql = "TRUNCATE TABLE #{table_name};"
      else
        sql = ""
      end

      sql += "COPY #{table_name} "
      sql += "(#{attributes.map(&:to_s).join(', ')}) "
      sql += "FROM STDIN " # WITH DELIMITER '\t' csv QUOTE ''''

      raw_conn.exec(sql)

      begin
        block.call(Writer.new(raw_conn))
      rescue Errno => err
        errmsg = "%s while reading copy data: %s, message: %s" % [ err.class.name, err.message ]
        raw_conn.put_copy_end( errmsg )
        raise Error, errmsg
      rescue PG::Error => e
        raw_conn.exec "ROLLBACK"
        raise Error, e
      else
        raw_conn.put_copy_end
        while res = raw_conn.get_result #KEEP THIS HERE - it's necessary to flush the buffer
          unless res.result_error_message.blank?
            raise Error, res.result_error_message
          end
        end
      end
    end
  end
end
