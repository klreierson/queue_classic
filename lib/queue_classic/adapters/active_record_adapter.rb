module QC
  module Conn
    class ActiveRecordAdapter
      def exec_query(stmt, params)
        connection.exec(stmt, params)
      end

      def connection
        ActiveRecord::Base.connection.raw_connection
      end
    end
  end
end
