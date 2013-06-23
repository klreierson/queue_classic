module QC
  module Conn
    class ActiveRecordAdapter < PGAdapter
      def connection
        ActiveRecord::Base.connection.raw_connection
      end
    end
  end
end
