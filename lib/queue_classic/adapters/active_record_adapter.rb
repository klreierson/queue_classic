module QC
  module Conn
    class ActiveRecordAdapter < PGAdapter
      def initialize
        super []
      end

      def connection
        ActiveRecord::Base.connection.raw_connection
      end
    end
  end
end
