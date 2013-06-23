module QC
  module Conn
    class PGAdapter
      def exec_query(stmt, params)
        connection.exec(stmt, params)
      end

      def connection
        @connection ||= connect
      end

      def connect
        QC.log(:at => "establish_conn")
        conn = PGconn.connect(*normalize_db_url(db_url))
        if conn.status != PGconn::CONNECTION_OK
          QC.log(:error => conn.error)
        end
        conn.exec("SET application_name = '#{QC::APP_NAME}'")
        conn
      end

      def disconnect
        begin connection.finish
        ensure @connection = nil
        end
      end

      def normalize_db_url(url)
        host = url.host
        host = host.gsub(/%2F/i, '/') if host

        [
         host, # host or percent-encoded socket path
         url.port || 5432,
         nil, '', #opts, tty
         url.path.gsub("/",""), # database name
         url.user,
         url.password
        ]
      end

      def db_url
        return @db_url if @db_url
        url = ENV["QC_DATABASE_URL"] ||
          ENV["DATABASE_URL"]    ||
          raise(ArgumentError, "missing QC_DATABASE_URL or DATABASE_URL")
        @db_url = URI.parse(url)
      end
    end
  end
end
