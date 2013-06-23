module QC
  module Conn
    class PGAdapter
      def initialize(connection_args)
        @connection_args = connection_args
      end

      def to_s
        @connection_args
      end

      def exec_query(stmt, params)
        connection.exec(stmt, params)
      end

      def disconnect
        begin connection.finish
        ensure @connection = nil
        end
      end

      def connect
        QC.log(:at => "establish_conn")
        conn = PGconn.connect(*@connection_args)
        if conn.status != PGconn::CONNECTION_OK
          QC.log(:error => conn.error)
        end
        conn.exec("SET application_name = '#{QC::APP_NAME}'")
        conn
      end

      def connection
        @connection ||= connect
      end

      def self.from_env
        url = ENV["QC_DATABASE_URL"] ||
          ENV["DATABASE_URL"]    ||
          raise(ArgumentError, "missing QC_DATABASE_URL or DATABASE_URL")
        db_url = URI.parse(url)

        new(normalize_db_url(db_url))
      end

      def self.normalize_db_url(url)
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
    end
  end
end
