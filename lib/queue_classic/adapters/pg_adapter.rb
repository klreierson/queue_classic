module QC
  module Conn
    class PGAdapter
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

      def initialize(connection_args)
        @connection_args = connection_args
        @exec_mutex = Mutex.new
      end

      def to_s
        @connection_args
      end

      def execute(stmt, *params)
        @exec_mutex.synchronize do
          QC.log(:at => "exec_sql", :sql => stmt.inspect)
          begin
            params = nil if params.empty?
            r = exec(stmt, params)
            result = []
            r.each {|t| result << t}
            result.length > 1 ? result : result.pop
          rescue PGError => e
            QC.log(:error => e.inspect)
            disconnect
            raise
          end
        end
      end

      def notify(chan)
        QC.log(:at => "NOTIFY")
        execute('NOTIFY "' + chan + '"') #quotes matter
      end

      def wait(chan, t)
        listen(chan)
        wait_for_notify(t)
        unlisten(chan)
        drain_notify
      end

      def transaction
        begin
          execute("BEGIN")
          yield
          execute("COMMIT")
        rescue Exception
          execute("ROLLBACK")
          raise
        end
      end

      def exec(stmt, params)
        connection.exec(stmt, params)
      end

      private
      def connection
        @connection ||= connect
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

      def disconnect
        begin connection.finish
        ensure @connection = nil
        end
      end

      def listen(chan)
        QC.log(:at => "LISTEN")
        execute('LISTEN "' + chan + '"') #quotes matter
      end

      def unlisten(chan)
        QC.log(:at => "UNLISTEN")
        execute('UNLISTEN "' + chan + '"') #quotes matter
      end

      def wait_for_notify(t)
        connection.wait_for_notify(t) do |event, pid, msg|
          QC.log(:at => "received_notification")
        end
      end

      def drain_notify
        until connection.notifies.nil?
          QC.log(:at => "drain_notifications")
        end
      end
    end
  end
end
