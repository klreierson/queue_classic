require 'thread'

module QC
  module Conn
    extend self
    @exec_mutex = Mutex.new

    def adapter=(adapter)
      @adapter = adapter
    end

    def adapter
      @adapter ||= PGAdapter.new
    end

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

    class ActiveRecordAdapter
      def exec_query(stmt, params)
        connection.exec(stmt, params)
      end

      def connection
        ActiveRecord::Base.connection.raw_connection
      end
    end

    def execute(stmt, *params)
      @exec_mutex.synchronize do
        QC.log(:at => "exec_sql", :sql => stmt.inspect)
        begin
          params = nil if params.empty?
          r = adapter.exec_query(stmt, params)
          result = []
          r.each {|t| result << t}
          result.length > 1 ? result : result.pop
        rescue PGError => e
          QC.log(:error => e.inspect)
          adapter.disconnect
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

    private

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
