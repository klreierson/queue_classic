require 'thread'

module QC
  module Conn
    extend self
    @exec_mutex = Mutex.new

    def adapter=(adapter)
      @adapter = adapter
    end

    def adapter
      @adapter ||= PGAdapter.from_env
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
      adapter.wait_for_notify(t)
    end

    def drain_notify
      adapter.drain_notify
    end
  end
end
