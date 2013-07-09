require 'thread'

module QC

  class Pool
    def initialize(sz=1)
      @conns = SizedQueue.new(sz)
      sz.times {@conns.enq(Conn.new)}
    end

    def checkout
      if block_given?
        c = @conns.deq
        begin result = yield(c)
        ensure @conns.enq(c)
        end
        return result
      else
        @conns.deq
      end
    end
  end

  class Conn
    attr_accessor :c
    def initialize
      connect
    end

    def execute(stmt, *params)
      log(:at => "exec_sql", :sql => stmt.inspect)
      begin
        params = nil if params.empty?
        r = @c.exec(stmt, params)
        result = []
        r.each {|t| result << t}
        result.length > 1 ? result : result.pop
      rescue PG::Error => e
        disconnect
        raise
      end
    end

    def wait(chan)
      execute('LISTEN "' + chan + '"')
      wait_for_notify(WAIT_TIME)
      execute('UNLISTEN "' + chan + '"')
      drain_notify
    end

    def disconnect
      begin @c.finish
      ensure @c = nil
      end
    end

    def connect
      log(:at => "establish_conn")
      @c = PGconn.connect(*normalize_db_url(db_url))
      if @c.status != PGconn::CONNECTION_OK
        log(:error => @c.error)
      end
      @c.exec("SET application_name = '#{QC::APP_NAME}'")
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

    private

    def log(msg)
      QC.log(msg)
    end

    def wait_for_notify(t)
      Array.new.tap do |msgs|
        @c.wait_for_notify(t) {|event, pid, msg| msgs << msg}
      end
    end

    def drain_notify
      until @c.notifies.nil?
        log(:at => "drain_notifications")
      end
    end

  end
end
