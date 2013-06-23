require 'thread'

module QC
  module Conn
    extend self

    def adapter=(adapter)
      @adapter = adapter
    end

    def adapter
      @adapter ||= PGAdapter.from_env
    end

    def execute(stmt, *params)
      adapter.execute(stmt, *params)
    end

    def notify(chan)
      adapter.notify(chan)
    end

    def wait(chan, t)
      adapter.wait(chan, t)
    end

    def transaction(&block)
      adapter.transaction(&block)
    end
  end
end
