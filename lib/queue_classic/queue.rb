require 'thread'

module QC
  class Queue

    attr_accessor :pool, :name, :top_bound
    def initialize(name, top_bound, pool)
      @name = name
      @top_bound = top_bound 
      @pool = pool
    end

    def enqueue(method, *args)
      QC.log_yield(:action => "insert_job") do
        s="INSERT INTO #{TABLE_NAME} (q_name, method, args) VALUES ($1, $2, $3)"
        pool.checkout do |c|
          c.execute(s, name, method, JSON.dump(args))
        end
      end
    end

    def lock
      s = "SELECT * FROM lock_head($1, $2)"
      if r = pool.checkout{|c| c.execute(s, name, top_bound)}
        {:id => r["id"], :method => r["method"], :args => JSON.parse(r["args"])}
      end
    end

    def wait
      pool.checkout {|c| c.wait(name)}
    end

    def delete(id)
      pool.checkout do |c| 
        c.execute("DELETE FROM #{TABLE_NAME} where id = $1", id)
      end
    end

    def delete_all
      s = "DELETE FROM #{TABLE_NAME} WHERE q_name = $1"
      pool.checkout {|c| c.execute(s, name)}
    end

    def count
      s = "SELECT COUNT(*) FROM #{TABLE_NAME} WHERE q_name = $1"
      r = pool.checkout{|c| c.execute(s, name)}
      r["count"].to_i
    end

  end
end
