module QC
  module Setup
    def self.create
      c = Conn.new
      c.execute(File.read(CreateTable))
      c.execute(File.read(SqlFunctions))
      c.disconnect
    end

    def self.drop
      c = Conn.new
      c.execute("DROP TABLE IF EXISTS queue_classic_jobs CASCADE")
      c.execute(File.read(DropSqlFunctions))
      c.disconnect
    end
  end
end
