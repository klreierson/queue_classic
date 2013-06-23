require 'rails/railtie'

module QC
  class Railtie < ::Rails::Railtie
    rake_tasks do
      load 'queue_classic/tasks.rb'
    end

    initializer "queue_classic.configure" do
      QC::Conn.adapter = QC::Conn::ActiveRecordAdapter.new
    end
  end
end
