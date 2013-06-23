require 'rails/railtie'

module QC
  class Railtie < ::Rails::Railtie
    rake_tasks do
      load 'queue_classic/tasks.rb'
    end

    initializer "queue_classic.configure" do
      require 'queue_classic/adapters/active_record_adapter'
      QC::Conn.adapter = QC::Conn::ActiveRecordAdapter.new
    end
  end
end
