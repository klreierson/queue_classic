require 'rails/railtie'

module QC
  class Railtie < ::Rails::Railtie
    rake_tasks do
      load 'queue_classic/tasks.rb'
    end

    initializer "queue_classic.configure" do
      ActiveSupport.on_load(:active_record) do
        QC::Conn.connection = ActiveRecord::Base.connection.raw_connection
      end
    end
  end
end
