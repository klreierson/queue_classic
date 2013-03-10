require 'rails/generators'
require 'rails/generators/migration'
require 'rails/generators/active_record/migration'

module QC
  class InstallGenerator < Rails::Generators::Base

    include Rails::Generators::Migration
    extend ActiveRecord::Generators::Migration

    namespace "queue_classic:install"
    self.source_paths << File.join(File.dirname(__FILE__), 'templates')

    desc 'Generates (but does not run) a migration to add a queue_classic table.'

    def create_migration_file
      migration_template 'add_queue_classic.rb', 'db/migrate/add_queue_classic.rb'
    end
  end
end
