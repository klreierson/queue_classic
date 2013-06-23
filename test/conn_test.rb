require File.expand_path("../helper.rb", __FILE__)

class PGAdapterTest < QCTest
  def test_extracts_the_segemnts_to_connect
    database_url = "postgres://ryan:secret@localhost:1234/application_db"
    normalized = QC::Conn::PGAdapter.normalize_db_url(URI.parse(database_url))
    assert_equal ["localhost",
                  1234,
                  nil, "",
                  "application_db",
                  "ryan",
                  "secret"], normalized
  end

  def test_regression_database_url_without_host
    database_url = "postgres:///my_db"
    normalized = QC::Conn::PGAdapter.normalize_db_url(URI.parse(database_url))
    assert_equal [nil, 5432, nil, "", "my_db", nil, nil], normalized
  end

  def test_repair_after_error
    adapter = QC::Conn::PGAdapter.from_env
    assert_equal({"number"=>"1"}, adapter.execute("SELECT 1 as number"))

    connection = adapter.send(:connection)
    def connection.exec(*args); raise PGError end

    assert_raises(PG::Error) do
      adapter.execute("SELECT 1 as number")
    end
    assert_equal({"number"=>"1"}, adapter.execute("SELECT 1 as number"))
  rescue PG::Error
    adapter.send(:disconnect)
    assert false, "Expected to QC repair after connection error"
  end

end
