require 'minitest/autorun'
require 'queue_classic'

class BenchmarkTest < Minitest::Test

  def setup
    QC::Setup.drop
    QC::Setup.create
  end

  def test_enqueue
    n = 10_000
    start = Time.now
    n.times do
      QC.enqueue("1.odd?", [])
    end
    assert_equal(n, QC.count)

    elapsed = Time.now - start
    assert_in_delta(4, elapsed, 1)
  end

  def test_dequeue
    worker = QC::Worker.new
    worker.running = true
    n = 10_000
    n.times do
      QC.enqueue("1.odd?", [])
    end
    assert_equal(n, QC.count)

    start = Time.now
    n.times do
      worker.work
    end
    elapsed = Time.now - start

    assert_equal(0, QC.count)
    assert_in_delta(10, elapsed, 3)
  end

  def test_worker
    worker = QC::Worker.new(concurrency: 5)
    n = 100
    n.times {worker.queue.enqueue("1.odd?")}
    assert_equal(n, worker.queue.count)

    start = Time.now
    worker.start(n).map(&:join)
    elapsed = Time.now - start

    assert_equal(0, QC.count)
    assert_in_delta(10, elapsed, 3)
  end

end
