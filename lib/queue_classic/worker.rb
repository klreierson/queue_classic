require 'thread'

module QC
  class Worker

    attr_accessor :queue, :running, :limiter
    # In the case no arguments are passed to the initializer,
    # the defaults are pulled from the environment variables.
    def initialize(args={})
      @q_name      = args[:q_name]      ||= QC::QUEUE
      @top_bound   = args[:top_bound]   ||= QC::TOP_BOUND
      @fork_worker = args[:fork_worker] ||= QC::FORK_WORKER
      @limiter     = SizedQueue.new((args[:concurrency] || 5).to_i)
      @queue = Queue.new((args[:q_name] || QUEUE), args[:top_bound], Pool.new)
      log(args.merge(:at => "worker_initialized"))
      @running = true
    end

    # Start a loop and work jobs indefinitely.
    # Call this method to start the worker.
    # This is the easiest way to start working jobs.
    def start(n=nil)
      n.nil? ? loop {work} : n.times.map {work}
    end

    # Call this method to stop the worker.
    # The worker may not stop immediately if the worker
    # is sleeping.
    def stop
      @running = false
    end

    # This method will lock a job & process the job.
    def work
      @limiter.enq(1)
      Thread.new do
        begin Process.wait fork {after_fork; process(get_job)}
        ensure @limiter.deq
        end
      end
    end

    def get_job
      while @running
        if job = @queue.lock
          return job
        end
        @queue.wait
      end
    end

    # A job is processed by evaluating the target code.
    # Errors are delegated to the handle_failure method.
    # Also, this method will make the best attempt to delete the job
    # from the queue before returning.
    def process(job)
      begin
        call(job)
      rescue => e
        handle_failure(job, e)
      ensure
        @queue.delete(job[:id])
        log(:at => "delete_job", :job => job[:id])
      end
    end

    # Each job includes a method column. We will use ruby's eval
    # to grab the ruby object from memory. We send the method to
    # the object and pass the args.
    def call(job)
      klass = eval(job[:method].split(".").first)
      message = job[:method].split(".").last
      args = job[:args]
      klass.send(message, *args)
    end

    # This method will be called when an exception
    # is raised during the execution of the job.
    def handle_failure(job,e)
      log(:at => "handle_failure", :job => job, :error => e.inspect)
    end

    # This method should be overriden if
    # your worker is forking and you need to
    # re-establish database connections
    def after_fork 
      @queue.pool = Pool.new
      log(:at => "setup_child")
    end
    
    def log(data)
      QC.log(data)
    end

  end
end
