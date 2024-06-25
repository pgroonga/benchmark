require "benchmark"
require "thread"
require "yaml"

require_relative "faker-source"

module PGroongaBenchmark
  class ScenarioRunner
    def initialize(config, path)
      @config = config
      @path = path
      @mutex = Thread::Mutex.new
      @statistics = []
    end

    def run
      data = YAML.safe_load(File.read(@path), aliases: true)
      setup = (data["setup"] || [])
      unless setup.empty?
        @config.postgresql.open_connection do |connection|
          setup.each do |sql|
            connection.exec(sql)
          end
        end
      end
      n_workers = data["n_workers"] || 1
      queue = Thread::Queue.new
      threads = n_workers.times.collect do
        Thread.new do
          @config.postgresql.open_connection do |connection|
            loop do
              job = queue.pop
              break if job.nil?
              process_job(connection, job)
            end
          end
        end
      end
      (data["n_tries"] || 1).times do |nth_try|
        data["jobs"].each_with_index do |job, nth_job, |
          if job["source"]
            source_job = SourceJob.new(nth_try, nth_job, job, @path)
            source_job.each_sql_job do |sql_job|
              queue << sql_job
            end
          else
            n = job["n"]
            if n
              n.times do |i|
                queue << SQLJob.new(job.merge("i" => i),
                                    @path)
              end
            else
              queue << SQLJob.new(job, @path)
            end
          end
        end
      end
      n_workers.times do
        queue << nil
      end
      threads.each(&:join)
      report
    end

    private
    def process_job(connection, job)
      elapsed_time = Benchmark.measure do
        job.execute(connection)
      end
      @mutex.synchronize do
        @statistics << [job.name, job.i, elapsed_time.real]
      end
    end

    def report
      grouped_statistics = @statistics.group_by do |name, i, elapsed_time|
        name
      end
      grouped_statistics.each do |name, statistics|
        elapsed_times = statistics.collect do |_, _, elapsed_time|
          elapsed_time
        end
        elapsed_times = elapsed_times.sort
        median = elapsed_times[elapsed_times.size / 2]
        mean = elapsed_times.sum / elapsed_times.size.to_f
        percentile_90 =
          elapsed_times[(elapsed_times.size * 0.9).ceil] ||
          elapsed_times.last
        percentile_95 =
          elapsed_times[(elapsed_times.size * 0.95).ceil] ||
          elapsed_times.last
        min, max = elapsed_times.minmax
        puts("#{name}:")
        puts("  Median: %.3fs" % median)
        puts("    Mean: %.3fs" % mean)
        puts(" 90%%tile: %.3fs" % percentile_90)
        puts(" 95%%tile: %.3fs" % percentile_95)
        puts("     Min: %.3fs" % min)
        puts("     Max: %.3fs" % max)
      end
    end

    class SourceJob
      def initialize(nth_try, nth_job, data, path)
        @nth_try = nth_try
        @nth_job = nth_job
        @data = data
        @path = path
      end

      def each_sql_job
        source = @data["source"]
        case source
        when "faker"
          source = FakerSource.new(@data["faker"].merge("nth_try" => @nth_try,
                                                        "nth_job" => @nth_job))
        when "wikipedia"
          source = WikipediaSource.new(@data["wikipedia"].merge("nth_try" => @nth_try,
                                                                "nth_job" => @nth_job))
        else
          raise "unsupported source for job: #{source}: #{@path}"
        end
        i = 0
        name = @data["name"]
        source.each_sql do |sql|
          yield(SQLJob.new(@data.merge("name" => name,
                                       "i" => i,
                                       "sql" => sql),
                           @path))
          i += 1
        end
      end
    end

    class SQLJob
      def initialize(data, path)
        @data = data
        @path = path
      end

      def name
        @data["name"]
      end

      def i
        @data["i"]
      end

      def sql
        @data["sql"]
      end

      def execute(connection)
        connection.exec(sql) do |result|
          # p result
        end
      end
    end
  end
end
