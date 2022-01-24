require "benchmark"
require "yaml"

require_relative "faker-source"
require_relative "scenario-runner"
require_relative "synonym-source"

module PGroongaBenchmark
  class Processor
    def initialize(config, status)
      @config = config
      @status = status
    end

    def process
      unless @status["prepared"]
        ensure_database
        define_schema
        load_data
        define_index
        @status.update("prepared" => true)
      end

      list_paths(@config.scenario_dir).each do |path|
        runner = ScenarioRunner.new(@config, path)
        runner.run
      end
    end

    private
    def run_sql(sql, **options)
      @config.postgresql.open_connection(**options) do |connection|
        execute_sql(connection, sql)
      end
    end

    def execute_sql(connection, sql, &block)
      all_lines = ""
      sql.each_line do |line|
        @config.logger.debug("SQL: #{line}")
        all_lines << line
      end
      connection.exec(all_lines, &block)
    end

    def ensure_database
      database = @config.postgresql.database
      run_sql("SELECT * FROM pg_catalog.pg_database " +
              "WHERE datname = '#{database}';",
              database: "postgres") do |result|
        return unless result.empty?
        @config.logger.info("Creating database: #{database}")
        run_sql(<<-SQL, database: "postgres")
CREATE DATABASE #{database}
  WITH TEMPLATE = 'template0'
       ENCODING = 'UTF8'
       LC_COLLATE = 'C.UTF-8'
       LC_CTYPE = 'C.UTF-8';
        SQL
        @config.logger.info("Created database: #{database}")
      end
    end

    def list_paths(dir)
      paths = []
      Dir.glob("#{dir}/*") do |path|
        next unless File.file?(path)
        paths << path
      end
      paths.sort
    end

    def define_schema
      list_paths(@config.schema_dir).each do |path|
        process_path(path)
      end
    end

    def load_data
      list_paths(@config.data_dir).each do |path|
        process_path(path)
      end
    end

    def define_index
      list_paths(@config.index_dir).each do |path|
        process_path(path)
      end
    end

    def process_path(path)
      extension = File.extname(path)
      case extension
      when ".sql"
        @config.logger.info("Processing: #{path}")
        @config.postgresql.open_connection do |connection|
          elapsed = Benchmark.measure do
            File.open(path, encoding: "UTF-8") do |input|
              execute_sql(connection, input)
            end
          end
          @config.logger.info("Processed: #{path}: #{elapsed}")
        end
      when ".yaml"
        @config.logger.info("Processing: #{path}")
        data = YAML.load(File.read(path))
        case data["source"]
        when "faker"
          source = FakerSource.new(data["faker"])
        when "synonym"
          source = SynonymSource.new(data["synonym"])
        else
          raise "unsupported source: #{source}: #{path}"
        end
        @config.postgresql.open_connection do |psql|
          elapsed = Benchmark.measure do
            source.process(psql)
          end
          @config.logger.info("Processed: #{path}: #{elapsed}")
        end
      else
        raise "unsupported extension: #{extension}: #{path}"
      end
    end
  end
end
