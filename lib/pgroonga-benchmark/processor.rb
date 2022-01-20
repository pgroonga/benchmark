require "benchmark"
require "yaml"

require_relative "faker-source"
require_relative "psql"
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

      search
    end

    private
    def default_psql_options
      {
        host: @config.postgresql.host,
        port: @config.postgresql.port,
        user: @config.postgresql.user,
        database: @config.postgresql.database,
      }
    end

    def open_psql(**options, &block)
      Psql.open(**default_psql_options.merge(options),
                &block)
    end

    def run_sql(sql, **options)
      open_psql(**options) do |psql|
        execute_sql(psql, sql)
      end
    end

    def execute_sql(psql, sql)
      result = ""
      sql.each_line do |line|
        @config.logger.debug("SQL: #{line}")
        result << psql.execute(line)
      end
      result << psql.finish
      result.each_line do |line|
        @config.logger.debug("SQL result: #{line}")
      end
      result
    end

    def ensure_database
      database = @config.postgresql.database
      result = run_sql("SELECT * FROM pg_catalog.pg_database " +
                       "WHERE datname = '#{database}';",
                       database: "postgres")
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

    def search
      list_paths(@config.select_dir).each do |path|
        process_path(path)
      end
    end

    def process_path(path)
      extension = File.extname(path)
      case extension
      when ".sql"
        @config.logger.info("Processing: #{path}")
        open_psql do |psql|
          elapsed = Benchmark.measure do
            File.open(path, encoding: "UTF-8") do |input|
              execute_sql(psql, input)
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
        open_psql do |psql|
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
