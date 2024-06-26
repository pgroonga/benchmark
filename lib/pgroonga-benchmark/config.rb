require "etc"
require "fileutils"
require "logger"
require "pg"
require "yaml"

require_relative "ltsv-log-formatter"
require_relative "psql"

class Logger::LogDevice
  private
  def add_log_header(file)
    # Disable adding log header
  end
end

module PGroongaBenchmark
  class Config
    class << self
      def integer_value(data, env_key, value_key, default)
        value = nil
        env_name = data[env_key]
        if env_name
          value = ENV[env_name]
          if value
            begin
              value = Integer(value, 10)
            rescue ArgumentError
              value = nil
            end
          end
        end
        value || data[value_key] || default
      end
    end

    module PathResolvable
      private
      def resolve_path(path)
        File.expand_path(path, @dir)
      end
    end

    include PathResolvable

    def initialize(dir)
      @name = "pgroonga-benchmark"
      @dir = dir
      @path = File.join(@dir, "config.yaml")
      if File.exist?(@path)
        @data = YAML.load(File.read(@path))
      else
        @data = {}
      end
    end

    def use_builtin_benchmark(benchmark)
      base_dir =
        File.expand_path(File.join(__dir__, "..", "..", "benchmark", benchmark))
      @data["schema_dir"] = File.join(base_dir, "schema")
      @data["data_dir"] = File.join(base_dir, "data")
      @data["index_dir"] = File.join(base_dir, "index")
      @data["scenario_dir"] = File.join(base_dir, "scenario")
    end

    def logger
      @logger ||= create_logger
    end

    def log_path
      resolve_path(File.join(@data["log_dir"] || "log",
                             "#{@name}.log"))
    end

    def log_age
      @data["log_age"] || 7
    end

    def log_max_size
      @data["log_max_size"] || (1024 * 1024)
    end

    def log_period_suffix
      @data["log_period_suffix"] || "%Y-%m-%d"
    end

    def log_level
      @data["log_level"] || "info"
    end

    def schema_dir
      resolve_path(@data["schema_dir"] || "schema")
    end

    def data_dir
      resolve_path(@data["data_dir"] || "data")
    end

    def index_dir
      resolve_path(@data["index_dir"] || "index")
    end

    def scenario_dir
      resolve_path(@data["scenario_dir"] || "scenario")
    end

    def postgresql
      @postgresql ||= PostgreSQL.new(@dir, @data["postgresql"] || {})
    end

    def reference_postgresql
      @reference_postgresql ||=
        PostgreSQL.new(@dir, @data["reference_postgresql"] || {})
    end

    def test_crash_safe?
      @data.fetch("test_crash_safe", false)
    end

    def crash_ratio
      Float(@data["crash_ratio"] || 1.0)
    end

    def crash_delay
      Float(@data["crash_delay"] || 1.0)
    end

    private
    def create_logger
      path = log_path
      FileUtils.mkdir_p(File.dirname(path))
      Logger.new(path,
                 log_age,
                 log_max_size,
                 formatter: LTSVLogFormatter.new,
                 level: log_level,
                 progname: @name,
                 shift_period_suffix: log_period_suffix)
    end

    class PostgreSQL
      include Config::PathResolvable

      def initialize(dir, data)
        @dir = dir
        @data = data
      end

      def host
        @data["host"] || ENV["PGHOST"]
      end

      def port
        @data["port"] || ENV["PGPORT"]
      end

      def user
        @data["user"] || ENV["PGUSER"] || Etc.getlogin
      end

      def password
        @data["password"]
      end

      def database
        @data["database"] || ENV["PGDATABASE"] || user
      end

      def open_psql(**options, &block)
        psql_options = {
          host: options[:host] || host,
          port: options[:port] || port,
          user: options[:user] || user,
          database: options[:database] || database,
        }
        Psql.open(**psql_options, &block)
      end

      def open_connection(**options)
        pg_options = {
          host: options[:host] || host,
          port: options[:port] || port,
          user: options[:user] || user,
          password: options[:password] || password,
          dbname: options[:database] || database,
        }
        connection = PG.connect(pg_options)
        begin
          yield(connection)
        ensure
          connection.finish unless connection.finished?
        end
      end
    end
  end
end
