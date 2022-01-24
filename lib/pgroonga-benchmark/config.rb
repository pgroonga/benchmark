require "fileutils"
require "logger"
require "pg"
require "yaml"

require_relative "ltsv-log-formatter"

class Logger::LogDevice
  private
  def add_log_header(file)
    # Disable adding log header
  end
end

module PGroongaBenchmark
  class Config
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

      def psql_options
        {
          host: host,
          port: port,
          user: user,
          database: database,
        }
      end

      def open_connection(**options)
        default_options = {
          host: host,
          port: port,
          user: user,
          password: password,
          dbname: database,
        }
        connection = PG.connect(default_options.merge(options))
        begin
          yield(connection)
        ensure
          connection.finish unless connection.finished?
        end
      end
    end
  end
end
