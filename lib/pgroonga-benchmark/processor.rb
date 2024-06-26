require "benchmark"
require "yaml"

require_relative "error"
require_relative "faker-source"
require_relative "scenario-runner"
require_relative "sql-source"
require_relative "synonym-source"
require_relative "wikipedia-source"

module PGroongaBenchmark
  class Processor
    def initialize(config, status)
      @config = config
      @status = status
      @scenario_progress = nil
    end

    def process(&scenario_progress)
      unless @status["prepared"]
        ensure_database
        define_schema
        load_data
        define_index
        @status.update("prepared" => true)
      end

      list_paths(@config.scenario_dir).each do |path|
        runner = ScenarioRunner.new(@config, path)
        runner.run(&scenario_progress)
      end
    end

    private
    def run_sql(sql, test_crash_safe: nil, **options, &block)
      may_test_crash_safe =
        (@config.test_crash_safe? and !options.key?(:database))
      unless may_test_crash_safe
        @config.postgresql.open_connection(**options) do |connection|
          execute_sql(connection, sql, &block)
        end
        return
      end

      test_crash_safe = true if test_crash_safe.nil?
      if (@config.crash_ratio - rand).negative?
        test_crash_safe = false
      end
      unless test_crash_safe
        @config.postgresql.open_connection(**options) do |connection|
          execute_sql(connection, sql, &block)
        end
        @config.reference_postgresql.open_connection(**options) do |connection|
          execute_sql(connection, sql, &block)
        end
        return
      end

      crashed = false
      @config.postgresql.open_connection(**options) do |connection|
        backend_pid = connection.exec("SELECT pg_backend_pid();") do |result|
          result[0]["pg_backend_pid"]
        end
        pid = spawn(Gem.ruby,
                    "-e",
                    "sleep(rand(#{@config.crash_delay})); " +
                    "Process.kill(:KILL, #{backend_pid})",
                    err: File::NULL)
        begin
          execute_sql(connection, sql, &block)
        rescue PG::Error
          crashed = true
        end
        Process.waitpid(pid)
      end

      expected_dumps = []
      @config.reference_postgresql.open_connection(**options) do |connection|
        if crashed
          expected_dumps << dump_pgroonga_content(connection)
        end
        execute_sql(connection, sql) {}
        if crashed
          expected_dumps << dump_pgroonga_content(connection)
        end
      end

      timeout = 5
      deadline = Time.now + timeout
      loop do
        begin
          @config.postgresql.open_connection(**options) do |connection|
            execute_sql(connection, "SELECT pgroonga_command('status');")
          end
        rescue PG::ConnectionBad, PG::CannotConnectNow
          if Time.now > deadline
            raise ConnectionError.new("failed to connect in #{timeout} seconds")
          end
          sleep(0.01)
        else
          break
        end
      end

      return unless crashed

      actual_dumps = []
      @config.postgresql.open_connection(**options) do |connection|
        actual_dumps << dump_pgroonga_content(connection)
        execute_sql(connection, sql, &block)
        actual_dumps << dump_pgroonga_content(connection)
      end
      verify_database(expected_dumps, actual_dumps)
    end

    def dump_pgroonga_content(connection)
      pgroonga_table_names = []
      table_name_map = {}
      connection.exec("SELECT oid, relname FROM pg_catalog.pg_class " +
                      " WHERE relam IN " +
                      "         (SELECT oid FROM pg_catalog.pg_am " +
                      "           WHERE amname = 'pgroonga')") do |result|
        result.each do |row|
          oid = row["oid"]
          name = row["relname"]
          pgroonga_table_names << "pgroonga_table_name('#{name}')"
          table_name_map["Sources#{oid}"] = name
        end
      end
      return "" if pgroonga_table_names.empty?

      dump = +""
      pgroonga_table_names = pgroonga_table_names.sort.join(" || ', ' || ")
      connection.exec("SELECT pgroonga_command(" +
                      "  'dump', " +
                      "  ARRAY[" +
                      "    'dump_plugins', 'no'," +
                      "    'dump_schema', 'no'," +
                      "    'dump_indexes', 'no'," +
                      "    'dump_configs', 'no'," +
                      "    'tables', #{pgroonga_table_names}" +
                      "  ]" +
                      ") AS dump") do |result|
        in_load = false
        result[0]["dump"].each_line do |line|
          case line.chomp
          when /\Aload --table/
            in_load = true
            line = line.gsub(/Sources\d+/) do |table_name|
              table_name_map[table_name] || table_name
            end
          when /\A\[/
            if in_load
              line = line.gsub(/\A\[(?:"_key"|\d+),/, "[")
            end
          when "]"
            in_load = false
          end
          dump << line
        end
      end
      dump
    end

    def verify_pgroonga_indexes(connection)
      pgroonga_index_names = []
      connection.exec(<<-SQL) do |result|
SELECT indexrelid, indnkeyatts
  FROM pg_catalog.pg_index
 WHERE indexrelid IN (
         SELECT oid
           FROM pg_catalog.pg_class
          WHERE relam IN (
                  SELECT oid
                    FROM pg_catalog.pg_am
                   WHERE amname = 'pgroonga'))
      SQL
        result.each do |row|
          oid = row["indexrelid"]
          n_key_attributes = Integer(row["indnkeyatts"], 10)
          n_key_attributes.times do |i|
            pgroonga_index_names <<
              "pgroonga_index_column_name(#{oid}::regclass::text::cstring, #{i})"
          end
        end
      end
      pgroonga_index_names.each do |index_name|
        connection.exec(<<-SQL) do |result|
SELECT pgroonga_command('index_column_diff',
                        ARRAY[
                          'table', split_part(#{index_name}, '.', 1),
                          'name', split_part(#{index_name}, '.', 2)
                        ]) AS index_column_diff
        SQL
          response = JSON.parse(result[0]["index_column_diff"])
          unless response[0][0].zero?
            break if response[0][3].include?("column doesn't exist")
            raise VerifyError.new("failed to run index_column_diff",
                                  index_column_name: index_name,
                                  index_column_diff: response)
          end
          unless response[1].empty?
            raise VerifyError.new("index is broken",
                                  index_column_name: index_name,
                                  index_column_diff: response)
          end
        end
      end
    end

    def verify_database(expected_dumps, actual_dumps)
      if (actual_dumps & expected_dumps).empty?
        raise VerifyError.new("PGroonga data is different",
                              actual_dumps: actual_dumps,
                              expected_dumps: expected_dumps)
      end
      @config.postgresql.open_connection do |connection|
        verify_pgroonga_indexes(connection)
      end
    end

    def execute_sql(connection, sql, &block)
      all_lines = +""
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
        return unless result.ntuples.zero?
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
        elapsed = Benchmark.measure do
          File.open(path, encoding: "UTF-8") do |input|
            run_sql(input.read)
          end
        end
        @config.logger.info("Processed: #{path}: #{elapsed}")
      when ".yaml"
        @config.logger.info("Processing: #{path}")
        data = YAML.load(File.read(path))
        source = data["source"]
        case source
        when "faker"
          source = FakerSource.new(data["faker"])
        when "sql"
          source = SQLSource.new(data["sql"])
        when "synonym"
          source = SynonymSource.new(data["synonym"])
        when "wikipedia"
          source = WikipediaSource.new(data["wikipedia"])
        else
          raise "unsupported source: #{source}: #{path}"
        end
        if @config.test_crash_safe?
          elapsed = Benchmark.measure do
            source.each_sql do |sql, options|
              run_sql(sql, **(options || {}))
            end
          end
        else
          elapsed = Benchmark.measure do
            @config.postgresql.open_connection do |connection|
              source.each_sql do |sql|
                execute_sql(connection, sql)
              end
            end
          end
        end
        @config.logger.info("Processed: #{path}: #{elapsed}")
      else
        raise "unsupported extension: #{extension}: #{path}"
      end
    end
  end
end
