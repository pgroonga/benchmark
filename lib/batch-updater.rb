require "benchmark"
require "csv"

require_relative "psql"

class BatchUpdater
  def initialize(database_name, table_name, options={})
    @database_name = database_name
    @table_name = table_name
    @n_updates = options[:n_upates] || 100
    @run_vacuum = options[:run_vacuum]
    @run_vacuum = true if @run_vacuum.nil?
    @data_dir = options[:data_dir] || "."

    @n_no_updates = 0
    initialize_metadata
  end

  def update(nth_batch)
    open_data do |csv|
      elapsed = Benchmark.measure do
        Psql.open(@database_name) do |psql|
          update_records(csv, psql, nth_batch)
        end
      end
      puts("Update #{@table_name}: #{elapsed}")
      unless @n_no_updates.zero?
        puts("#{@table_name} has #{@n_no_updates} no updates")
      end
    end

    if @run_vacuum
      elapsed = Benchmark.measure do
        Psql.open(@database_name) do |psql|
          run_sql(psql, "VACUUM ANALYZE #{@table_name};");
          run_io_flush(psql)
          psql.finish
        end
      end
      puts("VACUUM #{@table_name}: #{elapsed}")
    end

    elapsed = Benchmark.measure do
      Psql.open(@database_name) do |psql|
        index_name = "#{@table_name}_full_text_search_index"
        run_sql(psql, "SELECT pgroonga_wal_truncate('#{index_name}');")
        run_io_flush(psql)
        psql.finish
      end
    end
    puts("WAL    #{@table_name}: #{elapsed}")
  end

  private
  def initialize_metadata
    Psql.open(@database_name) do |psql|
      response = psql.execute(<<-SQL)
SELECT column_name
  FROM information_schema.columns
 WHERE table_catalog = '#{@database_name}' AND
       table_name = '#{@table_name}' AND
       table_name::regclass::oid = '#{@table_name}'::regclass::oid
      SQL
      response << psql.finish
      @columns = response.split
    end
    Psql.open(@database_name) do |psql|
      response = psql.execute(<<-SQL)
SELECT column_name
  FROM information_schema.constraint_column_usage AS usage
       INNER JOIN
       information_schema.table_constraints AS constraints
       USING (
         constraint_catalog,
         constraint_schema,
         constraint_name
       )
 WHERE constraint_type = 'PRIMARY KEY' AND
       usage.table_name = '#{@table_name}' AND
       usage.table_name::regclass::oid = '#{@table_name}'::regclass::oid
      SQL
      response << psql.finish
      @primary_key_names = response.split
    end
  end

  def initialize_data
    pattern = "#{@data_dir}/#{@table_name}.csv*"
    @data_path = Dir.glob(pattern).first
    if @data_path.nil?
      raise "Data can't be found by pattern: <#{pattern}>"
    end
  end

  def open_data
    case @data_path
    when /\.xz/i
      IO.pipe do |input, output|
        pid = spawn("xzcat", data_path, :out => output)
        begin
          output.close
          yield(CSV.new(input))
        ensure
          Process.kill(:TERM, pid)
          Process.waitpid(pid)
        end
      end
    when /\.gz/i
      IO.pipe do |input, output|
        pid = spawn("zcat", data_path, :out => output)
        begin
          output.close
          yield(CSV.new(input))
        ensure
          Process.kill(:TERM, pid)
          Process.waitpid(pid)
        end
      end
    when
      File.open(@data_path) do |input|
        yield(CSV.new(input))
      end
    end
  end

  def sql_escape(value, type)
    case type
    when "varchar", "char", "date", "text", "text[]", "integer[]"
      quote_escaped_value = value.gsub(/'/, "''")
      "'#{quote_escaped_value}'"
    else
      value
    end
  end

  def run_sql(psql, sql)
    check_response(psql.execute(sql))
  end

  def run_io_flush(psql)
    run_sql(psql,
            "SELECT pgroonga_command('io_flush', ARRAY['only_opened', 'yes']);")
  end

  def check_response(response)
    response.each_line do |line|
      case line
      when /\AUPDATE (\d+)/
        n_updates = Integer($1, 10)
        @n_no_updates += 1 if n_updates.zero?
      when /\AERROR/
      end
    end
  end

  def update_records(csv, psql, nth_batch)
    records = csv.each
    record_queue = [records.next.to_a]
    nth_batch.times do
      record_queue.push(records.next)
    end

    run_sql(psql, "BEGIN;")
    @n_updates.times do
      next_record = records.next
      update_record = record_queue.shift
      record_queue.push(next_record)

      sql = "UPDATE #{@table_name}\n"
      conditions = []
      updates = []
      @columns.each_with_index do |column, i|
        if @primary_key_names.include?(column[:name])
          escaped_value = sql_escape(next_record[i], column[:type])
          conditions << "#{column[:name]} = #{escaped_value}"
        else
          escaped_value = sql_escape(update_record[i], column[:type])
          updates << "#{column[:name]} = #{escaped_value}"
        end
      end
      sql << "  SET\n"
      sql << "    "
      sql << updates.join(",\n    ")
      sql << "\n"
      sql << "  WHERE\n"
      sql << "    "
      sql << conditions.join(" AND\n    ")
      sql << ";"
      run_sql(psql, sql)
    end
    run_sql(psql, "COMMIT;")
    run_io_flush(psql)
    check_response(psql.finish)
  end
end
