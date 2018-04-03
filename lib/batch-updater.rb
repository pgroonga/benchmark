require "benchmark"
require "csv"

require_relative "psql"
require_relative "schema"

class BatchUpdater
  def initialize(database_name, table_name, options={})
    @database_name = database_name
    @table_name = table_name
    @n_updates = options[:n_updates] || 100
    @run_vacuum = options[:run_vacuum]
    @run_vacuum = true if @run_vacuum.nil?
    @data_path = options[:data_path]
    if @data_path.nil?
      pattern = "#{@table_name}.csv*"
      @data_path = Dir.glob(pattern).first
      if @data_path.nil?
        raise "Data can't be found by pattern: <#{pattern}>"
      end
    end

    @n_no_updates = 0

    @schema = Schema.new(@database_name, @table_name)
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
      @schema.pgroonga_indexes.each do |name, _pgroonga_index|
        Psql.open(@database_name) do |psql|
          run_sql(psql, "SELECT pgroonga_wal_truncate('#{name}');")
          run_io_flush(psql)
          psql.finish
        end
      end
    end
    puts("WAL    #{@table_name}: #{elapsed}")
  end

  private
  def open_data
    data_path_string = @data_path.to_s
    case data_path_string
    when /\.xz/i
      IO.pipe do |input, output|
        pid = spawn("xzcat", data_path_string, :out => output)
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
        pid = spawn("zcat", data_path_string, :out => output)
        begin
          output.close
          yield(CSV.new(input))
        ensure
          Process.kill(:TERM, pid)
          Process.waitpid(pid)
        end
      end
    when
      @data_path.open do |input|
        yield(CSV.new(input))
      end
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
      @schema.columns.each_value.with_index do |column, i|
        if @schema.primary_key_names.include?(column.name)
          escaped_value = column.escape_value(next_record[i])
          conditions << "#{column.name} = #{escaped_value}"
        else
          escaped_value = column.escape_value(update_record[i])
          updates << "#{column.name} = #{escaped_value}"
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
