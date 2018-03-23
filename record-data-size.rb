#!/usr/bin/env ruby

require "csv"
require "fileutils"
require "json"
require "optparse"
require "ostruct"
require "pathname"
require "time"

options = OpenStruct.new
options.database = ENV["PGDATABASE"] || ENV["PGUSER"] || ENV["USER"]
options.table = nil
options.output_path = Pathname.new("data-size.csv")

parser = OptionParser.new
parser.on("--database=DATABASE",
          "Use DATABASE as the target database.",
          "(#{options.database})") do |database|
  options.database = database
end
parser.on("--table=TABLE",
          "Use TABLE as the target table.",
          "(all tables)") do |table|
  options.table = table
end
parser.on("--output=PATH",
          "Output data size statistics to PATH.",
          "(#{options.output_path})") do |path|
  options.output_path = Pathname.new(path)
end

parser.parse!(ARGV)

class Psql
  class << self
    def open(database_name)
      psql = new(database_name)
      begin
        yield(psql)
      ensure
        psql.close
      end
    end
  end

  def initialize(database_name)
    input, @output = IO.pipe
    @input, output = IO.pipe
    @pid = spawn("psql",
                 "--dbname", database_name,
                 "--no-psqlrc",
                 "--no-align",
                 "--tuples-only",
                 :in => input,
                 :out => output)
    input.close
    output.close
  end

  def execute(sql)
    @output.puts(sql)
    @output.flush
    read
  end

  def finish
    @output.close
    @input.read
  end

  def read(timeout=0)
    response = ""
    loop do
      break if IO.select([@input], nil, nil, timeout).nil?
      response << @input.gets
      timeout *= 0.1
    end
    response
  end

  def close
    return if @pid.nil?
    @input.close
    @output.close unless @output.closed?
    pid, status = Process.waitpid2(@pid)
    @pid = nil
    unless status.success?
      raise "Failed to run psql: #{status.to_i}"
    end
  end
end

class DataSizeReporter
  class << self
    def open(output_path, database_name, table_name)
      if output_path.exist?
        output_path.open("a") do |output|
          yield(new(output, database_name, table_name))
        end
      else
        FileUtils.mkdir_p(output_path.dirname)
        output_path.open("w") do |output|
          yield(new(output, database_name, table_name, write_headers: true))
        end
      end
    end
  end

  def initialize(output, database_name, table_name, options={})
    @output = output
    @database_name = database_name
    @table_name = table_name
    @options = options
    initialize_metadata
    initialize_csv
  end

  def report
    pg_all = run_sql("SELECT pg_database_size('#{@database_name}')",
                     type: :integer)
    pg_table = run_sql("SELECT pg_table_size(#{@table_oid})",
                       type: :integer)
    pg_indexes = run_sql("SELECT pg_indexes_size(#{@table_oid})",
                         type: :integer)

    pgroonga_all = 0
    pgroonga_data = 0
    pgroonga_indexes = 0
    pgroonga_record = []
    @target_pgroonga_indexes.each do |name, detail|
      table_disk_usage = groonga_disk_usage(detail[:groonga])
      pgroonga_record << table_disk_usage
      pgroonga_all += table_disk_usage
      pgroonga_data += table_disk_usage
      detail[:schema]["columns"].each do |column_name, column_detail|
        column_disk_usage = groonga_disk_usage(column_detail[:full_name])
        pgroonga_record << column_disk_usage
        pgroonga_all += column_disk_usage
        pgroonga_data += column_disk_usage
      end
      detail[:schema]["columns"].each do |column_name, column_detail|
        column_detail["indexes"].each do |index|
          lexicon_disk_usage = groonga_disk_usage(index["table"])
          index_column_disk_usage = groonga_disk_usage(index["full_name"])
          pgroonga_record << lexicon_disk_usage
          pgroonga_record << index_column_disk_usage
          pgroonga_all += lexicon_disk_usage + index_column_disk_usage
          pgroonga_indexes += lexicon_disk_usage + index_column_disk_usage
        end
      end
    end

    @csv << [
      Time.now.iso8601,
      pg_all + pgroonga_all,
      pg_all,
      pg_table,
      pg_indexes,
      pgroonga_all,
      pgroonga_data,
      pgroonga_indexes,
      *pgroonga_record
    ]
  end

  private
  def initialize_metadata
    @pgroonga_oid = run_sql("SELECT oid " +
                            "FROM pg_catalog.pg_am " +
                            "WHERE amname = 'pgroonga'",
                            type: :integer)
    @table_oid = run_sql("SELECT '#{@table_name}'::regclass::oid",
                         type: :integer)
    @schema = run_groonga("schema")

    target_pgroonga_indexes = run_sql(<<-SQL)
SELECT indexrelid, relname
  FROM pg_catalog.pg_index
       INNER JOIN
         pg_catalog.pg_class
         ON (indexrelid = oid)
 WHERE relam = #{@pgroonga_oid} AND
       indrelid = #{@table_oid}
    SQL
    @target_pgroonga_indexes = {}
    target_pgroonga_indexes.each_line do |line|
      oid, name = line.chomp.split("|")
      oid = Integer(oid, 10)
      groonga_table = run_sql("SELECT pgroonga_table_name('#{name}')",
                              type: :string)
      @target_pgroonga_indexes[name] = {
        oid: oid,
        name: name,
        groonga: groonga_table,
        schema: @schema["tables"][groonga_table],
      }
    end
  end

  def initialize_csv
    headers = [
      "timestamp",
      "all",
      "postgresql-all",
      "postgresql-table",
      "postgresql-indexes",
      "pgroonga-all",
      "pgroonga-data",
      "pgroonga-indexes",
    ]
    @target_pgroonga_indexes.each do |name, detail|
      headers << "pgroonga:#{name}:source"
      detail[:schema]["columns"].each do |column_name, column_detail|
        headers << "pgroonga:#{name}:source:#{column_name}"
      end
      detail[:schema]["columns"].each do |column_name, column_detail|
        column_detail["indexes"].each do |index|
          headers << "pgroonga:#{name}:index:#{column_name}:lexicon"
          headers << "pgroonga:#{name}:index:#{column_name}:column"
        end
      end
    end
    @csv = CSV.new(@output,
                   write_headers: @options[:write_headers],
                   headers: headers)
  end

  def run_sql(sql, type: nil)
    Psql.open(@database_name) do |psql|
      response = psql.execute(sql)
      response << psql.finish
      psql.close

      case type
      when :integer
        Integer(response, 10)
      when :string
        response.chomp
      when :json
        JSON.parse(response)
      else
        response
      end
    end
  end

  def run_groonga(command)
    response = run_sql("SELECT pgroonga_command('#{command}')",
                       type: :json)
    header, body = response
    unless header[0].zero?
      message = "Failed to execute Groonga command: "
      message << "#{header.inspect}: <#{command}>"
      raise message
    end
    body
  end

  def groonga_disk_usage(name)
    run_groonga("object_inspect #{name}")["disk_usage"]
  end
end

if options.table.nil?
  Psql.open(options.database) do |psql|
    response = psql.execute(<<-SQL)
WITH
  pgroonga_oids AS (
    SELECT oid
      FROM pg_catalog.pg_am
     WHERE amname = 'pgroonga'
  ),
  pgroonga_index_oids AS (
    SELECT oid
      FROM pg_catalog.pg_class
     WHERE relam = (SELECT oid FROM pgroonga_oids)
  ),
  pgroonga_indexed_table_oids AS (
    SELECT indrelid AS oid
      FROM pg_catalog.pg_index
      WHERE indexrelid IN (SELECT oid FROM pgroonga_index_oids)
  )
SELECT relname
  FROM pg_catalog.pg_class
 WHERE oid IN (SELECT oid FROM pgroonga_indexed_table_oids);
    SQL
    response << psql.finish
    psql.close

    response.each_line do |table_name|
      table_name = table_name.chomp
      ext = options.output_path.extname
      output_path = options.output_path.sub_ext("-#{table_name}#{ext}")
      DataSizeReporter.open(output_path,
                            options.database,
                            table_name) do |reporter|
        reporter.report
      end
    end
  end
else
  DataSizeReporter.open(options.output_path,
                        options.database,
                        options.table) do |reporter|
    reporter.report
  end
end
