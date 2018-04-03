require "csv"
require "fileutils"
require "pathname"
require "time"

require_relative "psql"
require_relative "schema"

class DataSizeReporter
  class << self
    def open(output_path, database_name, table_name)
      if output_path.exist?
        output_path.open("a") do |output|
          output.sync = true
          yield(new(output, database_name, table_name))
        end
      else
        FileUtils.mkdir_p(output_path.dirname)
        output_path.open("w") do |output|
          output.sync = true
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

    @schema = Schema.new(@database_name, @table_name)
    initialize_csv
  end

  def report
    pg_all = run_sql("SELECT pg_database_size('#{@database_name}')",
                     type: :integer)
    pg_table = run_sql("SELECT pg_table_size(#{@schema.table_oid})",
                       type: :integer)
    pg_indexes = run_sql("SELECT pg_indexes_size(#{@schema.table_oid})",
                         type: :integer)

    pgroonga_all = 0
    pgroonga_data = 0
    pgroonga_indexes = 0
    pgroonga_record = []
    @schema.pgroonga_indexes.each do |name, pgroonga_index|
      table_disk_usage = groonga_disk_usage(pgroonga_index.groonga_table_name)
      pgroonga_record << table_disk_usage
      pgroonga_all += table_disk_usage
      pgroonga_data += table_disk_usage
      columns = pgroonga_index.groonga_table_schema["columns"]
      columns.each do |column_name, column_detail|
        column_disk_usage = groonga_disk_usage(column_detail["full_name"])
        pgroonga_record << column_disk_usage
        pgroonga_all += column_disk_usage
        pgroonga_data += column_disk_usage
      end
      columns.each do |column_name, column_detail|
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
    @schema.pgroonga_indexes.each do |name, pgroonga_index|
      headers << "pgroonga:#{name}:source"
      columns = pgroonga_index.groonga_table_schema["columns"]
      columns.each do |column_name, column_detail|
        headers << "pgroonga:#{name}:source:#{column_name}"
      end
      columns.each do |column_name, column_detail|
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
    Psql.run(@database_name, sql, type: type)
  end

  def run_groonga(command)
    Psql.run_groonga(@database_name, command)
  end

  def groonga_disk_usage(name)
    run_groonga("object_inspect #{name}")["disk_usage"]
  end
end
