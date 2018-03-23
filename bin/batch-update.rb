#!/usr/bin/env ruby

require "optparse"
require "ostruct"

require_relative "../lib/batch-updater"
require_relative "../lib/data-size-reporter"

options = OpenStruct.new
options.database = ENV["PGDATABASE"] || ENV["PGUSER"] || ENV["USER"]
options.table = nil
options.output_path = Pathname.new("data-size.csv")
options.data_path = nil
options.n_batches = 10
options.run_vacuum = true
options.n_updates = 1000

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
parser.on("--data-path=PATH",
          "Use CSV data stored at PATH.",
          "(./\#{table_name}.csv*)") do |path|
  options.data_path = Pathname.new(path)
end
parser.on("--[no-]run-vacuum",
          "Whether use VACUUM or not.",
          "(#{options.run_vacuum})") do |run_vacuum|
  options.run_vacuum = run_vacuum
end
parser.on("--n-batches=N", Integer,
          "Run N batches.",
          "(#{options.n_batches})") do |n|
  options.n_batches = n
end
parser.on("--n-updates=N", Integer,
          "Update N records per batch.",
          "(#{options.n_updates})") do |n|
  options.n_updates = n
end

parser.parse!(ARGV)

def run(table_name, output_path, options)
  DataSizeReporter.open(output_path,
                        options.database,
                        table_name) do |reporter|
    updater = BatchUpdater.new(options.database,
                               table_name,
                               n_updates: options[:n_updates],
                               run_vacuum: options[:run_vacuum],
                               data_path: options[:data_path])
    options.n_batches.times do |i|
      puts("Batch #{i}")
      reporter.report
      updater.update(i)
    end
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
      run(table_name, output_path, options)
    end
  end
else
  run(options.table, options.output_path, options)
end
