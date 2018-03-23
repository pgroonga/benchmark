#!/usr/bin/env ruby

require "optparse"
require "ostruct"

require_relative "../lib/data-size-reporter"

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
