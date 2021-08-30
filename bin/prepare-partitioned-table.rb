#!/usr/bin/env ruby

require "optparse"
require "ostruct"
require "time"

require_relative "../lib/partitioned-table-preparer"

options = OpenStruct.new
options.database = ENV["PGDATABASE"] || ENV["PGUSER"] || ENV["USER"]
options.table = nil
options.column = nil
options.language = :ja
options.n_records_per_partition = 100_000

parser = OptionParser.new
parser.on("--database=DATABASE",
          "Use DATABASE as the target database.",
          "(#{options.database})") do |database|
  options.database = database
end
parser.on("--language=LANGUAGE",
          "Use Wikipedia text in LANGUAGE.",
          "(#{options.language})") do |language|
  options.language = language.to_sym
end
parser.on("--n-records-per-partition=N", Integer,
          "Put N records per partition.",
          "(#{options.n_records_per_partition})") do |n|
  options.n_records_per_partition = n
end

parser.parse!(ARGV)

preparer = PartitionedTablePreparer.new(
  options.database,
  language: options.language,
  n_records_per_partition: options.n_records_per_partition)
preparer.prepare
