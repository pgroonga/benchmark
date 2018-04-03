#!/usr/bin/env ruby

require "optparse"
require "ostruct"

require_relative "../lib/text-updater"

options = OpenStruct.new
options.database = ENV["PGDATABASE"] || ENV["PGUSER"] || ENV["USER"]
options.table = nil
options.column = nil
options.language = :ja
options.n_records = 1_500_000
options.n_characters = 3_000

parser = OptionParser.new
parser.on("--database=DATABASE",
          "Use DATABASE as the target database.",
          "(#{options.database})") do |database|
  options.database = database
end
parser.on("--table=TABLE",
          "Use TABLE as the target table.") do |table|
  options.table = table
end
parser.on("--column=COLUMN",
          "Add texts to COLUMN.") do |column|
  options.column = column
end
parser.on("--language=LANGUAGE",
          "Use Wikipedia text in LANGUAGE.",
          "(#{options.language})") do |language|
  options.language = language.to_sym
end
parser.on("--n-records=N", Integer,
          "Added text of N records.",
          "(#{options.n_records})") do |n|
  options.n_records = n
end
parser.on("--n-characters=N", Integer,
          "Use N characters per record.",
          "(#{options.n_characters})") do |n|
  options.n_characters = n
end

parser.parse!(ARGV)

if options.table.nil?
  $stderr.puts("--table is missing")
  exit(false)
end

if options.column.nil?
  $stderr.puts("--column is missing")
  exit(false)
end

updater = TextUpdater.new(options.database,
                          options.table,
                          options.column,
                          language: options.language,
                          n_records: options.n_records,
                          n_characters: options.n_characters)
updater.update
