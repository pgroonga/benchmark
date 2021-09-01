#!/usr/bin/env ruby

require "optparse"
require "ostruct"
require "time"

require_relative "../lib/synonym-preparer"

options = OpenStruct.new
options.database = ENV["PGDATABASE"] || ENV["PGUSER"] || ENV["USER"]

parser = OptionParser.new
parser.on("--database=DATABASE",
          "Use DATABASE as the target database.",
          "(#{options.database})") do |database|
  options.database = database
end

parser.parse!(ARGV)

preparer = SynonymPreparer.new(options.database)
preparer.prepare
