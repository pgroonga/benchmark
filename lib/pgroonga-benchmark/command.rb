require "optparse"

require_relative "config"
require_relative "processor"
require_relative "status"

module PGroongaBenchmark
  class Command
    def initialize
      @dir = "."
      @builtin_benchmark = nil
    end

    def run(args)
      catch do |tag|
        parse_args(args, tag)
        config = Config.new(@dir)
        config.use_builtin_benchmark(@builtin_benchmark) if @builtin_benchmark
        begin
          status = Status.new(@dir)
          processor = Processor.new(config, status)
          processor.process
          true
        rescue => error
          config.logger.error(error)
          raise
        end
      end
    end

    private
    def parse_args(args, tag)
      parser = OptionParser.new
      parser.on("--dir=DIR",
                "Use DIR as directory that has configuration files",
                "(#{@dir})") do |dir|
        @dir = dir
      end
      parser.on("--builtin-benchmark=BENCHMARK",
                "Use builtin BENCHMARK") do |benchmark|
        @builtin_benchmark = benchmark
      end
      parser.on("--version",
                "Show version and exit") do
        puts(VERSION)
        throw(tag, true)
      end
      parser.on("--help",
                "Show this message and exit") do
        puts(parser.help)
        throw(tag, true)
      end
      begin
        parser.parse!(args.dup)
      rescue OptionParser::InvalidOption => error
        puts(error.message)
        puts(parser.help)
        throw(tag, false)
      end
    end
  end
end
