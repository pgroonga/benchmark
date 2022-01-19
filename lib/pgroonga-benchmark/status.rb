require "fileutils"
require "yaml"

module PGroongaBenchmark
  class Status
    def initialize(dir)
      @dir = dir
      @path = File.join(@dir, "status.yaml")
      if File.exist?(@path)
        @data = YAML.load(File.read(@path))
      else
        @data = {}
      end
    end

    def [](key)
      @data[key]
    end

    def update(data)
      @data.update(data)
      FileUtils.mkdir_p(@dir)
      File.open(@path, "w") do |output|
        output.puts(YAML.dump(@data))
      end
    end
  end
end
