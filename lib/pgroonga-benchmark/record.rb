module PGroongaBenchmark
  class Counter
    def initialize
      @current = 0
    end

    def next
      value = @current
      @current += 1
      value
    end
  end

  class Context
    def initialize(data)
      @data = data
      @counters = {}
    end

    def nth_try
      @data["nth_try"]
    end

    def nth_job
      @data["nth_job"]
    end

    def counter(name)
      @counters[name] ||= Counter.new
    end
  end

  class Record
    attr_reader :context
    attr_reader :parent
    def initialize(context, columns, parent)
      @context = context
      @columns = columns
      @parent = parent
      @values = {}
    end

    def [](name)
      name = name.to_s
      @values[name] ||= evaluate(name)
    end

    def column_names
      @columns.keys
    end

    private
    def evaluate(name)
      expression = @columns[name]
      begin
        instance_eval(expression)
      rescue => error
        $stderr.puts("#{error.class}: #{error}")
        $stderr.puts(error.backtrace)
        raise "Failed to evaluate: #{name}: #{expression}"
      end
    end
  end
end
