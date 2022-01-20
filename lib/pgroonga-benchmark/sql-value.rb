module PGroongaBenchmark
  class SQLValue
    def initialize(value)
      @value = value
    end

    def to_s
      case @value
      when String
        quote_escaped_value = @value.gsub("'", "''")
        "'#{quote_escaped_value}'"
      else
        @value.to_s
      end
    end
  end
end
