module PGroongaBenchmark
  class SQLSource
    def initialize(data)
      @data = data
    end

    def each_sql(&block)
      @data["sqls"].each do |sql|
        options = {
          test_crash_safe: @data["test_crash_safe"],
        }
        yield(sql, options)
      end
    end
  end
end
