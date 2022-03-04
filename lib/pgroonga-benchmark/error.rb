module PGroongaBenchmark
  class Error < StandardError
  end

  class VerifyError < Error
    attr_reader :actual_dumps
    attr_reader :expected_dumps
    attr_reader :index_column_name
    attr_reader :index_column_diff
    def initialize(message,
                   actual_dumps: nil,
                   expected_dumps: nil,
                   index_column_name: nil,
                   index_column_diff: nil)
      super(message)
      @actual_dumps = actual_dumps
      @expected_dumps = expected_dumps
      @index_column_name = index_column_name
      @index_column_diff = index_column_diff
    end
  end
end
