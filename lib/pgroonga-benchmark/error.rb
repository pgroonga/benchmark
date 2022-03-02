module PGroongaBenchmark
  class Error < StandardError
  end

  class VerifyError < Error
    attr_reader :actual_dumps
    attr_reader :expected_dumps
    def initialize(message,
                   actual_dumps: nil,
                   expected_dumps: nil)
      super(message)
      @actual_dumps = actual_dumps
      @expected_dumps = expected_dumps
    end
  end
end
