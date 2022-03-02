require "groonga-synonym"

module PGroongaBenchmark
  class SynonymSource
    def initialize(data)
      @data = data
      @source = create_source(@data["source"])
      @table = @data["table"]
      @term_column = @data["term_column"] || "term"
      @synonyms_column = @data["synonyms_column"] || "synonyms"
    end

    def each_sql
      output = StringIO.new
      generator = GroongaSynonym::PGroongaGenerator.new(@source,
                                                        @table,
                                                        @term_column,
                                                        @synonyms_column,
                                                        output: output)
      generator.generate
      yield(output.string)
    end

    private
    def create_source(source)
      case source || "sudachi"
      when "sudachi"
        GroongaSynonym::Sudachi.new
      end
    end

    class Output
      attr_reader :result
      def initialize(psql)
        @psql = psql
        @result = ""
      end

      def print(sql)
        @result << @psql.execute(sql)
      end

      def finish
        @result << @psql.finish
      end
    end
  end
end
