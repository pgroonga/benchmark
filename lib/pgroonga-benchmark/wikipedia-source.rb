require "datasets"

require_relative "record"
require_relative "sql-value"

module PGroongaBenchmark
  class WikipediaSource
    def initialize(data)
      @data = data
      language = (@data["language"] || "en").to_sym
      @dataset = Datasets::Wikipedia.new(language: language)
    end

    def each_sql(&block)
      @data["tables"].each do |table, config|
        generate_sqls(table, config, &block)
      end
    end

    private
    def generate_sqls(table, config, parent=nil, &block)
      n_records = config["n_records"] || 1_000
      case n_records
      when Hash
        n_records = rand(Range.new(n_records["min"], n_records["max"]))
      end
      context = PageContext.new(@data, @dataset.each)
      n_records.round.times do |i|
        record = PageRecord.new(context, config["columns"], parent)
        column_names = record.column_names
        values = column_names.collect do |name|
          SQLValue.new(record[name]).to_s
        end
        sql = <<-INSERT
INSERT INTO #{table} (#{column_names.join(", ")})
  VALUES (#{values.join(", ")});
        INSERT
        yield(sql)
      end
    end

    class PageContext < Context
      def initialize(data, wikipedia)
        super(data)
        @wikipedia = wikipedia
        @pages = @wikipedia.each
      end

      def next_page
        begin
          @pages.next
        rescue StopIteration
          @pages = @wikipedia.each
          retry
        end
      end
    end

    class PageRecord < Record
      private
      def page
        @page ||= context.next_page
      end
    end
  end
end
