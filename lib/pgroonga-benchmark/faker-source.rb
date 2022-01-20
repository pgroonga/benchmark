require "faker"
require "natto"
require "romaji"

require_relative "sql-value"

module PGroongaBenchmark
  class FakerSource
    def initialize(data)
      @data = data
    end

    def process(psql)
      Faker::Config.locale = @data["locale"] if @data["locale"]
      @data["tables"].each do |table, config|
        insert_records(psql, table, config)
      end
      psql.finish
    end

    private
    def insert_records(psql, table, config, parent=nil)
      n_records = config["n_records"] || 1
      case n_records
      when Hash
        n_records = rand(Range.new(n_records["min"], n_records["max"]))
      end
      context = Context.new
      n_records.times do |i|
        record = Record.new(context, config["columns"], parent)
        column_names = record.column_names
        values = column_names.collect do |name|
          SQLValue.new(record[name]).to_s
        end
        psql.execute(<<-INSERT)
INSERT INTO #{table} (#{column_names.join(", ")})
  VALUES (#{values.join(", ")});
        INSERT
        sub_records = config["sub_records"] || {}
        (sub_records["tables"] || []).each do |sub_table, sub_config|
          insert_records(psql, sub_table, sub_config, record)
        end
      end
    end

    class Context
      def initialize
        @counters = {}
      end

      def counter(name)
        @counters[name] ||= Counter.new
      end
    end

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
        instance_eval(@columns[name])
      end

      def katakanaize(value)
        katakana = ""
        mecab = Natto::MeCab.new
        mecab.parse(value) do |node|
          feature = node.feature.dup.force_encoding("UTF-8").split(",")
          part_of_speech = feature[0]
          break if part_of_speech == "BOS/EOS"
          reading = feature[7]
          katakana << reading if reading
        end
        katakana
      end

      def hiraganaize(value)
        Romaji.kata2hira(katakanaize(value))
      end

      def katakana_to_romaji(value)
        Romaji.kana2romaji(value)
      end
    end
  end
end
