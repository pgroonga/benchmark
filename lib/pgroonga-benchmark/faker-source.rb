require "faker"
require "natto"
require "romaji"

require_relative "record"
require_relative "sql-value"

module PGroongaBenchmark
  class FakerSource
    def initialize(data)
      @data = data
      @mecab = Natto::MeCab.new
    end

    def each_sql(&block)
      Faker::Config.locale = @data["locale"] if @data["locale"]
      @data["tables"].each do |table, config|
        generate_sqls(table, config, &block)
      end
    end

    private
    def generate_sqls(table, config, parent=nil, &block)
      n_records = config["n_records"] || 1
      case n_records
      when Hash
        n_records = rand(Range.new(n_records["min"], n_records["max"]))
      end
      primary_keys = config["primary_keys"]
      primary_keys = Array(primary_keys) if primary_keys
      update_columns = config["update_columns"]
      context = FakerContext.new(@data, @mecab)
      n_records.round.times do |i|
        record = FakerRecord.new(context, config["columns"], parent)
        column_names = record.column_names
        values = column_names.collect do |name|
          SQLValue.new(record[name]).to_s
        end
        sql = <<-INSERT
INSERT INTO #{table} (#{column_names.join(", ")})
  VALUES (#{values.join(", ")})
  ON CONFLICT DO NOTHING;
        INSERT
        options = {
          test_crash_safe: config["test_crash_safe"],
        }
        yield(sql, options)
        if primary_keys and update_columns
          update_record = FakerRecord.new(context, config["columns"], parent)
          primary_key_values = primary_keys.collect do |name|
            "#{name} = #{SQLValue.new(record[name])}"
          end
          update_values = update_columns.collect do |name|
            "#{name} = #{SQLValue.new(update_record[name])}"
          end
          update = <<-UPDATE
UPDATE #{table} SET #{update_values.join(", ")}
  WHERE #{primary_key_values.join(" AND ")};
          UPDATE
          yield(update)
        end
        sub_records = config["sub_records"] || {}
        (sub_records["tables"] || []).each do |sub_table, sub_config|
          generate_sqls(sub_table, sub_config, record, &block)
        end
      end
    end

    class FakerContext < Context
      attr_reader :mecab
      def initialize(data, mecab)
        super(data)
        @mecab = mecab
      end
    end

    class FakerRecord < Record
      private
      def katakanaize(value)
        katakana = ""
        context.mecab.parse(value) do |node|
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
