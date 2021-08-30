require "datasets"

require_relative "psql"
require_relative "schema"

class PartitionedTablePreparer
  def initialize(database_name, options={})
    @database_name = database_name
    @options = options

    @language = options[:language] || :ja
    # https://ja.wikipedia.org/wiki/Wikipedia:%E6%97%A5%E6%9C%AC%E8%AA%9E%E7%89%88%E3%81%AE%E7%B5%B1%E8%A8%88
    @n_records = 1_500_000
    @n_records_per_partition = options[:n_records_per_partition] || 100_000

    @wikipedia = Datasets::Wikipedia.new(language: @language)
    @pages = @wikipedia.each
  end

  def prepare
    prepare_schema
    prepare_records
  end

  private
  def prepare_schema
    Psql.open(@database_name) do |psql|
      response = ""
      response << psql.execute(<<-SQL)
DROP TABLE IF EXISTS pages;
      SQL
      response << psql.execute(<<-SQL)
CREATE TABLE pages (
  id int PRIMARY KEY,
  title text not null,
  content text
) PARTITION BY RANGE (id);
      SQL
      offsets = 0.step(@n_records, @n_records_per_partition).each
      offsets.each_cons(2) do |start, next_start|
        response << psql.execute(<<-SQL)
CREATE TABLE pages_#{start}_#{next_start}
  PARTITION OF pages
  FOR VALUES FROM (#{start}) TO (#{next_start});
        SQL
      end
      response << psql.finish
    end
  end

  def prepare_records
    text_column = Column.new("name", "text")
    Psql.open(@database_name) do |psql|
      @wikipedia.each do |page|
        psql.execute(<<-SQL)
INSERT INTO pages
  VALUES (#{page.id},
          #{text_column.escape_value(page.title)},
          #{text_column.escape_value(page.revision.text)});
        SQL
      end
      psql.finish
    end
  end
end
