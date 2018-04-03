require "datasets"

require_relative "psql"
require_relative "schema"

class TextUpdater
  def initialize(database_name, table_name, column_name, options={})
    @database_name = database_name
    @table_name = table_name
    @column_name = column_name
    @options = options

    @language = options[:language] || :ja
    @n_records = options[:n_records] || 1_500_000
    @n_characters = options[:n_characters] || 3_000

    @schema = Schema.new(@database_name, @table_name)
    @wikipedia = Datasets::Wikipedia.new(language: @language)
    @pages = @wikipedia.each
    @buffer = ""
  end

  def update
    primary_key_values = collect_primary_key_values
    update_texts(primary_key_values)
  end

  private
  def collect_primary_key_values
    Psql.open(@database_name) do |psql|
      columns = @schema.primary_key_names.join(", ")
      response = psql.execute(<<-SQL)
SELECT #{columns} FROM #{@table_name} LIMIT #{@n_records}
      SQL
      response << psql.finish
      primary_key_values = []
      response.each_line do |line|
        primary_key_values << line.chomp.split("|")
      end
      primary_key_values
    end
  end

  def update_texts(primary_key_values)
    text_column = @schema.columns[@column_name]
    Psql.open(@database_name) do |psql|
      response = psql.execute("BEGIN;")
      primary_key_values.each do |record|
        text = next_text
        escaped_text = text_column.escape_value(text)
        conditions = []
        @schema.primary_key_names.each_with_index do |name, i|
          column = @schema.columns[name]
          escaped_value = column.escape_value(record[i])
          conditions << "#{column.name} = #{escaped_value}"
        end
        if text_column.array?
          set_value = "array_append(#{@column_name}, #{escaped_text})"
        else
          set_value = escaped_text
        end
        response << psql.execute(<<-SQL)
UPDATE #{@table_name}
  SET #{@column_name} = #{set_value}
  WHERE #{conditions.join(" AND ")};
        SQL
        check_response!(response)
      end
      response << psql.execute("COMMIT;")
      response << psql.finish
      check_response!(response)
    end
  end

  def next_text
    while @buffer.length < @n_characters
      begin
        page = @pages.next
      rescue StopIteration
        @pages = @wikipedia.each
        retry
      end
      @buffer << page.revision.text
    end

    @buffer.slice!(0, @n_characters)
  end

  def check_response!(response)
    io = StringIO.new(response)
    loop do
      line = io.gets
      if line.nil?
        response.slice!(0, io.pos)
        break
      end

      case line.chomp
      when "BEGIN"
      when "UPDATE 1"
      when "COMMIT"
      else
        $stderr.puts("Failed to update: #{line}")
      end
    end
  end
end
