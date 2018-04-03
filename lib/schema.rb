require_relative "psql"
require_relative "column"

class Schema
  attr_reader :database_name
  attr_reader :table_name
  attr_reader :columns
  attr_reader :primary_key_names
  def initialize(database_name, table_name)
    @database_name = database_name
    @table_name = table_name
    initialize_columns
    initialize_primary_key_names
  end

  private
  def initialize_columns
    Psql.open(@database_name) do |psql|
      response = psql.execute(<<-SQL)
SELECT column_name, data_type, udt_name
  FROM information_schema.columns
 WHERE table_catalog = '#{@database_name}' AND
       table_name = '#{@table_name}' AND
       table_name::regclass::oid = '#{@table_name}'::regclass::oid
      SQL
      response << psql.finish
      @columns = {}
      response.each_line do |line|
        name, data_type, udt_name = line.chomp.split("|")
        if data_type == "ARRAY"
          type = udt_name.gsub(/\A_/, "") + "[]"
        else
          type = udt_name
        end
        @columns[name] = Column.new(name, type)
      end
    end
  end

  def initialize_primary_key_names
    Psql.open(@database_name) do |psql|
      response = psql.execute(<<-SQL)
SELECT column_name
  FROM information_schema.constraint_column_usage AS usage
       INNER JOIN
       information_schema.table_constraints AS constraints
       USING (
         constraint_catalog,
         constraint_schema,
         constraint_name
       )
 WHERE constraint_type = 'PRIMARY KEY' AND
       usage.table_name = '#{@table_name}' AND
       usage.table_name::regclass::oid = '#{@table_name}'::regclass::oid
      SQL
      response << psql.finish
      @primary_key_names = response.split
    end
  end
end
