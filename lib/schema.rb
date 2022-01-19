require_relative "pgroonga-benchmark/psql"
require_relative "column"
require_relative "pgroonga-index"

class Schema
  attr_reader :database_name
  attr_reader :table_name
  attr_reader :table_oid
  attr_reader :columns
  attr_reader :primary_key_names
  attr_reader :pgroonga_indexes
  def initialize(database_name, table_name)
    @database_name = database_name
    @table_name = table_name

    initialize_table_oid
    initialize_columns
    initialize_primary_key_names
    initialize_pgroonga_indexes
  end

  private
  def initialize_table_oid
    @table_oid = PGroongaBenchmark::Psql.run(<<-SQL, type: :integer, database: @database_name)
SELECT '#{@table_name}'::regclass::oid;
    SQL
  end

  def initialize_columns
    response = PGroongaBenchmark::Psql.run(<<-SQL, database: @database_name)
SELECT column_name, data_type, udt_name
  FROM information_schema.columns
 WHERE lower(table_catalog) = lower('#{@database_name}') AND
       lower(table_name) = lower('#{@table_name}')
    SQL
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

  def initialize_primary_key_names
    response = PGroongaBenchmark::Psql.run(<<-SQL, database: @database_name)
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
       lower(usage.table_name) = lower('#{@table_name}')
    SQL
    @primary_key_names = response.split
  end

  def initialize_pgroonga_indexes
    @pgroonga_oid =
      PGroongaBenchmark::Psql.run(<<-SQL, type: :integer, database: @database_name)
SELECT oid FROM pg_catalog.pg_am WHERE amname = 'pgroonga';
    SQL

    @groonga_schema =
      PGroongaBenchmark::Psql.run_groonga("schema", database: @database_name)
    pgroonga_indexes =
      PGroongaBenchmark::Psql.run(<<-SQL, database: @database_name)
SELECT indexrelid, relname
  FROM pg_catalog.pg_index
       INNER JOIN
         pg_catalog.pg_class
         ON (indexrelid = oid)
 WHERE relam = #{@pgroonga_oid} AND
       indrelid = #{@table_oid}
    SQL
    @pgroonga_indexes = {}
    pgroonga_indexes.each_line do |line|
      oid, name = line.chomp.split("|")
      oid = Integer(oid, 10)
      groonga_table =
        PGroongaBenchmark::Psql.run("SELECT pgroonga_table_name('#{name}')",
                                    type: :string,
                                    database: @database_name)
      groonga_schema = @groonga_schema["tables"][groonga_table]
      pgroonga_index = PGroongaIndex.new(oid,
                                         name,
                                         groonga_table,
                                         groonga_schema)
      @pgroonga_indexes[name] = pgroonga_index
    end
  end
end
