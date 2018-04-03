class PGroongaIndex
  attr_reader :oid
  attr_reader :name
  attr_reader :groonga_table_name
  attr_reader :groonga_table_schema
  def initialize(oid, name, groonga_table_name, groonga_table_schema)
    @oid = oid
    @name = name
    @groonga_table_name = groonga_table_name
    @groonga_table_schema = groonga_table_schema
  end
end
