class Column
  attr_reader :name
  attr_reader :type
  def initialize(name, type)
    @name = name
    @type = type
  end

  def array?
    @type.end_with?("[]")
  end

  def escape_value(value)
    case type
    when "varchar", "bpchar", "date", "text", "text[]", "int4[]"
      quote_escaped_value = value.gsub(/'/, "''")
      "'#{quote_escaped_value}'"
    else
      value
    end
  end
end
