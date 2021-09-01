require_relative "psql"

class SynonymPreparer
  def initialize(database_name)
    @database_name = database_name
  end

  def prepare
    prepare_schema
    prepare_records
    prepare_index
  end

  private
  def prepare_schema
    Psql.open(@database_name) do |psql|
      psql.execute(<<-SQL)
DROP TABLE IF EXISTS system_thesaurus;
      SQL
      psql.execute(<<-SQL)
DROP TABLE IF EXISTS user_thesaurus;
      SQL
      psql.execute(<<-SQL)
CREATE TABLE system_thesaurus (
  term text PRIMARY KEY,
  synonyms text[]
);
      SQL
      psql.execute(<<-SQL)
CREATE TABLE user_thesaurus (
  synonyms text[]
);
      SQL
      psql.finish
    end
  end

  def prepare_records
    Psql.open(@database_name) do |psql|
      IO.popen(["groonga-synonym-generate",
                "--format", "pgroonga",
                "--table", "system_thesaurus"]) do |input|
        psql.execute(input)
      end
      psql.finish
    end
  end

  def prepare_index
    Psql.open(@database_name) do |psql|
      psql.execute(<<-SQL)
CREATE INDEX system_thesaurus_term_index
  ON system_thesaurus
  USING pgroonga (term pgroonga_text_term_search_ops_v2);
      SQL
      psql.execute(<<-SQL)
CREATE INDEX user_thesaurus_synonyms_index
  ON user_thesaurus
  USING pgroonga (synonyms pgroonga_text_array_term_search_ops_v2);
      SQL
      psql.finish
    end
  end
end
