-- -*- mode: sql; sql-product: postgres -*-

DROP TABLE IF EXISTS pages;
CREATE TABLE pages (
  id serial PRIMARY KEY,
  title text NOT NULL,
  content text
);

CREATE INDEX pages_index
 ON pages
 USING pgroonga (title, content);
