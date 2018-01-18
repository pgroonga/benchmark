#!/usr/bin/bash

while :
do
  sudo -u postgres psql -c "select datname, tup_inserted FROM pg_stat_database;" -d fluentd
  sleep 1s
done
