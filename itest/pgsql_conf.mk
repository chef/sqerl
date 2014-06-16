#
# config vars for make
#
# using the postgres database keeps us from having errors about droping current database
PG_USER ?= $(USER)
DB_CMD = psql -q -d postgres -h localhost -p 5432 -U $(PG_USER) -f -
