#
# config vars for make
#
DB_HOST = "localhost"
DB_PORT = 5432
DB_NAME = itest
DB_TYPE = pgsql
# using the postges database keeps us from having errors about droping current database
DB_CMD = psql -q -d postgres -h ${DB_HOST} -p ${DB_PORT} -f -
