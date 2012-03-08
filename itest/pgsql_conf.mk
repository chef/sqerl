#
# config vars for make
#
# using the postges database keeps us from having errors about droping current database
DB_CMD = psql -q -d postgres -h localhost -p 5432 -f -
