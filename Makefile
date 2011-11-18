DEPS = deps/emysql deps/meck deps/automeck deps/gen_bunny \
       deps/poolboy deps/epgsql

#DB_HOST = "localhost"
#DB_PORT = 3306
#DB_NAME = itest
#DB_TYPE = mysql
#DB_CMD = mysql -u root --host=${DB_HOST} --protocol=TCP

DB_HOST = "localhost"
DB_PORT = 5432
DB_NAME = itest
DB_TYPE = pgsql
# using the postges database keeps us from having errors about droping current database
DB_CMD = psql -d postgres -h ${DB_HOST} -p ${DB_PORT} -f -

all: compile eunit

clean:
	@rebar skip_deps=true clean

allclean:
	@rebar clean

distclean:
	@rebar skip_deps=true clean
	@rm -rf deps

compile: $(DEPS)
	@rebar compile
	@dialyzer -Wrace_conditions -Wunderspecs -r ebin

dialyzer:
	@dialyzer -Wrace_conditions -Wunderspecs -r ebin

$(DEPS):
	@rebar get-deps

eunit: compile
	@rebar skip_deps=true eunit

test: eunit

itest_create:
	${DB_CMD} < itest/itest_${DB_TYPE}_create.sql

itest_clean:
	${DB_CMD} < itest/itest_${DB_TYPE}_clean.sql

itest: compile itest_create
	@cd itest;erlc *.erl
	@erl -pa deps/*/ebin -pa ebin -pa itest -noshell -eval "eunit:test(itest, [verbose])" \
	-s erlang halt -host ${DB_HOST} -port ${DB_PORT} -db ${DB_NAME} -db_type ${DB_TYPE}
	@make itest_clean
	@rm -f itest/*.beam
