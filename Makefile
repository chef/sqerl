DEPS = deps/emysql deps/meck deps/automeck deps/gen_bunny \
       deps/poolboy deps/epgsql

DB_HOST = "localhost"
DB_PORT = 3306
DB_NAME = itest
DB_TYPE = mysql

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
	@mysql -u root --host=${DB_HOST} --protocol=TCP < itest/itest_create.sql

itest_clean:
	@mysql -u root --host=${DB_HOST} --protocol=TCP < itest/itest_clean.sql

itest: compile itest_create
	@cd itest;erlc *.erl
	@erl -pa deps/*/ebin -pa ebin -pa itest -noshell -eval "eunit:test(itest, [verbose])" \
	-s erlang halt -host ${DB_HOST} -port ${DB_PORT} -db ${DB_NAME} -db_type ${DB_TYPE}
	@make itest_clean
	@rm -f itest/*.beam
