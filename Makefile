DEPS = deps/emysql deps/meck deps/automeck deps/gen_bunny \
       deps/poolboy deps/epgsql

DB_TYPE = mysql
#DB_TYPE = pgsql

include ${DB_TYPE}_conf.mk

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
	@rm -f itest/*.beam
	${DB_CMD} < itest/itest_${DB_TYPE}_clean.sql

itest: itest_create itest_run itest_clean

itest_run:
	cd itest;erlc -I ../include *.erl
	@erl -pa deps/*/ebin -pa ebin -pa itest -noshell -eval "eunit:test(itest, [verbose])" \
	-s erlang halt -host ${DB_HOST} -port ${DB_PORT} -db ${DB_NAME} -db_type ${DB_TYPE}

