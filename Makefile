DEPS = deps/emysql deps/meck deps/automeck \
       deps/pooler deps/epgsql
REBAR = ./rebar

## Set the environment variable $DB_TYPE to either mysql or pgsql
## to run the correct integration tests.
-include itest/$(DB_TYPE)_conf.mk

all: compile eunit

clean:
	@$(REBAR) skip_deps=true clean

allclean:
	@$(REBAR) clean

distclean:
	@$(REBAR) skip_deps=true clean
	@rm -rf deps

compile: $(DEPS)
	@$(REBAR) compile
	@dialyzer -Wrace_conditions -Wunderspecs -r ebin

dialyzer:
	@dialyzer -Wrace_conditions -Wunderspecs -r ebin

$(DEPS):
	@$(REBAR) get-deps

eunit: compile
	@$(REBAR) skip_deps=true eunit

test: eunit

itest_create:
	@echo Creating integration test database
	@${DB_CMD} < itest/itest_${DB_TYPE}_create.sql

itest_clean:
	@rm -f itest/*.beam
	@echo Dropping integration test database
	@${DB_CMD} < itest/itest_${DB_TYPE}_clean.sql

itest: compile itest_create itest_run itest_clean

itest_run:
	cd itest;erlc -I ../include *.erl
	@erl -pa deps/*/ebin -pa ebin -pa itest -noshell -eval "eunit:test(itest, [verbose])" \
	-s erlang halt -host ${DB_HOST} -port ${DB_PORT} -db ${DB_NAME} -db_type $(DB_TYPE)

