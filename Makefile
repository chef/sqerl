# This Makefile written by concrete
#
# {concrete_makefile_version, 1}
#
# Use this to override concrete's default dialyzer options of
# -Wunderspecs
# DIALYZER_OPTS = ...

# List dependencies that you do NOT want to be included in the
# dialyzer PLT for the project here.  Typically, you would list a
# dependency here if it isn't spec'd well and doesn't play nice with
# dialyzer or otherwise mucks things up.
#
# DIALYZER_SKIP_DEPS = bad_dep_1 \
#                      bad_dep_2

# If you want to add dependencies to the default "all" target provided
# by concrete, add them here (along with make rules to build them if needed)
# ALL_HOOK = ...

concrete_rules_file = $(wildcard concrete.mk)
ifeq ($(concrete_rules_file),concrete.mk)
    include concrete.mk
else
    all:
	@echo "ERROR: missing concrete.mk"
	@echo "  run: concrete update"
endif

DB_TYPE ?= pgsql
-include itest/$(DB_TYPE)_conf.mk

itest_create:
	@echo Creating integration test database
	@${DB_CMD} < itest/itest_${DB_TYPE}_create.sql

itest_clean:
	@rm -f itest/*.beam
	@echo Dropping integration test database
	@${DB_CMD} < itest/itest_${DB_TYPE}_clean.sql

itest_module_%:
	# sadly enough, make macros are not expanded in target names
	$(MAKE) compile	itest_create itest_run_module_$* itest_clean

itest_run_module_%:
	cd itest && erlc -I ../include *.erl
	erl -pa deps/*/ebin -pa ebin -pa itest -noshell \
		-eval "ok = eunit:test($*, [verbose])" \
		-s erlang halt -db_type $(DB_TYPE)

itest: itest_module_itest

perftest: itest_module_perftest

.PHONY: itest itest_clean itest_create itest_run perftest
