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
