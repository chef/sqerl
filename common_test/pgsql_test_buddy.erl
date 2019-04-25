-module(pgsql_test_buddy).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-compile([export_all]).

-define(USER, string:strip(os:cmd("echo $USER"), right, $\n)).
-define(DB_PIPE_CMD, "psql -q -d postgres -h localhost -p 5432 -U " ++ ?USER ++ " -f - < ~s").
-define(DB_CMD, "psql -h localhost -p 5432 -U " ++ ?USER ++ " -d postgres -w -c '~s'").

-define(POOL_NAME, sqerl).
-define(POOLER_TIMEOUT, 500).
-define(MAX_POOL_COUNT, 3).

-define(GET_ARG(Name, Args), proplists:get_value(Name, Args)).

clean() ->
    [ os:cmd(io_lib:format(?DB_CMD, [Cmd])) || Cmd <- [
        "drop database if exists itest",
        "drop user if exists itest"
    ]].

create(Config) ->
    Dir = lists:reverse(filename:split(?config(data_dir, Config))),
    {_, Good} = lists:split(1, Dir),
    File = filename:join(lists:reverse(Good) ++ ["pgsql_create.sql"]),
    ct:pal("File: ~s", [File]),
    os:cmd(io_lib:format(?DB_PIPE_CMD, [File])).

setup_env() ->
    application:stop(sasl),
    Info = db_config(),
    ok = application:set_env(sqerl, db_driver_mod, sqerl_pgsql_client),
    ok = application:set_env(sqerl, db_host, ?GET_ARG(host, Info)),
    ok = application:set_env(sqerl, db_port, ?GET_ARG(port, Info)),
    ok = application:set_env(sqerl, db_user, "itest"),
    ok = application:set_env(sqerl, db_pass, "itest"),
    ok = application:set_env(sqerl, db_name, ?GET_ARG(db, Info)),
    ok = application:set_env(sqerl, idle_check, 10000),
    ok = application:set_env(sqerl, pooler_timeout, ?POOLER_TIMEOUT),
    %% we could also call it like this:
    %% {prepared_statements, statements()},
    %% {prepared_statements, "itest/statements_pgsql.conf"},
    ok = application:set_env(sqerl, prepared_statements, {obj_user, '#statements', []}),
    ColumnTransforms = [{<<"created">>,
                         fun sqerl_transformers:convert_YMDHMS_tuple_to_datetime/1}],
    ok = application:set_env(sqerl, column_transforms, ColumnTransforms),
    PoolConfig = [{name, ?POOL_NAME},
                  {max_count, ?MAX_POOL_COUNT},
                  {init_count, 1},
                  {start_mfa, {sqerl_client, start_link, []}}],
    ok = application:set_env(pooler, pools, [PoolConfig]),
    Apps = [crypto, asn1, public_key, ssl, epgsql, pooler],
    [ application:start(A) || A <- Apps ],

    Status = application:start(sqerl),
    %% sqerl should start or already be running for each test
    ?assert(lists:member(Status, [ok, {error, {already_started, sqerl}}])),
    ok.

teardown_env() ->
    Apps = lists:reverse([crypto, asn1, public_key, ssl, epgsql, pooler, sqerl]),
    [application:stop(A) || A <- Apps].

db_config() ->
    [
        {host, "localhost"},
        {port, 5432},
        {db, "itest"}
    ].

kill_pool() -> kill_pool(?MAX_POOL_COUNT).
kill_pool(1) ->
    pooler:take_member(?POOL_NAME, ?POOLER_TIMEOUT);
kill_pool(X) ->
    pooler:take_member(?POOL_NAME, ?POOLER_TIMEOUT),
    kill_pool(X - 1).
