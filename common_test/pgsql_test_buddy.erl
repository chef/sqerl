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
    set_env_for(sqerl, sqerl_config()),
    set_env_for(pooler, pooler_config()),
    Apps = [crypto, asn1, public_key, ssl, epgsql, pooler],
    [ application:start(A) || A <- Apps ],

    Status = application:start(sqerl),
    %% sqerl should start or already be running for each test
    ?assert(lists:member(Status, [ok, {error, {already_started, sqerl}}])),
    ok.

set_env_for(Key, EnvEntries) ->
    [ application:set_env(Key, Entry, Value) || {Entry, Value} <- EnvEntries ].

teardown_env() ->
    Apps = lists:reverse([crypto, asn1, public_key, ssl, epgsql, pooler, sqerl]),
    [application:stop(A) || A <- Apps].


sqerl_config() ->
    [{db_driver_mod, sqerl_pgsql_client},
     {ip_mode, [ipv4]},
     {db_host, "127.0.0.1" },
     {db_port, 5432 },
     {db_user, "itest" },
     {db_pass, "itest" },
     {db_name, "itest" },
     {idle_check, 1000},
     {column_transforms,[{<<"created">>,
                          fun sqerl_transformers:convert_YMDHMS_tuple_to_datetime/1}]},
     {pooler_timeout, ?POOLER_TIMEOUT},
     {prepared_statements,  {obj_user, '#statements', []}}
    ].
pooler_config() ->
    [{pools,
     [[{name, sqerl},
       {max_count,  ?MAX_POOL_COUNT},
       {init_count, 1},
       {start_mfa, {sqerl_client, start_link, []}}
      ]
     ]
    }].

kill_pool() -> kill_pool(?MAX_POOL_COUNT).
kill_pool(1) ->
    pooler:take_member(?POOL_NAME, ?POOLER_TIMEOUT);
kill_pool(X) ->
    pooler:take_member(?POOL_NAME, ?POOLER_TIMEOUT),
    kill_pool(X - 1).

get_user() ->
    case os:getenv("PG_USER") of
        false -> os:getenv("USER");
        User -> User
    end.

