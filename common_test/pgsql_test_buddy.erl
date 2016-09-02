-module(pgsql_test_buddy).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-compile([export_all]).

-define(USER, string:strip(os:cmd("echo $USER"), right, $\n)).
% NOTE - do not merge.  Doing this because kitchen isn't syncing up my local changes
% and spending my time debugging secondary issues (or completely cycling the kitchen boxes
% for each test) isn't something I'm inclined to do at the moment..
%-define(DB_PIPE_CMD, "psql -q -d postgres -h localhost -p 5432 -U " ++ ?USER ++ " -f - < ~s").
%-define(DB_CMD, "psql -h localhost -p 5432 -U " ++ ?USER ++ " -d postgres -w -c '~s'").
-define(DB_PIPE_CMD, "sudo -u postgres psql -q -d postgres -f - < ~s").
-define(DB_CMD, "psql -d postgres -w -c '~s'").
% END NOTE


-define(POOL_NAME, sqerl).
-define(POOLER_TIMEOUT, 500).
-define(MAX_POOL_COUNT, 3).

-define(GET_ARG(Name, Args), proplists:get_value(Name, Args)).

clean() ->
    [ os:cmd(io_lib:format(?DB_CMD, [Cmd])) || Cmd <- [
        "drop database if exists itest_sqerl1",
        "drop database if exists itest_sqerl2",
        "drop user if exists itest_sqerl1",
        "drop user if exists itest_sqerl2"
    ]].

config_file(Config, File) ->
    Dir = lists:reverse(filename:split(?config(data_dir, Config))),
    {_, Good} = lists:split(1, Dir),
    filename:join(lists:reverse(Good) ++ [File]).

create(Config) ->
    File1 = config_file(Config, "pgsql_create_itest2.sql"),
    ct:pal("SQL File 1/2: ~s", [File1]),
    os:cmd(io_lib:format(?DB_PIPE_CMD, [File1])),
    File2 = config_file(Config, "pgsql_create.sql"),
    ct:pal("SQL File 222: ~s", [File2]),
    os:cmd(io_lib:format(?DB_PIPE_CMD, [File2])).

setup_env() ->
    application:stop(sasl),
    % By default we're going to set up multi-pool - this gives defacto verification
    % that nothing gets broken in existing code (or tests) when the pool name is not
    % specified.
    EnvCfg = config([{sqerl, "itest_sqerl1"},
                     {poo12, "itest_sqerl2"}]),
    set_env_for(sqerl, ?config(sqerl, EnvCfg)),
    set_env_for(pooler, ?config(pooler, EnvCfg)),
    ct:pal("Environment configuration: ~p", [[{sqerl, application:get_all_env(sqerl)},
                                              {pooler, application:get_all_env(pooler)}]]),

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


config(DBInfo) ->
    [{sqerl, [
              {ip_mode, [ ipv4 ] },
              {db_driver_mod, sqerl_pgsql_client},
              {pooler_timeout, ?POOLER_TIMEOUT},
              {databases, [ sqerl_db_config(Id, Name) || {Id, Name} <- DBInfo ]}
             ]},
     {pooler, [
               {pools, [ pool_config(Id) || {Id, _} <- DBInfo ]}
               %{metrics_module, folsom_metrics}
              ]
    }].

sqerl_db_config(Id, TheName) ->
    {Id,  [{db_host, "127.0.0.1"},
           {db_port, 5432 },
           {db_user, TheName},
           {db_pass, TheName},
           {db_name, TheName},
           {db_timeout, 5000},
           {idle_check, 1000},
           {column_transforms,  [{<<"created">>,
                                  fun sqerl_transformers:convert_YMDHMS_tuple_to_datetime/1}]},
           {prepared_statements,  {obj_user, '#statements', []}} ]
    }.

pool_config(Id) ->
   [{name, Id},
    {max_count, ?MAX_POOL_COUNT},
    {init_count, 1},
    {start_mfa, {sqerl_client, start_link, [{pool, Id}]}},
    {queue_max, 5}].

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

