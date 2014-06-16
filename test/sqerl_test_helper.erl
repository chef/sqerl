-module(sqerl_test_helper).

-export([
         run_cmd/1,
         setup_db/0,
         setup_db_app/0
        ]).

-define(TEST_DB_NAME, "sqerl_rec_test_db").
-define(TEST_DB_SCHEMA, "../test/sqerl_rec_test_db_schema.sql").
-define(DB_HOST, "localhost").
-define(DB_PORT, 5432).
%% this one doesn't matter since we rely on local user authN w/ pg
-define(DB_PASS, "sesame").

setup_db() ->
    setup_db_i({sqerl_rec, statements, [[kitchen, cook]]}).

setup_db_app() ->
    setup_db_i({sqerl_rec, statements, [[kitchen, cook, {app, sqerl}]]}).

setup_db_i(PreparedStatements) ->
    User = get_user(),

    handle_cmd_result(drop_db(User), [0, 1]),
    handle_cmd_result(create_db(User), [0]),
    handle_cmd_result(create_tables(User), [0]),

    SqerlEnv = [{db_host, ?DB_HOST},
                {db_port, ?DB_PORT},
                {db_user, User},
                {db_pass, ?DB_PASS},
                {db_name, ?TEST_DB_NAME},
                {idle_check, 10000},
                {prepared_statements, PreparedStatements},
                {column_transforms, []}],
    [ ok = application:set_env(sqerl, Key, Val) || {Key, Val} <- SqerlEnv ],

    io:format("Sqerl env: ~p~n", [SqerlEnv]),

    PoolConfig = [{name, sqerl},
                  {max_count, 3},
                  {init_count, 1},
                  {start_mfa, {sqerl_client, start_link, []}}],
    ok = application:set_env(pooler, pools, [PoolConfig]),
    Apps = [crypto, asn1, public_key, ssl, pooler, epgsql, sqerl],
    [ application:start(A) || A <- Apps ].

get_user() ->
    case os:getenv("PG_USER") of
        false -> os:getenv("USER");
        User -> User
    end.

create_db(User) ->
    Cmd = ["createdb -T template1 -E utf8 -U ", User, ?TEST_DB_NAME],
    run_cmd(Cmd).

drop_db(User) ->
    Cmd = ["dropdb -U ", User, ?TEST_DB_NAME],
    run_cmd(Cmd).

create_tables(User) ->
    Cmd = ["psql -U", User, "--set ON_ERROR_STOP=1", ?TEST_DB_NAME, "-f", ?TEST_DB_SCHEMA],
    run_cmd(Cmd).

-spec run_cmd([string()]) -> {Status :: integer(),
                              StdOutPlusStdErr :: binary()}.
run_cmd(CmdList) ->
    Cmd = string:join(CmdList, " "),
    Port = erlang:open_port({spawn, Cmd}, [{line, 256}, exit_status,
                                           stderr_to_stdout]),
    gather_data(Port, 10000, []).

gather_data(Port, Timeout, Acc) ->
    receive
        {Port, {exit_status, Status}} ->
            {Status, erlang:iolist_to_binary(lists:reverse(Acc))};
        {Port, {data, {eol, Line}}} ->
            gather_data(Port, Timeout, ["\n", Line | Acc])
    after Timeout ->
            keygen_timeout
    end.

handle_cmd_result({Status, Output}, Allowed) ->
    case lists:member(Status, Allowed) of
        true ->
            ok;
        false ->
            erlang:error({run_cmd,
                          {allowed_status, Allowed},
                          {status, Status},
                          {output, Output}})
    end.
