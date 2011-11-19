-module(itest).

-exports([setup_env/0, basic_test_/0]).

-include_lib("eunit/include/eunit.hrl").
-include("sqerl.hrl").

-record(user, {id, first_name, last_name}).

-define(ARGS, [host, port, db, db_type]).
-define(GET_ARG(Name, Args), proplists:get_value(Name, Args)).
-define(NAMES, [["Kevin", "Smith"],
                ["Mark", "Anderson"],
                ["Chris", "Maier"]]).

get_dbinfo() ->
    F = fun(port) ->
                {ok, [[Port]]} = init:get_argument(port),
                {port, list_to_integer(Port)};
           (db_type) ->
                {ok, [[Type]]} = init:get_argument(db_type),
                {db_type, list_to_atom(Type)};
           (Name) ->
                {ok, [[Value]]} = init:get_argument(Name),
                {Name, Value} end,
    [F(Arg) || Arg <- ?ARGS].

setup_env() ->
    Info = get_dbinfo(),
    Type = ?GET_ARG(db_type, Info),
    ok = application:set_env(sqerl, db_host, ?GET_ARG(host, Info)),
    ok = application:set_env(sqerl, db_port, ?GET_ARG(port, Info)),
    ok = application:set_env(sqerl, db_user, "itest"),
    ok = application:set_env(sqerl, db_pass, "itest"),
    ok = application:set_env(sqerl, db_name, ?GET_ARG(db, Info)),
    ok = application:set_env(sqerl, db_pool_size, 3),
    ok = application:set_env(sqerl, db_type, Type),
    ok = application:set_env(sqerl, db_prepared_statements, "itest/statements_pgsql.conf"),
    application:start(crypto),
    application:start(emysql),
    application:start(public_key),
    application:start(ssl),
    application:start(epgsql).   

basic_test_() ->
    setup_env(),
    application:start(sqerl),
    {foreach,
     fun() -> error_logger:tty(false) end,
     fun(_) -> error_logger:tty(true) end,
     [
      {<<"Insert operations">>,
       fun insert_data/0},
      {<<"Select operations">>,
       fun select_data/0},
      {<<"Select w/record xform operations">>,
       fun select_data_as_record/0},
      {<<"Delete operation">>,
       fun delete_data/0}
     ]}.

insert_data() ->
    ?assertMatch([1,1,1], [sqerl:statement(new_user, Name) || Name <- ?NAMES]).

select_data() ->
    {ok, User} = sqerl:select(find_user_by_lname, ["Smith"], first),
    ?assertMatch(<<"Kevin">>, proplists:get_value(<<"first_name">>, User)),
    ?assertMatch(<<"Smith">>, proplists:get_value(<<"last_name">>, User)),
    ?assert(is_integer(proplists:get_value(<<"id">>, User))).

select_data_as_record() ->
    {ok, User} = sqerl:select(find_user_by_lname, ["Anderson"], ?FIRST(user)),
    ?assertMatch(<<"Mark">>, User#user.first_name),
    ?assertMatch(<<"Anderson">>, User#user.last_name),
    ?assert(is_integer(User#user.id)).

delete_data() ->
    ?assertMatch([1,1,1], [sqerl:statement(delete_user_by_lname, [LName]) ||
                          [_, LName] <- ?NAMES]).
