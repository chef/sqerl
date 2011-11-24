-module(itest).

-exports([setup_env/0, basic_test_/0]).

-include_lib("eunit/include/eunit.hrl").
-include("sqerl.hrl").

-record(user, {id, first_name, last_name, high_score}).

-define(ARGS, [host, port, db, db_type]).
-define(GET_ARG(Name, Args), proplists:get_value(Name, Args)).
-define(NAMES, [["Kevin", "Smith", 666],
                ["Mark", "Anderson", 42],
                ["Chris", "Maier", 0]]).

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
    case Type of
        pgsql ->
            ok = application:set_env(sqerl, db_prepared_statements, "itest/statements_pgsql.conf"),
	    ok = application:set_env(sqerl, db_column_transforms,
				     [{<<"created">>, fun sqerl_transformers:convert_YMDHMS_tuple_to_datetime/1}]);
	
        mysql ->
            ok = application:set_env(sqerl, db_prepared_statements, "itest/statements_mysql.conf")
    end,

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
      {<<"Ensure a select that returns the number zero doesn't come back as 'none'">>,
       fun select_first_number_zero/0},
      {<<"Update blob type">>, 
       fun update_datablob/0},
      {<<"Select blob type">>, 
       fun select_datablob/0},

      {<<"Update timestamp type">>, 
       fun update_created/0},
      {<<"Select timestamp type">>, 
       fun select_created/0},

      {<<"Delete operation">>, 
       fun delete_data/0}
     ]}.

insert_data() ->
    Expected = lists:duplicate(3, {ok, 1}),
    ?assertMatch(Expected, [sqerl:statement(new_user, Name) || Name <- ?NAMES]).

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

select_first_number_zero() ->
    Expected = [{ok, 666}, {ok, 42}, {ok, 0}],
    ?assertMatch(Expected, [sqerl:select(find_score_by_lname, [LName], first_as_scalar, [high_score]) ||
                               [_, LName, _] <- ?NAMES]).

delete_data() ->
    Expected = lists:duplicate(3, {ok, 1}),
    ?assertMatch(Expected, [sqerl:statement(delete_user_by_lname, [LName]) ||
                               [_, LName, _] <- ?NAMES]).

update_datablob() ->
    ?assertMatch({ok, 1}, 
		 sqerl:statement(update_datablob_by_lname, 
				 [<<"foobar">>, "Smith"] )).

select_datablob() ->
    {ok, User} = sqerl:select(find_datablob_by_lname, ["Smith"], first_as_scalar, [datablob]),
    ?assertMatch(<<"foobar">>, User).

update_created() ->
    ?assertMatch({ok, 1},
		 sqerl:statement(update_created_by_lname,
				 [{datetime, {{2011, 11, 1}, {16, 47, 46}}},
				  "Smith"])).

select_created() ->
    {ok, User} = sqerl:select(find_created_by_lname, ["Smith"], first_as_scalar, [created]),
    ?assertMatch({datetime, {{2011, 11, 01}, {16, 47, 46}}}, User).
