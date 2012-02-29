-module(itest).

-exports([setup_env/0, basic_test_/0]).

-include_lib("eunit/include/eunit.hrl").
-include("sqerl.hrl").

-record(user, {id, first_name, last_name, high_score}).

-define(ARGS, [host, port, db, db_type]).
-define(GET_ARG(Name, Args), proplists:get_value(Name, Args)).
-define(NAMES, [["Kevin", "Smith", 666, <<"2011-10-01 16:47:46">>],
                ["Mark", "Anderson", 42, <<"2011-10-02 16:47:46">>],
                ["Chris", "Maier", 0, <<"2011-10-03 16:47:46">>],
                ["Elvis", "Presley", 16, <<"2011-10-04 16:47:46">>]]).

-compile([export_all]).

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
    ok = application:set_env(sqerl, host, ?GET_ARG(host, Info)),
    ok = application:set_env(sqerl, port, ?GET_ARG(port, Info)),
    ok = application:set_env(sqerl, user, "itest"),
    ok = application:set_env(sqerl, pass, "itest"),
    ok = application:set_env(sqerl, db, ?GET_ARG(db, Info)),
    ok = application:set_env(sqerl, max_count, 3),
    ok = application:set_env(sqerl, init_count, 1),
    ok = application:set_env(sqerl, db_type, Type),
    case Type of
        pgsql ->
            ok = application:set_env(sqerl, prepared_statement_source, "itest/statements_pgsql.conf"),
            ok = application:set_env(sqerl, column_transforms,
                                     [{<<"created">>,
                                       fun sqerl_transformers:convert_YMDHMS_tuple_to_datetime/1}]);
        mysql ->
            ok = application:set_env(sqerl, prepared_statement_source, "itest/statements_mysql.conf"),
            ok = application:set_env(sqerl, column_transforms, [{}])
    end,

    application:start(crypto),
    application:start(emysql),
    application:start(public_key),
    application:start(ssl),
    application:start(epgsql),
    application:start(pooler).

basic_test_() ->
    setup_env(),
    Status = application:start(sqerl),
    %% sqerl should start or already be running for each test
    ?assert(lists:member(Status, [ok, {error, {already_started, sqerl}}])),
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
       fun select_created_by_lname/0},
      {<<"Select timestamp type">>,
       fun select_lname_by_created/0},

      {<<"Tolerates bounced server">>,
       {timeout, 10,
        fun bounced_server/0}},

      {<<"Delete operation">>,
       fun delete_data/0}
     ]}.

insert_data() ->
    Expected = lists:duplicate(4, {ok, 1}),
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
    Expected = [{ok, 666}, {ok, 42}, {ok, 0}, {ok, 16} ],
    Returned =  [sqerl:select(find_score_by_lname, [LName], first_as_scalar, [high_score]) ||
                    [_, LName, _, _] <- ?NAMES],
    ?assertMatch(Expected, Returned).

delete_data() ->
    Expected = lists:duplicate(4, {ok, 1}),
    ?assertMatch(Expected, [sqerl:statement(delete_user_by_lname, [LName]) ||
                               [_, LName, _, _] <- ?NAMES]).

bounced_server() ->
    Info = get_dbinfo(),
    case proplists:get_value(db_type, Info) of
        mysql ->
            os:cmd("mysql.server stop"),
            os:cmd("mysql.server start"),
            {ok, Result} = sqerl:select(find_user_by_lname, ["Smith"], first),
            ?assertMatch(<<"Smith">>, proplists:get_value(<<"last_name">>, Result));
        Type ->
            ?debugFmt("Skipping bounced server test for ~p~n", [Type])
    end.

update_datablob() ->
    ?assertMatch({ok, 1},
		 sqerl:statement(update_datablob_by_lname,
				 [<<"foobar">>, "Smith"] )).

select_datablob() ->
    {ok, User} = sqerl:select(find_datablob_by_lname, ["Smith"], first_as_scalar, [datablob]),
    ?assertMatch(<<"foobar">>, User).


%%%
%%% Tests for timestamp behavior....
%%%
update_created() ->
    ?assertMatch({ok, 1},
		 sqerl:statement(update_created_by_lname,
				 [{datetime, {{2011, 11, 1}, {16, 47, 46}}},
				  "Smith"])),
    ?assertMatch({ok, 1},
		 sqerl:statement(update_created_by_lname,
				 [{{2011, 11, 2}, {16, 47, 46}}, "Anderson"])),
    ?assertMatch({ok, 1},
		 sqerl:statement(update_created_by_lname,
				 [<<"2011-11-03 16:47:46">>, "Maier"])),

    {ok, User1} = sqerl:select(find_created_by_lname, ["Smith"], first_as_scalar, [created]),
    ?assertMatch({datetime, {{2011, 11, 01}, {16, 47, 46}}}, User1),
    {ok, User2} = sqerl:select(find_created_by_lname, ["Anderson"], first_as_scalar, [created]),
    ?assertMatch({datetime, {{2011, 11, 02}, {16, 47, 46}}}, User2),
    {ok, User3} = sqerl:select(find_created_by_lname, ["Maier"], first_as_scalar, [created]),
    ?assertMatch({datetime, {{2011, 11, 03}, {16, 47, 46}}}, User3).

select_created_by_lname() ->
    {ok, User1} = sqerl:select(find_created_by_lname, ["Presley"], first_as_scalar, [created]),
    ?assertMatch({datetime, {{2011, 10, 04}, {16, 47, 46}}}, User1).
select_lname_by_created() ->
    {ok, User1} = sqerl:select(find_lname_by_created, [{datetime, {{2011, 10, 04}, {16, 47, 46}}}], first_as_scalar, [last_name]),
    ?assertMatch(<<"Presley">>, User1).
