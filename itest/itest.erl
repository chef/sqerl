%% Copyright 2012 Opscode, Inc. All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%

-module(itest).

-exports([setup_env/0, basic_test_/0,
          statements/1]).

-include_lib("eunit/include/eunit.hrl").
-include("sqerl.hrl").

-record(user, {id, first_name, last_name, high_score, active}).

-define(GET_ARG(Name, Args), proplists:get_value(Name, Args)).
-define(NAMES, [["Kevin", "Smith", 666, <<"2011-10-01 16:47:46">>, true],
                ["Mark", "Anderson", 42, <<"2011-10-02 16:47:46">>, true],
                ["Chris", "Maier", 0, <<"2011-10-03 16:47:46">>, true],
                ["Elvis", "Presley", 16, <<"2011-10-04 16:47:46">>, false]]).
-define(MAX_POOL_COUNT, 3).

-compile([export_all]).

get_db_type() ->
    {ok, [[Type]]} = init:get_argument(db_type),
    list_to_atom(Type).

read_db_config() ->
    Type = get_db_type(),
    Path = filename:join([filename:dirname(code:which(?MODULE)), atom_to_list(Type) ++ ".config"]),
    {ok, Config} = file:consult(Path),
    Config.

setup_env() ->
    Type = get_db_type(),
    Info = read_db_config(),
    ok = application:set_env(sqerl, db_type, Type),
    ok = application:set_env(sqerl, db_host, ?GET_ARG(host, Info)),
    ok = application:set_env(sqerl, db_port, ?GET_ARG(port, Info)),
    ok = application:set_env(sqerl, db_user, "itest"),
    ok = application:set_env(sqerl, db_pass, "itest"),
    ok = application:set_env(sqerl, db_name, ?GET_ARG(db, Info)),
    ok = application:set_env(sqerl, idle_check, 10000),
    %% we could also call it like this:
    %% {prepared_statements, statements(Type)},
    %% {prepared_statements, "itest/statements_pgsql.conf"},
    ok = application:set_env(sqerl, prepared_statements, {?MODULE, statements, [Type]}),
    ColumnTransforms = case Type of
                           pgsql ->
                               [{<<"created">>,
                                 fun sqerl_transformers:convert_YMDHMS_tuple_to_datetime/1}];
                           mysql ->
                               [{<<"active">>,
                                 fun sqerl_transformers:convert_integer_to_boolean/1}]
                       end,
    ok = application:set_env(sqerl, column_transforms, ColumnTransforms),
    PoolConfig = [{name, "sqerl"},
                  {max_count, ?MAX_POOL_COUNT},
                  {init_count, 1},
                  {start_mfa, {sqerl_client, start_link, []}}],
    ok = application:set_env(pooler, pools, [PoolConfig]),
    application:start(crypto),
    application:start(emysql),
    application:start(public_key),
    application:start(ssl),
    application:start(epgsql).

statements(mysql) ->
    {ok, Statements} = file:consult("itest/statements_mysql.conf"),
    Statements;
statements(pgsql) ->
    {ok, Statements} = file:consult("itest/statements_pgsql.conf"),
    Statements.

basic_test_() ->
    setup_env(),
    Status = application:start(sqerl),
    %% sqerl should start or already be running for each test
    ?assert(lists:member(Status, [ok, {error, {already_started, sqerl}}])),
    {foreach,
     fun() -> error_logger:tty(true) end,
     fun(_) -> error_logger:tty(true) end,
     [
      {<<"Connection pool overflow">>,
       fun pool_overflow/0},
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
      {<<"Select boolean">>,
       fun select_boolean/0},

      {<<"Update timestamp type">>,
       fun update_created/0},
      {<<"Select timestamp type">>,
       fun select_created_by_lname/0},
      {<<"Select timestamp type">>,
       fun select_lname_by_created/0},

      {<<"In clause with NULLs">>,
       fun in_clause_with_nulls/0},
      {<<"Adhoc select All">>,
       fun adhoc_select_all/0},
      {<<"Adhoc select equals int">>,
       fun adhoc_select_equals_int/0},
      {<<"Adhoc select equals string">>,
       fun adhoc_select_equals_str/0},
      {<<"Adhoc select equals timestamp">>,
       fun adhoc_select_equals_timestamp/0},
      {<<"Adhoc select equals boolean">>,
       fun adhoc_select_equals_boolean/0},
      {<<"Adhoc select not equals int">>,
       fun adhoc_select_nequals_int/0},
      {<<"Adhoc select not equals str">>,
       fun adhoc_select_nequals_str/0},
      {<<"Adhoc select NOT">>,
       fun adhoc_select_not/0},
      {<<"Adhoc select In (IDs)">>,
       fun adhoc_select_in_ids/0},
      {<<"Adhoc select In (Names)">>,
       fun adhoc_select_in_names/0},
      {<<"Adhoc select In (*)">>,
       fun adhoc_select_in_star/0},
      {<<"Adhoc select AND">>,
       fun adhoc_select_and/0},
      {<<"Adhoc select OR">>,
       fun adhoc_select_or/0},
      {<<"Adhoc select complex">>,
       fun adhoc_select_complex/0},
      
      {<<"Adhoc update">>,
       fun adhoc_update/0},
      
      {<<"Adhoc insert">>,
       fun adhoc_insert/0},
      {<<"Adhoc insert many users">>,
       fun adhoc_insert_many_users/0},
      {<<"Adhoc insert even more users">>,
       fun adhoc_insert_even_more_users/0},

      {<<"Tolerates bounced server">>,
       {timeout, 10,
        fun bounced_server/0}},

      {<<"Delete operation">>,
       fun delete_data/0},
      {"Resultset-returning Stored Procedure",
       fun() ->
               case get_db_type() of
                   mysql ->
                       %% It won't actually return anything; this is just to
                       %% make sure that we're properly handling the insanity
                       %% of MySQL returning multiple results from a stored
                       %% procedure call.
                       %%
                       %% Basically, the fact that it doesn't crash is test
                       %% enough :)
                       {ok, Actual} = sqerl:select(test_the_sp, []),
                       ?assertEqual(none, Actual);
                   Type ->
                       ?debugFmt("Skipping stored procedure test for non-MySQL database ~p~n", [Type])
               end
       end},
      {foreach,
       fun() ->
               %% Don't want to have the error message muddy up the test output
               error_logger:tty(false) end,
       fun(_) -> error_logger:tty(true) end,
       [{"Does NOT handle SPs that return more than one result packet",
         fun() ->
                 case get_db_type() of
                     mysql ->
                         ?assertException(exit,
                                          {{{case_clause, [_Result1,_Result2,_OKPacket]}, _}, _},
                                          sqerl:select(test_the_multi_sp, []));
                     Type ->
                         ?debugFmt("Skipping stored procedure test for non-MySQL database ~p~n", [Type])
                 end
         end}]
      }
     ]}.

kill_pool(1) ->
    pooler:take_member();
kill_pool(X) ->
    pooler:take_member(),
    kill_pool(X - 1).

pool_overflow() ->
    kill_pool(?MAX_POOL_COUNT),
    % Doesn't matter what we do from here; we're just testing operations with
    % a depleted pool
    Expected = {error, no_connections},
    Results = sqerl:select(find_user_by_lname, ["Smith"], ?FIRST(user)),
    ?assertEqual(Expected, Results).

insert_data() ->
    Expected = lists:duplicate(4, {ok, 1}),
    ?assertMatch(Expected, [sqerl:statement(new_user, Name) || Name <- ?NAMES]).

select_data() ->
    {ok, User} = sqerl:select(find_user_by_lname, ["Smith"], first),
    ?assertMatch(<<"Kevin">>, proplists:get_value(<<"first_name">>, User)),
    ?assertMatch(<<"Smith">>, proplists:get_value(<<"last_name">>, User)),
    ?assertEqual(666, proplists:get_value(<<"high_score">>, User)),
    ?assertEqual(true, proplists:get_value(<<"active">>, User)),
    ?assert(is_integer(proplists:get_value(<<"id">>, User))).

select_data_as_record() ->
    {ok, User} = sqerl:select(find_user_by_lname, ["Smith"], ?FIRST(user)),
    ?assertMatch(<<"Kevin">>, User#user.first_name),
    ?assertMatch(<<"Smith">>, User#user.last_name),
    ?assertEqual(666, User#user.high_score),
    ?assertEqual(true, User#user.active),
    ?assert(is_integer(User#user.id)).

select_first_number_zero() ->
    Expected = [{ok, 666}, {ok, 42}, {ok, 0}, {ok, 16} ],
    Returned =  [sqerl:select(find_score_by_lname, [LName], first_as_scalar, [high_score]) ||
                    [_, LName, _, _, _] <- ?NAMES],
    ?assertMatch(Expected, Returned).

delete_data() ->
    Expected = lists:duplicate(4, {ok, 1}),
    ?assertMatch(Expected, [sqerl:statement(delete_user_by_lname, [LName]) ||
                               [_, LName, _, _, _] <- ?NAMES]).

bounced_server() ->
    case get_db_type() of
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

select_boolean() ->
    {ok, User} = sqerl:select(find_user_by_lname, ["Smith"], first),
    ?assertEqual(true, proplists:get_value(<<"active">>, User)),

    {ok, User1} = sqerl:select(find_user_by_lname, ["Presley"], first),
    ?assertEqual(false, proplists:get_value(<<"active">>, User1)).


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

%% @doc Tests padding parameters with null for a "select ... in (?, ?, ...)"
%% prepared statement that expects 10 parameters but we have fewer.
in_clause_with_nulls() ->
    % need to fill in the parameters to 10 since the SQL expects 10 arguments
    % user records were created earlier by insert_data
    Parameters = fill([1,2,3], 10, null),
    {ok, Results} = sqerl:select(in_clause, Parameters),
    ?assertMatch([[{<<"id">>,1}],[{<<"id">>,2}],[{<<"id">>,3}]], Results).

%% @doc Append DefaultValue elements to L so that the 
%% number of elements is N
fill(L, N, DefaultValue) ->
    PadLength = N - length(L),
    case PadLength > 0 of
        true -> L ++ lists:duplicate(PadLength, DefaultValue);
        _ -> L
    end.


adhoc_select_all() ->
    ExpectedNumRows = length(?NAMES),
    {ok, Rows} = sqerl:adhoc_select([<<"*">>], <<"users">>, all),
    ?assertEqual(ExpectedNumRows, length(Rows)).

adhoc_select_equals_int() ->
    ExpectedRows = [[{<<"last_name">>, <<"Smith">>}]],
    {ok, Rows} = sqerl:adhoc_select([<<"last_name">>], <<"users">>,
                                    {<<"id">>, equals, 1}),
    ?assertEqual(ExpectedRows, Rows).

adhoc_select_equals_timestamp() ->
    ExpectedRows = [[{<<"last_name">>, <<"Presley">>}]],
    {ok, Rows} = sqerl:adhoc_select([<<"last_name">>], <<"users">>,
                                    {<<"created">>, equals, {datetime,{{2011,10,4},{16,47,46}}}}),
    ?assertEqual(ExpectedRows, Rows).

adhoc_select_equals_boolean() ->
    ExpectedRows = [[{<<"last_name">>, <<"Presley">>}]],
    {ok, Rows} = sqerl:adhoc_select([<<"last_name">>], <<"users">>,
                                    {<<"active">>, equals, false}),
    ?assertEqual(ExpectedRows, Rows).

adhoc_select_equals_str() ->
    ExpectedRows = [[{<<"id">>, 1}]],
    {ok, Rows} = sqerl:adhoc_select([<<"id">>], <<"users">>,
                                    {<<"last_name">>, equals, <<"Smith">>}),
    ?assertEqual(ExpectedRows, Rows).

adhoc_select_nequals_str() ->
    {ok, Rows} = sqerl:adhoc_select([<<"id">>], <<"users">>,
                                    {<<"last_name">>, nequals, <<"Smith">>}),
    ExpectedRows = [[{<<"id">>,2}],[{<<"id">>,3}],[{<<"id">>,4}]],
    ?assertEqual(lists:sort(ExpectedRows), lists:sort(Rows)).

adhoc_select_nequals_int() ->
    {ok, Rows} = sqerl:adhoc_select([<<"last_name">>], <<"users">>,
                                    {<<"id">>, nequals, 1}),
    ExpectedRows = [
                    [{<<"last_name">>,<<"Presley">>}],
                    [{<<"last_name">>,<<"Anderson">>}],
                    [{<<"last_name">>,<<"Maier">>}]
                   ],
    ?assertEqual(lists:sort(ExpectedRows), lists:sort(Rows)).

adhoc_select_not() ->
    Cols = [<<"id">>],
    Table = <<"users">>,
    Where = {'not', {<<"last_name">>, equals, <<"Smith">>}},
    {ok, Rows} = sqerl:adhoc_select(Cols, Table, Where),
    ExpectedRows = [
                    [{<<"id">>, 2}],
                    [{<<"id">>, 3}],
                    [{<<"id">>, 4}]
                   ],
    ?assertEqual(lists:sort(ExpectedRows), lists:sort(Rows)).

adhoc_select_in_ids() ->
    %% previous test setup has users with id 1 and 2; no users with id 309, 409
    ExpectedRows = [
                    [{<<"last_name">>, <<"Smith">>}],
                    [{<<"last_name">>, <<"Anderson">>}]
                   ],
    Values = [1, 2, 309, 409],
    {ok, Rows} = sqerl:adhoc_select([<<"last_name">>],
                                    <<"users">>,
                                    {<<"id">>, in, Values}),
    ?assertEqual(lists:sort(ExpectedRows), lists:sort(Rows)).

adhoc_select_in_names() ->
    %% previous test setup has users with last names Smith and Anderson but not Toto and Tata
    ExpectedRows = [
                    [{<<"id">>, 1},{<<"first_name">>, <<"Kevin">>}],
                    [{<<"id">>, 2},{<<"first_name">>, <<"Mark">>}]
                   ],
    Values = [<<"Smith">>, <<"Anderson">>, <<"Toto">>, <<"Tata">>],
    {ok, Rows} = sqerl:adhoc_select([<<"id">>, <<"first_name">>],
                                    <<"users">>,
                                    {<<"last_name">>, in, Values}),
    ?assertEqual(lists:sort(ExpectedRows), lists:sort(Rows)).

adhoc_select_in_star() ->
    %% previous test setup has user with last name Smith but not Toto
    ExpectedNumRows = 1,
    ExpectedNumCols = 7,
    Values = [<<"Smith">>, <<"Toto">>],
    {ok, Rows} = sqerl:adhoc_select([<<"*">>],
                                    <<"users">>,
                                    {<<"last_name">>, in, Values}),
    ?assertEqual(ExpectedNumRows, length(Rows)),
    ?assertEqual(ExpectedNumCols, length(lists:nth(1, Rows))).

adhoc_select_and() ->
    Cols = [<<"id">>],
    Table = <<"users">>,
    Where1 = {<<"id">>, gt, 1},
    Where2 = {<<"last_name">>, nequals, <<"Smith">>},
    Where = {'and', [Where1, Where2]},
    {ok, Rows} = sqerl:adhoc_select(Cols, Table, Where),
    ExpectedRows = [
                    [{<<"id">>, 2}],
                    [{<<"id">>, 3}],
                    [{<<"id">>, 4}]
                   ],
    ?assertEqual(lists:sort(ExpectedRows), lists:sort(Rows)).

adhoc_select_or() ->
    Cols = [<<"id">>],
    Table = <<"users">>,
    Where1 = {<<"id">>, gt, 1},
    Where2 = {<<"last_name">>, nequals, <<"Smith">>},
    Where = {'or', [Where1, Where2]},
    {ok, Rows} = sqerl:adhoc_select(Cols, Table, Where),
    ExpectedRows = [
                    [{<<"id">>, 2}],
                    [{<<"id">>, 3}],
                    [{<<"id">>, 4}]
                   ],
    ?assertEqual(lists:sort(ExpectedRows), lists:sort(Rows)).

adhoc_select_complex() ->
    Where = {'and', [{<<"high_score">>, gt, 10},
                     {'or', [{<<"last_name">>, in, [<<"Maier">>, <<"Anderson">>]},
                             {<<"active">>, equals, false}]}
                     ]},
    {ok, Rows} = sqerl:adhoc_select([<<"id">>], <<"users">>, Where),
    ExpectedRows = [
                    [{<<"id">>, 2}],
                    [{<<"id">>, 4}]
                   ],
    ?assertEqual(lists:sort(ExpectedRows), lists:sort(Rows)).

adhoc_update() ->
    RowUpdate = [{<<"last_name">>, <<"MAIER">>}, {<<"first_name">>, <<"Toto">>}],
    Where = {<<"last_name">>, equals, <<"Maier">>},
    {ok, Count} = sqerl:adhoc_update(<<"users">>, RowUpdate, Where),
    ?assertEqual(1, Count),
    {ok, [Row]} = sqerl:adhoc_select([<<"first_name">>], <<"users">>, {<<"last_name">>, equals, <<"MAIER">>}),
    ?assertEqual([{<<"first_name">>, <<"Toto">>}], Row),
    RowUpdate2 = [{<<"last_name">>, <<"Maier">>}, {<<"first_name">>, <<"Chris">>}],
    Where2 = {<<"last_name">>, equals, <<"MAIER">>},
    {ok, Count2} = sqerl:adhoc_update(<<"users">>, RowUpdate2, Where2),
    ?assertEqual(1, Count2).

adhoc_insert() ->
    Table = <<"users">>,
    Columns = [<<"first_name">>,
               <<"last_name">>,
               <<"high_score">>,
               <<"active">>],
    Data = [[<<"Joe1">>, <<"Smith">>, 17, true],
            [<<"Joe2">>, <<"Anderson">>, 23, false]],
    adhoc_insert_delete_test(Table, Columns, Data).

adhoc_insert_delete_test(Table, Columns, Data) ->
    {ok, InsertCount} = sqerl:adhoc_insert(Table, Columns, Data),
    %% verify data was inserted correctly -- relies on first field
    %% values in test being unique.
    Field = lists:nth(1, Columns),
    Values = [Value || [Value|_] <- Data],
    Where = {Field, in, Values},
    {ok, Rows} = sqerl:adhoc_select(Columns, Table, Where),
    {ReturnedColumns, ReturnedData} = sqerl:extract_insert_data(Rows),
    %% clean up before asserts
    {ok, DeleteCount} = sqerl:adhoc_delete(Table, Where),
    %% now verify...
    ?assertEqual(length(Data), InsertCount),
    ?assertEqual(Columns, ReturnedColumns),
    ?assertEqual(Data, ReturnedData),
    ?assertEqual(length(Data), DeleteCount).

adhoc_insert_many_users() ->
    Table = <<"users">>,
    Columns = [<<"first_name">>,
               <<"last_name">>],
    F = fun(I) -> list_to_binary(integer_to_list(I)) end,
    Data = [[F(I), F(I)] || I <- lists:seq(1, 35)],
    adhoc_insert_delete_test(Table, Columns, Data).

adhoc_insert_even_more_users() ->
    Table = <<"users">>,
    Columns = [<<"first_name">>,
               <<"last_name">>],
    F = fun(I) -> list_to_binary(integer_to_list(I)) end,
    Data = [[F(I), F(I)] || I <- lists:seq(1000, 2004)],
    adhoc_insert_delete_test(Table, Columns, Data).
