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

-exports([setup_env/0, basic_test_/0, array_test_/0,
          statements/1]).

-include_lib("eunit/include/eunit.hrl").
-include("sqerl.hrl").

-record(user, {id, first_name, last_name, high_score, active}).

-define(GET_ARG(Name, Args), proplists:get_value(Name, Args)).
-define(NAMES, [["Kevin", "Smith", 666, <<"2011-10-01 16:47:46">>, true],
                ["Mark", "Anderson", 42, <<"2011-10-02 16:47:46">>, true],
                ["Chris", "Maier", 0, <<"2011-10-03 16:47:46">>, true],
                ["Elvis", "Presley", 16, <<"2011-10-04 16:47:46">>, false]]).
-define(POOL_NAME, sqerl).
-define(MAX_POOL_COUNT, 3).

-compile([export_all]).

read_db_config() ->
    Path = filename:join([filename:dirname(code:which(?MODULE)), "pgsql" ++ ".config"]),
    {ok, Config} = file:consult(Path),
    Config.

setup_env() ->
    Info = read_db_config(),
    ok = application:set_env(sqerl, db_driver_mod, sqerl_pgsql_client),
    ok = application:set_env(sqerl, db_host, ?GET_ARG(host, Info)),
    ok = application:set_env(sqerl, db_port, ?GET_ARG(port, Info)),
    ok = application:set_env(sqerl, db_user, "itest"),
    ok = application:set_env(sqerl, db_pass, "itest"),
    ok = application:set_env(sqerl, db_name, ?GET_ARG(db, Info)),
    ok = application:set_env(sqerl, idle_check, 10000),
    %% we could also call it like this:
    %% {prepared_statements, statements()},
    %% {prepared_statements, "itest/statements_pgsql.conf"},
    ok = application:set_env(sqerl, prepared_statements, {?MODULE, statements, []}),
    ColumnTransforms = [{<<"created">>,
                         fun sqerl_transformers:convert_YMDHMS_tuple_to_datetime/1}],
    ok = application:set_env(sqerl, column_transforms, ColumnTransforms),
    PoolConfig = [{name, ?POOL_NAME},
                  {max_count, ?MAX_POOL_COUNT},
                  {init_count, 1},
                  {start_mfa, {sqerl_client, start_link, []}}],
    ok = application:set_env(pooler, pools, [PoolConfig]),
    application:start(crypto),
    application:start(public_key),
    application:start(ssl),
    application:start(epgsql).

statements() ->
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

      {<<"Execute simple query">>,
       fun select_simple/0},
      {<<"Execute simple query with parameters">>,
       fun select_simple_with_parameters/0},

      {<<"Handles timeout correctly">>,
        {timeout, 30,
        fun select_timeout/0}},

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
      {<<"Adhoc select strings">>,
       fun adhoc_select_complex_strings/0},
      {<<"Adhoc select atoms">>,
       fun adhoc_select_complex_atoms/0},
      {<<"Adhoc select order by">>,
       fun adhoc_select_order_by/0},
      {<<"Adhoc select limit">>,
       fun adhoc_select_limit/0},
      {<<"Adhoc select offset">>,
       fun adhoc_select_offset/0},

      %%{<<"Adhoc update">>,
      %% fun adhoc_update/0},

      {<<"Adhoc insert">>,
       fun adhoc_insert/0},
      {<<"Adhoc insert many users">>,
       fun adhoc_insert_many_users/0},
      {<<"Adhoc insert even more users">>,
       fun adhoc_insert_even_more_users/0},
      {<<"Adhoc insert rows">>,
       fun adhoc_insert_rows/0},

      {<<"Insert/select gzip data">>,
        {setup,
         fun() ->
                 Text = <<"data to compress with gzip">>,
                 GzipData = zlib:gzip(Text),
                 Row = [{last_name, <<"gzip">>}, {datablob, GzipData}],
                 {ok, Count} = sqerl:adhoc_insert(users, [Row]),
                 ?assertEqual(1, Count),
                 Text
         end,
         fun(_) ->
                 {ok, Count} = sqerl:adhoc_delete(users, {last_name, equals, <<"gzip">>}),
                 ?assertEqual(1, Count)
         end,
         fun(Text) ->
                 fun() ->
                         {ok, ReturnedRows} = sqerl:adhoc_select([last_name, datablob],
                                                                 users,
                                                                 {last_name, equals, <<"gzip">>}),
                         ?assertEqual(1, length(ReturnedRows)),
                         [[{_, _}, {<<"datablob">>, QueriedGzipData}]] = ReturnedRows,
                         ?assertEqual(Text, zlib:gunzip(QueriedGzipData))
                 end
         end}},

      {<<"Delete operation">>,
       fun delete_data/0}
     ]}.

kill_pool(1) ->
    pooler:take_member(?POOL_NAME);
kill_pool(X) ->
    pooler:take_member(?POOL_NAME),
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


%%%
%%% Tests for execute interface
%%%
select_simple() ->
    Sql = <<"select count(*) as num_users from users">>,
    {ok, Rows} = sqerl:execute(Sql),
    [[{<<"num_users">>, NumUsers}]] = Rows,
    ?assertEqual(4, NumUsers).

select_simple_with_parameters() ->
    Sql = <<"select id from users where last_name = $1">>,
    {ok, Rows} = sqerl:execute(Sql, ["Smith"]),
    ExpectedRows = [[{<<"id">>,1}]],
    ?assertEqual(ExpectedRows, Rows).


%%
%% Tests for adhoc SQL
%%
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

adhoc_select_complex_strings() ->
    Where = {'and', [{"high_score", gt, 10},
                     {'or', [{"last_name", in, ["Maier", "Anderson"]},
                             {"active", equals, false}]}
                     ]},
    {ok, Rows} = sqerl:adhoc_select(["id"], "users", Where),
    ExpectedRows = [
                    [{<<"id">>, 2}],
                    [{<<"id">>, 4}]
                   ],
    ?assertEqual(lists:sort(ExpectedRows), lists:sort(Rows)).

adhoc_select_complex_atoms() ->
    Where = {'and', [{high_score, gt, 10},
                     {'or', [{last_name, in, ['Maier', 'Anderson']},
                             {active, equals, false}]}
                     ]},
    {ok, Rows} = sqerl:adhoc_select([id], users, Where),
    ExpectedRows = [
                    [{<<"id">>, 2}],
                    [{<<"id">>, 4}]
                   ],
    ?assertEqual(lists:sort(ExpectedRows), lists:sort(Rows)).

adhoc_select_order_by() ->
    {ok, Rows} = sqerl:adhoc_select([<<"id">>], <<"users">>, all, [{order_by, [<<"id">>]}]),
    ExpectedRows = [
                    [{<<"id">>, 1}],
                    [{<<"id">>, 2}],
                    [{<<"id">>, 3}],
                    [{<<"id">>, 4}]
                   ],
    ?assertEqual(ExpectedRows, Rows).

adhoc_select_limit() ->
    {ok, Rows} = sqerl:adhoc_select([<<"id">>], <<"users">>, all, [{order_by, [<<"id">>]}, {limit, 2}]),
    ExpectedRows = [
                    [{<<"id">>, 1}],
                    [{<<"id">>, 2}]
                   ],
    ?assertEqual(ExpectedRows, Rows).

adhoc_select_offset() ->
    {ok, Rows} = sqerl:adhoc_select([<<"id">>], <<"users">>, all, [{order_by, [<<"id">>]}, {limit, {2, offset, 2}}]),
    ExpectedRows = [
                    [{<<"id">>, 3}],
                    [{<<"id">>, 4}]
                   ],
    ?assertEqual(ExpectedRows, Rows).


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
    adhoc_insert_delete_test(Table, Columns, Data, 10).

adhoc_insert_delete_test(Table, Columns, Data, BatchSize) ->
    {ok, InsertCount} = sqerl:adhoc_insert(Table, Columns, Data, BatchSize),
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
    adhoc_insert_delete_test(Table, Columns, Data, 10).

adhoc_insert_even_more_users() ->
    Table = <<"users">>,
    Columns = [<<"first_name">>,
               <<"last_name">>],
    F = fun(I) -> list_to_binary(integer_to_list(I)) end,
    Data = [[F(I), F(I)] || I <- lists:seq(1000, 2004)],
    adhoc_insert_delete_test(Table, Columns, Data, 100).

adhoc_insert_rows() ->
    Table = <<"users">>,
    F = fun(I) -> list_to_binary(integer_to_list(I)) end,
    Rows = [[{<<"first_name">>, F(I)}, {<<"last_name">>, F(I)}] || I <- lists:seq(3000, 4004)],
    {ok, InsertCount} = sqerl:adhoc_insert(Table, Rows, 100),
    %% verify data was inserted correctly -- relies on first field
    Where = {<<"first_name">>, in, [F(I) || I <- lists:seq(3000, 4004)]},
    {ok, ReturnedRows} = sqerl:adhoc_select([<<"first_name">>, <<"last_name">>], Table, Where),
    %% clean up before asserts
    {ok, DeleteCount} = sqerl:adhoc_delete(Table, Where),
    %% now verify...
    ?assertEqual(length(Rows), InsertCount),
    ?assertEqual(Rows, ReturnedRows),
    ?assertEqual(length(Rows), DeleteCount).

select_timeout() ->
    SQL = <<"select pg_sleep(30)">>,
    Result = sqerl:execute(SQL),
    ?assertEqual({error, timeout}, Result).

array_test_() ->
    setup_env(),
    Status = application:start(sqerl),
    %% sqerl should start or already be running for each test
    ?assert(lists:member(Status, [ok, {error, {already_started, sqerl}}])),
    {foreach,
     fun() -> error_logger:tty(true) end,
     fun(_) -> error_logger:tty(true) end,
     [
      {<<"Insert arrays with statements">>,
       fun insert_direct_array/0},
      {<<"Insert arrays with prepared statements">>,
       fun insert_array/0},
      {<<"Insert array of UUIDs">>,
       fun insert_uuidarray/0}

     ]}.

insert_direct_array() ->
    Stmt = <<"SELECT insert_users(ARRAY['Kevin'], ARRAY['Bacon'], ARRAY[999], ARRAY['2011-10-01 16:47:46'], ARRAY[true])">>,
    Expected = {ok, [[{<<"insert_users">>,<<>>}]]},
    Got = sqerl:execute(Stmt),
    ?assertMatch(Expected, Got),
    %% Cleanup
    ?assertMatch({ok, 1}, sqerl:statement(delete_user_by_lname, [<<"Bacon">>])).

insert_array() ->
    Data = to_columns(?NAMES),
    Expected = {ok, [[{<<"insert_users">>, <<>>}]]},
    ?assertMatch(Expected, sqerl:execute(new_users, Data)),

    %% cleanup
    Ok = lists:duplicate(4, {ok, 1}),
    ?assertMatch(Ok, [sqerl:statement(delete_user_by_lname, [LName]) ||
            [_, LName, _, _, _] <- ?NAMES]).

insert_uuidarray() ->
    Data = [ [<<"46f16152-6366-4abd-8110-dbd9756bde91">>,
              <<"733ef125-e115-41e2-a2fb-4fe7fdd84f92">>,
              <<"52b49b92-0308-46c1-bc15-c657a044f0e3">>] ],
    Expected = {ok, [[{<<"insert_ids">>, <<>>}]]},
    ?assertMatch(Expected, sqerl:execute(new_ids, Data)),

    ExpectedData = [[{<<"id">>, <<"46f16152-6366-4abd-8110-dbd9756bde91">>}],
                    [{<<"id">>, <<"733ef125-e115-41e2-a2fb-4fe7fdd84f92">>}],
                    [{<<"id">>, <<"52b49b92-0308-46c1-bc15-c657a044f0e3">>}]],
    ?assertMatch({ok, ExpectedData}, sqerl:execute(<<"SELECT * from uuids">>, [])).


to_columns(Rows) ->
    to_columns(Rows, [], [], [], [], []).

to_columns([], Col1, Col2, Col3, Col4, Col5) ->
    [Col1, Col2, Col3, Col4, Col5];
to_columns([[C1, C2, C3, C4, C5] | Rest], Col1, Col2, Col3, Col4, Col5) ->
    to_columns(Rest, [list_to_binary(C1)|Col1], [list_to_binary(C2) | Col2], [C3 | Col3],
               [C4 | Col4], [C5 | Col5]).
