-module(sqerl_integration_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(FIRST(Record), {first_as_record, [Record, Record:fields()]}).

-compile([export_all]).

-define(NAMES, [["Kevin", "Smith", 666, <<"2011-10-01 16:47:46">>, true],
                ["Mark", "Anderson", 42, <<"2011-10-02 16:11:46">>, true],
                ["Chris", "Maier", 0, <<"2011-10-03 16:47:46">>, true],
                ["Elvis", "Presley", 16, <<"2011-10-04 16:47:46">>, false]]).

all() -> [pool_overflow, insert_data, insert_returning, select_data, select_data_as_record,
            select_first_number_zero, delete_data, update_datablob, select_boolean, update_created,
            select_simple, select_simple_multipool_, adhoc_select, adhoc_insert, insert_select_gzip_data, array_test,
            select_timeout, execute_timeout].

init_per_testcase(_, Config) ->
    pgsql_test_buddy:clean(),
    pgsql_test_buddy:create(Config),
    pgsql_test_buddy:setup_env(),
    Config.

end_per_testcase(_, _Config) ->
    pgsql_test_buddy:teardown_env(),
    ok.

pool_overflow(_Config) ->
    pgsql_test_buddy:kill_pool(),
    % Doesn't matter what we do from here; we're just testing operations with
    % a depleted pool
    Expected = {error, no_connections},
    Results = sqerl:select(find_user_by_lname, ["Smith"], ?FIRST(obj_user)),
    ?assertEqual(Expected, Results),
    Expected = Results,
    ct:pal("We did it! World's best cup of coffee!").

insert_data(_Config) ->
    Expected = lists:duplicate(4, {ok, 1}),
    ?assertEqual(Expected, [sqerl:statement(new_user, Name) || Name <- ?NAMES]).

insert_returning(_) ->
    UserData = ["Free", "Ride", 0, <<"2014-10-03 16:47:46">>, true],
    {ok, 1, [Data]} = sqerl:select(new_user_returning, UserData),
    ?assert(is_integer(proplists:get_value(<<"id">>, Data))),
    ?assertEqual(<<"Free">>, proplists:get_value(<<"first_name">>, Data)).

select_data(Config) ->
    insert_data(Config),
    {ok, User} = sqerl:select(find_user_by_lname, ["Smith"], first),
    ?assertMatch(<<"Kevin">>, proplists:get_value(<<"first_name">>, User)),
    ?assertMatch(<<"Smith">>, proplists:get_value(<<"last_name">>, User)),
    ?assertEqual(666, proplists:get_value(<<"high_score">>, User)),
    ?assertEqual(true, proplists:get_value(<<"active">>, User)),
    ?assert(is_integer(proplists:get_value(<<"id">>, User))).

select_data_as_record(Config) ->
    insert_data(Config),
    {ok, User} = sqerl:select(find_user_by_lname, ["Smith"], ?FIRST(obj_user)),
    ?assertMatch(<<"Kevin">>, obj_user:getval(first_name, User)),
    ?assertMatch(<<"Smith">>, obj_user:getval(last_name, User)),
    ?assertEqual(666, obj_user:getval(high_score, User)),
    ?assertEqual(true, obj_user:getval(active, User)),
    ?assert(is_integer(obj_user:getval(id, User))).

select_first_number_zero(Config) ->
    insert_data(Config),
    Expected = [{ok, 666}, {ok, 42}, {ok, 0}, {ok, 16} ],
    Returned =  [sqerl:select(find_score_by_lname, [LName], first_as_scalar, [high_score]) ||
                    [_, LName, _, _, _] <- ?NAMES],
    ?assertMatch(Expected, Returned).

delete_data(Config) ->
    insert_data(Config),
    Expected = lists:duplicate(4, {ok, 1}),
    ?assertMatch(Expected, [sqerl:statement(delete_user_by_lname, [LName]) ||
                               [_, LName, _, _, _] <- ?NAMES]).

update_datablob(Config) ->
    insert_data(Config),
    ?assertMatch({ok, 1},
                 sqerl:statement(update_datablob_by_lname,
                                 [<<"foobar">>, "Smith"] )),
    {ok, User} = sqerl:select(find_datablob_by_lname, ["Smith"], first_as_scalar, [datablob]),
    ?assertMatch(<<"foobar">>, User).

select_boolean(Config) ->
    insert_data(Config),
    {ok, User} = sqerl:select(find_user_by_lname, ["Smith"], first),
    ?assertEqual(true, proplists:get_value(<<"active">>, User)),

    {ok, User1} = sqerl:select(find_user_by_lname, ["Presley"], first),
    ?assertEqual(false, proplists:get_value(<<"active">>, User1)).

update_created(Config) ->
    insert_data(Config),
    ?assertMatch({ok, 1},
                 sqerl:statement(update_created_by_lname,
                     [{datetime, {{2011, 11, 1}, {16, 47, 46}}},
                      "Smith"])),
    ?assertMatch({ok, 1},
                 sqerl:statement(update_created_by_lname,
                     [{{2011, 11, 2}, {16, 11, 46}}, "Anderson"])),
    ?assertMatch({ok, 1},
                 sqerl:statement(update_created_by_lname,
                     [<<"2011-11-03 16:47:46">>, "Maier"])),

    {ok, User1} = sqerl:select(find_created_by_lname, ["Smith"], first_as_scalar, [created]),
    ?assertMatch({datetime, {{2011, 11, 01}, {16, 47, 46}}}, User1),
    {ok, User2} = sqerl:select(find_created_by_lname, ["Anderson"], first_as_scalar, [created]),
    ?assertMatch({datetime, {{2011, 11, 02}, {16, 11, 46}}}, User2),
    {ok, User3} = sqerl:select(find_created_by_lname, ["Maier"], first_as_scalar, [created]),
    ?assertMatch({datetime, {{2011, 11, 03}, {16, 47, 46}}}, User3),

    %% former select_created_by_lname() ->
    {ok, User4} = sqerl:select(find_created_by_lname, ["Presley"], first_as_scalar, [created]),
    ?assertMatch({datetime, {{2011, 10, 04}, {16, 47, 46}}}, User4),

    %% former select_lname_by_created() ->
    {ok, User5} = sqerl:select(find_lname_by_created, [{datetime, {{2011, 10, 04}, {16, 47, 46}}}], first_as_scalar, [last_name]),
    ?assertMatch(<<"Presley">>, User5).

%%%
%%% Tests for execute interface
%%%
select_simple(Config) ->
    insert_data(Config),
    insert_returning(Config),

    SqlCount = <<"select count(*) as num_users from users">>,
    {ok, Count} = sqerl:execute(SqlCount),
    [[{<<"num_users">>, 5}]] = Count,



    %% select_simple_with_parameters() ->
    Sql = <<"select id from users where last_name = $1">>,
    {ok, Rows} = sqerl:execute(Sql, ["Smith"]),
    ExpectedRows = [[{<<"id">>,1}]],
    ?assertEqual(ExpectedRows, Rows).

select_simple_multipool_(_Config) ->
    [ ?_assertMatch(ok, 0), sqerl:execute_with(sqerl:make_context(other), <<"SELECT COUNT(*) FROM only_in_itest_sqerl2_db">>),
      ?_assertMatch(ok, 0), sqerl:execute_with(sqerl:make_context(sqerl), <<"SELECT COUNT(*) FROM only_in_itest_sqerl1_db">>)].

adhoc_select(Config) ->
    insert_data(Config),
    insert_returning(Config),

    %% This is kind of a silly way to do it, but wrapping these in fun()s lets
    %% us reuse varaibles by limiting scope. Making the diff from itest.erl
    %% much easier to read

    %% adhoc_select_all
    fun() ->
        ExpectedNumRows = length(?NAMES) + 1,
        {ok, Rows} = sqerl:adhoc_select([<<"*">>], <<"users">>, all),
        ?assertEqual(ExpectedNumRows, length(Rows))
    end(),

    %% adhoc_select_equals_int
    fun() ->
        ExpectedRows = [[{<<"last_name">>, <<"Smith">>}]],
        {ok, Rows} = sqerl:adhoc_select([<<"last_name">>], <<"users">>,
                                        {<<"id">>, equals, 1}),
        ?assertEqual(ExpectedRows, Rows)
    end(),

    %% adhoc_select_equals_timestamp
    fun() ->
        ExpectedRows = [[{<<"last_name">>, <<"Presley">>}]],
        {ok, Rows} = sqerl:adhoc_select([<<"last_name">>], <<"users">>,
                                        {<<"created">>, equals, {datetime,{{2011,10,4},{16,47,46}}}}),
        ?assertEqual(ExpectedRows, Rows)
    end(),

    %% adhoc_select_equals_boolean
    fun() ->
        ExpectedRows = [[{<<"last_name">>, <<"Presley">>}]],
        {ok, Rows} = sqerl:adhoc_select([<<"last_name">>], <<"users">>,
                                        {<<"active">>, equals, false}),
        ?assertEqual(ExpectedRows, Rows)
    end(),

    %% adhoc_select_equals_str
    fun() ->
        ExpectedRows = [[{<<"id">>, 1}]],
        {ok, Rows} = sqerl:adhoc_select([<<"id">>], <<"users">>,
                                        {<<"last_name">>, equals, <<"Smith">>}),
        ?assertEqual(ExpectedRows, Rows)
    end(),

    %% adhoc_select_nequals_str
    fun() ->
        {ok, Rows} = sqerl:adhoc_select([<<"id">>], <<"users">>,
                                        {<<"last_name">>, nequals, <<"Smith">>}),
        ExpectedRows = [[{<<"id">>,2}],[{<<"id">>,3}],[{<<"id">>,4}], [{<<"id">>,5}]],
        ?assertEqual(lists:sort(ExpectedRows), lists:sort(Rows))
    end(),

    %% adhoc_select_nequals_int
    fun() ->
        {ok, Rows} = sqerl:adhoc_select([<<"last_name">>], <<"users">>,
                                        {<<"id">>, nequals, 1}),
        ExpectedRows = [
                        [{<<"last_name">>,<<"Presley">>}],
                        [{<<"last_name">>,<<"Anderson">>}],
                        [{<<"last_name">>,<<"Maier">>}],
                        [{<<"last_name">>,<<"Ride">>}]
                    ],
        ?assertEqual(lists:sort(ExpectedRows), lists:sort(Rows))
    end(),

    %% adhoc_select_not
    fun() ->
        Cols = [<<"id">>],
        Table = <<"users">>,
        Where = {'not', {<<"last_name">>, equals, <<"Smith">>}},
        {ok, Rows} = sqerl:adhoc_select(Cols, Table, Where),
        ExpectedRows = [
                        [{<<"id">>, 2}],
                        [{<<"id">>, 3}],
                        [{<<"id">>, 4}],
                        [{<<"id">>, 5}]
                       ],
        ?assertEqual(lists:sort(ExpectedRows), lists:sort(Rows))
    end(),

    %% adhoc_select_in_ids
    %% previous test setup has users with id 1 and 2; no users with id 309, 409
    fun() ->
        ExpectedRows = [
                        [{<<"last_name">>, <<"Smith">>}],
                        [{<<"last_name">>, <<"Anderson">>}]
                       ],
        Values = [1, 2, 309, 409],
        {ok, Rows} = sqerl:adhoc_select([<<"last_name">>],
                                        <<"users">>,
                                        {<<"id">>, in, Values}),
        ?assertEqual(lists:sort(ExpectedRows), lists:sort(Rows))
    end(),

    %% adhoc_select_in_names
    %% previous test setup has users with last names Smith and Anderson but not Toto and Tata
    fun() ->
        ExpectedRows = [
                        [{<<"id">>, 1},{<<"first_name">>, <<"Kevin">>}],
                        [{<<"id">>, 2},{<<"first_name">>, <<"Mark">>}]
                       ],
        Values = [<<"Smith">>, <<"Anderson">>, <<"Toto">>, <<"Tata">>],
        {ok, Rows} = sqerl:adhoc_select([<<"id">>, <<"first_name">>],
                                        <<"users">>,
                                        {<<"last_name">>, in, Values}),
        ?assertEqual(lists:sort(ExpectedRows), lists:sort(Rows))
    end(),

    %% adhoc_select_in_star()
    %% previous test setup has user with last name Smith but not Toto
    fun() ->
        ExpectedNumRows = 1,
        ExpectedNumCols = 7,
        Values = [<<"Smith">>, <<"Toto">>],
        {ok, Rows} = sqerl:adhoc_select([<<"*">>],
                                        <<"users">>,
                                        {<<"last_name">>, in, Values}),
        ?assertEqual(ExpectedNumRows, length(Rows)),
        ?assertEqual(ExpectedNumCols, length(lists:nth(1, Rows)))
    end(),

    %% adhoc_select_and()
    fun() ->
        Cols = [<<"id">>],
        Table = <<"users">>,
        Where1 = {<<"id">>, gt, 1},
        Where2 = {<<"last_name">>, nequals, <<"Smith">>},
        Where = {'and', [Where1, Where2]},
        {ok, Rows} = sqerl:adhoc_select(Cols, Table, Where),
        ExpectedRows = [
                        [{<<"id">>, 2}],
                        [{<<"id">>, 3}],
                        [{<<"id">>, 4}],
                        [{<<"id">>, 5}]
                       ],
        ?assertEqual(lists:sort(ExpectedRows), lists:sort(Rows))
    end(),

    %% adhoc_select_or
    fun() ->
        Cols = [<<"id">>],
        Table = <<"users">>,
        Where1 = {<<"id">>, gt, 1},
        Where2 = {<<"last_name">>, nequals, <<"Smith">>},
        Where = {'or', [Where1, Where2]},
        {ok, Rows} = sqerl:adhoc_select(Cols, Table, Where),
        ExpectedRows = [
                        [{<<"id">>, 2}],
                        [{<<"id">>, 3}],
                        [{<<"id">>, 4}],
                        [{<<"id">>, 5}]
                       ],
        ?assertEqual(lists:sort(ExpectedRows), lists:sort(Rows))
    end(),

    %% adhoc_select_complex
    fun() ->
        Where = {'and', [{<<"high_score">>, gt, 10},
                         {'or', [{<<"last_name">>, in, [<<"Maier">>, <<"Anderson">>]},
                                 {<<"active">>, equals, false}]}
                         ]},
        {ok, Rows} = sqerl:adhoc_select([<<"id">>], <<"users">>, Where),
        ExpectedRows = [
                        [{<<"id">>, 2}],
                        [{<<"id">>, 4}]
                       ],
        ?assertEqual(lists:sort(ExpectedRows), lists:sort(Rows))
    end(),

    %% adhoc_select_complex_strings
    fun() ->
        Where = {'and', [{"high_score", gt, 10},
                         {'or', [{"last_name", in, ["Maier", "Anderson"]},
                                 {"active", equals, false}]}
                         ]},
        {ok, Rows} = sqerl:adhoc_select(["id"], "users", Where),
        ExpectedRows = [
                        [{<<"id">>, 2}],
                        [{<<"id">>, 4}]
                       ],
        ?assertEqual(lists:sort(ExpectedRows), lists:sort(Rows))
    end(),

    %% adhoc_select_complex_atoms
    fun() ->
        Where = {'and', [{high_score, gt, 10},
                         {'or', [{last_name, in, ['Maier', 'Anderson']},
                                 {active, equals, false}]}
                         ]},
        {ok, Rows} = sqerl:adhoc_select([id], users, Where),
        ExpectedRows = [
                        [{<<"id">>, 2}],
                        [{<<"id">>, 4}]
                       ],
        ?assertEqual(lists:sort(ExpectedRows), lists:sort(Rows))
    end(),

    %% adhoc_select_order_by
    fun() ->
        {ok, Rows} = sqerl:adhoc_select([<<"id">>], <<"users">>, all, [{order_by, [<<"id">>]}]),
        ExpectedRows = [
                        [{<<"id">>, 1}],
                        [{<<"id">>, 2}],
                        [{<<"id">>, 3}],
                        [{<<"id">>, 4}],
                        [{<<"id">>, 5}]
                       ],
        ?assertEqual(ExpectedRows, Rows)
    end(),

    %% adhoc_select_limit
    fun() ->
        {ok, Rows} = sqerl:adhoc_select([<<"id">>], <<"users">>, all, [{order_by, [<<"id">>]}, {limit, 2}]),
        ExpectedRows = [
                        [{<<"id">>, 1}],
                        [{<<"id">>, 2}]
                       ],
        ?assertEqual(ExpectedRows, Rows)
    end(),
    %% adhoc_select_limit
    fun() ->
        {ok, Rows} = sqerl:adhoc_select_with(sqerl:make_context(sqerl), [<<"id">>], <<"users">>, all, [{order_by, [<<"id">>]}, {limit, 2}]),
        ExpectedRows = [
                        [{<<"id">>, 1}],
                        [{<<"id">>, 2}]
                       ],
        ?assertEqual(ExpectedRows, Rows)
    end(),

    %% adhoc_select_offset
    fun() ->
        {ok, Rows} = sqerl:adhoc_select([<<"id">>], <<"users">>, all, [{order_by, [<<"id">>]}, {limit, {2, offset, 2}}]),
        ExpectedRows = [
                        [{<<"id">>, 3}],
                        [{<<"id">>, 4}]
                       ],
        ?assertEqual(ExpectedRows, Rows)
    end().

adhoc_insert_delete_test(Table, Columns, Data, BatchSize) ->
    {ok, InsertCount} = sqerl:adhoc_insert(Table, Columns, Data, BatchSize),
    %% verify data was inserted correctly -- relies on first field
    %% values in test being unique.
    Field = lists:nth(1, Columns),
    Values = [Value || [Value|_] <- Data],
    Where = {Field, in, Values},
    {ok, Rows} = sqerl:adhoc_select(Columns, Table, Where),
    {ReturnedColumns, ReturnedData} = sqerl_core:extract_insert_data(Rows),
    %% clean up before asserts
    {ok, DeleteCount} = sqerl:adhoc_delete(Table, Where),
    %% now verify...
    ?assertEqual(length(Data), InsertCount),
    ?assertEqual(Columns, ReturnedColumns),
    ?assertEqual(Data, ReturnedData),
    ?assertEqual(length(Data), DeleteCount).

adhoc_insert(Config) ->
    insert_data(Config),
    insert_returning(Config),

    %% adhoc_insert
    fun() ->
        Table = <<"users">>,
        Columns = [<<"first_name">>,
                   <<"last_name">>,
                   <<"high_score">>,
                   <<"active">>],
        Data = [[<<"Joe1">>, <<"Smith">>, 17, true],
                [<<"Joe2">>, <<"Anderson">>, 23, false]],
        adhoc_insert_delete_test(Table, Columns, Data, 10)
    end(),

    %% adhoc_insert_many_users
    fun() ->
        Table = <<"users">>,
        Columns = [<<"first_name">>,
                   <<"last_name">>],
        F = fun(I) -> list_to_binary(integer_to_list(I)) end,
        Data = [[F(I), F(I)] || I <- lists:seq(1, 35)],
        adhoc_insert_delete_test(Table, Columns, Data, 10)
    end(),

    %% adhoc_insert_even_more_users
    fun() ->
        Table = <<"users">>,
        Columns = [<<"first_name">>,
                   <<"last_name">>],
        F = fun(I) -> list_to_binary(integer_to_list(I)) end,
        Data = [[F(I), F(I)] || I <- lists:seq(1000, 2004)],
        adhoc_insert_delete_test(Table, Columns, Data, 100)
    end(),

    %% adhoc_insert_rows
    fun() ->
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
        ?assertEqual(length(Rows), DeleteCount)
    end(),
    ok.

insert_select_gzip_data(_Config) ->
    Text = <<"data to compress with gzip">>,
    GzipData = zlib:gzip(Text),
    Row = [{last_name, <<"gzip">>}, {datablob, GzipData}],
    {ok, Count} = sqerl:adhoc_insert(users, [Row]),
    ?assertEqual(1, Count),
    {ok, ReturnedRows} = sqerl:adhoc_select([last_name, datablob],
                                             users,
                                             {last_name, equals, <<"gzip">>}),
    ?assertEqual(1, length(ReturnedRows)),
    [[{_, _}, {<<"datablob">>, QueriedGzipData}]] = ReturnedRows,
    ?assertEqual(Text, zlib:gunzip(QueriedGzipData)),
    ok.

execute_timeout(_Config) ->
    SQL = <<"select pg_sleep(30)">>,
    Result = sqerl:execute(SQL),
    ?assertEqual({error, timeout}, Result),
    ok.

select_timeout(_Config) ->
    Result = sqerl:select(select_sleep, []),
    ?assertEqual({error, timeout}, Result),
    ok.

array_test(_Config) ->
    %% insert_direct_array
    Stmt = <<"SELECT insert_users(ARRAY['Kevin'], ARRAY['Bacon'], ARRAY[999], ARRAY['2011-10-01 16:47:46'], ARRAY[true])">>,
    Expected = {ok, [[{<<"insert_users">>,<<>>}]]},
    Got = sqerl:execute(Stmt),
    ?assertMatch(Expected, Got),
    %% Cleanup
    ?assertMatch({ok, 1}, sqerl:statement(delete_user_by_lname, [<<"Bacon">>])),

    %% insert_array
    Data = to_columns(?NAMES),
    Expected = {ok, [[{<<"insert_users">>, <<>>}]]},
    ?assertMatch(Expected, sqerl:execute(new_users, Data)),

    %% cleanup
    Ok = lists:duplicate(4, {ok, 1}),
    ?assertMatch(Ok, [sqerl:statement(delete_user_by_lname, [LName]) ||
            [_, LName, _, _, _] <- ?NAMES]),

    %% insert_uuidarray
    Data2 = [ [<<"46f16152-6366-4abd-8110-dbd9756bde91">>,
              <<"733ef125-e115-41e2-a2fb-4fe7fdd84f92">>,
              <<"52b49b92-0308-46c1-bc15-c657a044f0e3">>] ],
    Expected2 = {ok, [[{<<"insert_ids">>, <<>>}]]},
    ?assertMatch(Expected2, sqerl:execute(new_ids, Data2)),

    ExpectedData = [[{<<"id">>, <<"46f16152-6366-4abd-8110-dbd9756bde91">>}],
                    [{<<"id">>, <<"733ef125-e115-41e2-a2fb-4fe7fdd84f92">>}],
                    [{<<"id">>, <<"52b49b92-0308-46c1-bc15-c657a044f0e3">>}]],
    ?assertMatch({ok, ExpectedData}, sqerl:execute(<<"SELECT * from uuids">>, [])),
    ok.

to_columns(Rows) ->
    to_columns(Rows, [], [], [], [], []).

to_columns([], Col1, Col2, Col3, Col4, Col5) ->
    [Col1, Col2, Col3, Col4, Col5];
to_columns([[C1, C2, C3, C4, C5] | Rest], Col1, Col2, Col3, Col4, Col5) ->
    to_columns(Rest, [list_to_binary(C1)|Col1], [list_to_binary(C2) | Col2], [C3 | Col3],
               [C4 | Col4], [C5 | Col5]).
