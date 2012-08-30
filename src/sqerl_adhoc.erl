%% -*- erlang-indent-level: 4;indent-tabs-mode: nil; fill-column: 92-*-
%% ex: ts=4 sw=4 et
%% @author Kevin Smith <kevin@opscode.com>
%% Copyright 2011-2012 Opscode, Inc. All Rights Reserved.
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

-module(sqerl_adhoc).

-ifndef(TEST).
-export([select/4]).
-else.
-include_lib("eunit/include/eunit.hrl").
-compile([export_all]).
-endif.

-include("sqerl.hrl").

-define(SAFE_VALUE_RE, "^[A-Za-z0-9_]*$").
-define(IS_TEXT(X), is_binary(X) orelse is_list(X)).

select(Columns, Table, Where, Params) when ?IS_TEXT(Table),
                                           length(Columns) > 0 ->
    %% Play gatekeeper to make sure only "clean" inputs
    %% are used
    [ensure_safe(C) || C <- Columns],
    ensure_safe(Table),
    ensure_safe(Where),
    validate_params(Params),
    build_select(Columns, Table, Where, Params).

%% Internal functions
ensure_safe({in, Field}) ->
    ensure_safe(Field);
ensure_safe(Text) when is_list(Text);
                       is_binary(Text) ->
    Type = if is_list(Text) ->
                   list;
              true ->
                   binary
           end,
    case re:run(Text, ?SAFE_VALUE_RE, [{capture, first, Type}]) of
        {match, [Text]} ->
            ok;
         _ ->
            error(badarg)
    end.

validate_params(Params) when length(Params) > 0 ->
    ok;
validate_params(Params) when Params > 0 ->
    ok;
validate_params(_Params) ->
    error(badarg).

build_select(Columns, Table, {in, Field}, Params) ->
    SelectFromClause = build_select_from_clause(Columns, Table, []),
    WhereClause = list_to_binary([<<" WHERE ">>, Field, <<" IN (">>]),
    ParamList = generate_param_list(Params),
    {ok, list_to_binary([SelectFromClause, WhereClause, ParamList, <<")">>])}.

build_select_from_clause([Column|[]], Table, Accum) ->
    list_to_binary([<<"SELECT ">>, lists:reverse(Accum), Column, <<" FROM ">>, Table]);
build_select_from_clause([Column|T], Table, Accum) ->
    build_select_from_clause(T, Table, [list_to_binary([Column, ","])|Accum]).

generate_param_list(ParamCount) when is_integer(ParamCount) ->
    validate_params(ParamCount),
    {ok, DbType} = application:get_env(sqerl, db_type),
    string:join([sqerl_client:create_bound_parameter(DbType, X) || X <- lists:seq(1, ParamCount)], ",");
generate_param_list(ParamValues) when is_list(ParamValues) ->
    validate_params(ParamValues),
    string:join([make_param_value(PV) || PV <- ParamValues], ",").

make_param_value(V) when is_integer(V) ->
    integer_to_list(V);
make_param_value(V) when is_float(V) ->
    [CV] = io_lib:format("~p", [V]),
    CV;
%% Do we need to validate against regex?
make_param_value(V) when is_list(V);
                         is_binary(V) ->
    ["'", V, "'"].

-ifdef(TEST).
select_int_test() ->
	ExpectedSQL = <<"SELECT Field1,Field2 FROM Table1 WHERE MatchField IN (1,5,3,4)">>,
	{ok, GeneratedStringSQL} = select(["Field1", "Field2"], "Table1", {in, "MatchField"}, [1,5,3,4]),
	{ok, GeneratedBinarySQL} = select([<<"Field1">>, "Field2"], <<"Table1">>, {in, "MatchField"}, [1,5,3,4]),
	?assertEqual(ExpectedSQL, GeneratedStringSQL),
    ?assertEqual(ExpectedSQL, GeneratedBinarySQL).

select_float_test() ->
	ExpectedSQL = <<"SELECT Field1,Field2 FROM Table1 WHERE MatchField IN (0.1,1.0,0.123)">>,
	{ok, GeneratedStringSQL} = select(["Field1", "Field2"], "Table1", {in, "MatchField"}, [0.1,1.0,0.123]),
	{ok, GeneratedBinarySQL} = select([<<"Field1">>, "Field2"], <<"Table1">>, {in, "MatchField"}, [0.1,1.0,0.123]),
	?assertEqual(ExpectedSQL, GeneratedStringSQL),
    ?assertEqual(ExpectedSQL, GeneratedBinarySQL).


select_string_test() ->
	ExpectedSQL = <<"SELECT Field1,Field2 FROM Table1 WHERE MatchField IN ('1','5','3','4')">>,
	{ok, GeneratedStringSQL} = select(["Field1", "Field2"], "Table1", {in, "MatchField"}, ["1","5","3","4"]),
	{ok, GeneratedBinarySQL} = select([<<"Field1">>, "Field2"], <<"Table1">>, {in, "MatchField"}, ["1",<<"5">>,"3",<<"4">>]),
	?assertEqual(ExpectedSQL, GeneratedStringSQL),
    ?assertEqual(ExpectedSQL, GeneratedBinarySQL).

select_in_param_mysql_test() ->
	%% Set db type to mysql to check mysql-style (qmark).
	set_db_type(mysql),
	ExpectedSQL = <<"SELECT Field1,Field2 FROM Table1 WHERE MatchField IN (?,?,?,?)">>,
	{ok, GeneratedSQL} = select(["Field1", "Field2"], "Table1", {in, "MatchField"}, 4),
	?assertEqual(ExpectedSQL, GeneratedSQL).

select_in_param_pgsql_test() ->
	%% Set db type to pgsql to check pgsql-style (dollarn).
	set_db_type(pgsql),
	ExpectedSQL = <<"SELECT Field1,Field2 FROM Table1 WHERE MatchField IN ($1,$2,$3,$4)">>,
	{ok, GeneratedSQL} = select(["Field1", "Field2"], "Table1", {in, "MatchField"}, 4),
	?assertEqual(ExpectedSQL, GeneratedSQL).

string_param_list_test() ->
    ExpectedSQL = <<"'test1','test2','test3','test4'">>,
    Params = list_to_binary(generate_param_list(["test1", "test2", "test3", "test4"])),
    ?assertMatch(ExpectedSQL, Params).

binary_param_list_test() ->
    ExpectedSQL = <<"'test1','test2','test3','test4'">>,
    Params = list_to_binary(generate_param_list([<<"test1">>, <<"test2">>, <<"test3">>, <<"test4">>])),
    ?assertMatch(ExpectedSQL, Params).

int_param_list_test() ->
    ExpectedSQL = <<"1,2,3,4,5">>,
    Params = list_to_binary(generate_param_list([1,2,3,4,5])),
    ?assertMatch(ExpectedSQL, Params).

float_param_list_test() ->
    ExpectedSQL = <<"1.0,0.1,0.123">>,
    Params = list_to_binary(generate_param_list([1.0, 0.1, 0.123])),
    ?assertMatch(ExpectedSQL, Params).

mysql_positional_param_list_test() ->
    set_db_type(mysql),
    Single = <<"?">>,
    Multi = <<"?,?,?">>,
    SingleParam = list_to_binary(generate_param_list(1)),
    ?assertMatch(Single, SingleParam),
    MultiParams = list_to_binary(generate_param_list(3)),
    ?assertMatch(Multi, MultiParams).

pgsql_positional_param_list_test() ->
    set_db_type(pgsql),
    Single = <<"$1">>,
    Multi = <<"$1,$2,$3">>,
    SingleParam = list_to_binary(generate_param_list(1)),
    ?assertMatch(Single, SingleParam),
    MultiParams = list_to_binary(generate_param_list(3)),
    ?assertMatch(Multi, MultiParams).

bad_arg_test() ->
    %% Bad column name
    ?assertError(badarg, sqerl_adhoc:select(["test!", "test1", "test2"], "test_table", {in, "test2"}, 3)),
    ?assertError(badarg, sqerl_adhoc:select([<<"test!">>, "test1", "test2"], "test_table", {in, "test2"}, 3)),
    %% Bad table name
    ?assertError(badarg, sqerl_adhoc:select(["test3", "test1", "test2"], "test-table", {in, "test2"}, 3)),
    ?assertError(badarg, sqerl_adhoc:select(["test3", "test1", "test2"], <<"test-table">>, {in, "test2"}, 3)),
    %% Bad where
    ?assertError(badarg, sqerl_adhoc:select(["test3", "test1", "test2"], "test-table", {in, "test2%"}, 3)),
    ?assertError(badarg, sqerl_adhoc:select(["test3", "test1", "test2"], "test-table", {in, <<"test2%">>}, 3)),
    %% Funky params
    ?assertError(badarg, sqerl_adhoc:select(["test3", "test1", "test2"], "test-table", {in, "test2"}, three)),
    ?assertError(badarg, sqerl_adhoc:select(["test3", "test1", "test2"], "test-table", {in, "test2"}, erlang:make_ref())).

set_db_type(DBType) ->
	application:set_env(sqerl, db_type, DBType).
-endif.
