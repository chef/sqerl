%% -*- erlang-indent-level: 4;indent-tabs-mode: nil; fill-column: 92-*-
%% ex: ts=4 sw=4 et
%% @author Jean-Philippe Langlois <jpl@opscode.com>
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

-module(sqerl_sql).

-export([select_in/4,
         select_in/5]).

-include_lib("eunit/include/eunit.hrl").

%% @doc Generates SELECT ... IN SQL with parameter strings (not values),
%% which can be prepared and executed.
%%
%% e.g. SELECT Field1 FROM Table WHERE MatchField IN (?, ?, ?, ...)
%%
%% ParameterStyle is qmark (?, ?, ... for e.g. mysql) 
%% or dollarn ($1, $2, etc. for e.g. pgsql)
%%
-spec select_in([any()], any(), any(), integer(), atom()) -> string().
select_in(ReportFields, Table, MatchField, NumValues, ParameterStyle) 
  when is_integer(NumValues), NumValues > 0 ->
    ArgumentStrings = parameter_strings(NumValues, ParameterStyle),
    InString = string:join(ArgumentStrings, ","),
    generate_select_in(ReportFields, Table, MatchField, InString).

%% @doc Generates SQL with values inline.
%%
%% e.g. SELECT Field1 FROM Table WHERE MatchField IN (Val1, Val2, ...)
%%
-spec select_in([any()], any(), any(), [any()]) -> string().
select_in(ReportFields, Table, MatchField, MatchValues) when is_list(MatchValues) ->
    ConvertedValues = convert_values(MatchValues), %% also validates safe values
    InString = string:join(ConvertedValues, ","),
    generate_select_in(ReportFields, Table, MatchField, InString).

%% @doc Generates SQL. Internal version. Assumes InString is safe.
-spec generate_select_in([any()], any(), any(), [any()]) -> string().
generate_select_in(ReportFields, Table, MatchField, InString) ->
    safe_values(ReportFields),
    safe_value(Table),
    safe_value(MatchField),
    FieldsString = comma_join(ReportFields),
    lists:flatten(io_lib:format(
      "SELECT ~s FROM ~s WHERE ~s IN (~s)",
      [FieldsString, Table, MatchField, InString])).

select_in_strings_test() -> 
    ExpectedSQL = "SELECT Field1,Field2 FROM Table1 WHERE MatchField IN ('1','5','3','4')",
    GeneratedSQL = select_in(["Field1", "Field2"], "Table1", "MatchField", ["1","5","3","4"]),
    ?assertEqual(ExpectedSQL, GeneratedSQL).

select_in_ints_test() -> 
    ExpectedSQL = "SELECT Field1,Field2 FROM Table1 WHERE MatchField IN (1,5,3,4)",
    GeneratedSQL = select_in(["Field1", "Field2"], "Table1", "MatchField", [1,5,3,4]),
    ?assertEqual(ExpectedSQL, GeneratedSQL).

select_in_param_qmark_test() -> 
    ExpectedSQL = "SELECT Field1,Field2 FROM Table1 WHERE MatchField IN (?,?,?,?)",
    GeneratedSQL = select_in(["Field1", "Field2"], "Table1", "MatchField", 4, qmark),
    ?assertEqual(ExpectedSQL, GeneratedSQL).

select_in_param_dollarn_test() -> 
    ExpectedSQL = "SELECT Field1,Field2 FROM Table1 WHERE MatchField IN ($1,$2,$3,$4)",
    GeneratedSQL = select_in(["Field1", "Field2"], "Table1", "MatchField", 4, dollarn),
    ?assertEqual(ExpectedSQL, GeneratedSQL).


%%
%% Value conversion
%%

-define(SQB, <<"'">>).
-define(SQS, "'").

%% @doc Convert a list of values to values usable in 
%% an SQL query. See convert_value/1.
-spec convert_values([any()]) -> [any()].
convert_values(L) -> [convert_value(E) || E <- L].

convert_values_integers_test() ->
    Input = [1, 5, 10],
    ExpectedOutput = ["1", "5", "10"],
    Output = convert_values(Input),
    ?assertEqual(ExpectedOutput, Output).

convert_values_binaries_test() ->
    Input = [<<"Hello">>, <<"There">>],
    ExpectedOutput = [[?SQB, <<"Hello">>, ?SQB], [?SQB, <<"There">>, ?SQB]],
    Output = convert_values(Input),
    ?assertEqual(ExpectedOutput, Output).

convert_values_strings_test() ->
    Input = ["Hello", "There"],
    ExpectedOutput = ["'Hello'", "'There'"],
    Output = convert_values(Input),
    ?assertEqual(ExpectedOutput, Output).

%% @doc Convert value to something usable in SQL,
%% including enclosing the value in quotes
%% if applicable. Also validates that the values are safe.
-spec convert_value(string() | integer() | binary()) -> string() | binary().
convert_value(V) when is_list(V) -> safe_value(V), ?SQS ++ V ++ ?SQS; %% strings are lists of ints
convert_value(V) when is_integer(V) -> integer_to_list(V);
convert_value(V) when is_binary(V) -> safe_value(V), [?SQB, V, ?SQB].

convert_value_integer_test() ->
    ?assertEqual("12", convert_value(12)).

convert_string_test() ->
    ?assertEqual("'Hello'", convert_value("Hello")).

convert_value_binary_test() ->
    ?assertEqual([?SQB, <<"Hello">>, ?SQB], convert_value(<<"Hello">>)).


%%%
%%% SQL Value Safety
%%% Uses RE so can take string and binary as input
%%%

-define(SAFE_VALUE_RE, "^[A-Za-z0-9_]*$").

%% @doc Checks that value is safe to use while generating SQL.
-spec safe_value(any()) -> true.
safe_value(Value) -> {match, _} = re:run(Value, ?SAFE_VALUE_RE), true.

safe_value_string_test() ->
    ?assertEqual(true, safe_value("ABCdef123_")).

safe_value_binary_test() ->
    ?assertEqual(true, safe_value(<<"ABCdef123_">>)).

safe_value_bad_values_test() ->
    BadValues = "`-=[]\;',./~!@#$%^&*()+{}|:\"<>?",
    safe_value_bad_values_test(BadValues).

safe_value_bad_values_test([]) -> 
    ok;

safe_value_bad_values_test([H|T]) -> 
    safe_value_bad_value_test([H]),
    safe_value_bad_values_test(T).

safe_value_bad_value_test(BadValue) ->
    ?assertException(error, {badmatch, nomatch}, safe_value(BadValue)).


%% @doc Check that all values are safe.
-spec safe_values([any()]) -> true.
safe_values([]) -> true;
safe_values([H|T]) -> safe_value(H), safe_values(T).

safe_values_test() ->
    safe_values(["toto", "mytable", "_Field_1", <<"binary">>]).

safe_values_bad_values_test() ->
    BadValuesSets = [
                     ["good", "`bad`", "ok"],
                     ["ok", "'bad'", "bad;"]],
    safe_values_bad_values_test(BadValuesSets).

safe_values_bad_values_test([]) -> 
    ok;
safe_values_bad_values_test([H|T]) ->
    ?assertException(error, {badmatch, nomatch}, safe_values(H)),
    safe_values_bad_values_test(T).


%%
%% Parameter strings
%%

%% @doc Generates a list of parameter strings with the
%% given parameter style.
-spec parameter_strings(integer(), atom()) -> [string()].
parameter_strings(NumValues, Style) when NumValues > 0 ->
    [parameter_string(N, Style) || N <- lists:seq(1, NumValues)].

parameter_strings_qmark_test() ->
    ?assertEqual(["?", "?", "?"], parameter_strings(3, qmark)),
    ?assertEqual(["?", "?", "?", "?", "?"], parameter_strings(5, qmark)).

parameter_strings_dollarn_test() ->
    ?assertEqual(["$1", "$2", "$3"], parameter_strings(3, dollarn)),
    ?assertEqual(["$1", "$2", "$3", "$4", "$5", "$6"], parameter_strings(6, dollarn)).

parameter_strings_le0_test() ->
    ?assertException(error, function_clause, parameter_strings(0, qmark)),
    ?assertException(error, function_clause, parameter_strings(-2, qmark)).

%% @doc Returns a parameter string for the given style
%% at position N in the statement.
-spec parameter_string(integer(), atom()) -> string().
parameter_string(_N, qmark)  -> "?";
parameter_string(N, dollarn) -> "$" ++ integer_to_list(N).

parameter_string_qmark_test() ->
    %% For qmark style, it should always be a qmark
    ?assertEqual("?", parameter_string(1, qmark)),
    ?assertEqual("?", parameter_string(2, qmark)),
    ?assertEqual("?", parameter_string(120, qmark)).

parameter_string_dollarn_test() ->
    %% For dollarn style it should be "$n"
    ?assertEqual("$1", parameter_string(1, dollarn)),
    ?assertEqual("$2", parameter_string(2, dollarn)),
    ?assertEqual("$5", parameter_string(5, dollarn)),
    ?assertEqual("$120", parameter_string(120, dollarn)).

parameter_string_bad_style_test() ->
    %% Anything else should cause an error
    ?assertException(error, function_clause, parameter_string(1, unsupported)),
    ?assertException(error, function_clause, parameter_string(4, toto)).

%%
%% Utilities
%%

%% @doc Join list with commas.
%% For strings, uses string:join.
%% For binaries, use our own. Note that in this case a deep list
%% will be created, but consumers of this data should expect that
%% and know how to handle it.
comma_join([]) -> [];
comma_join([H|T]) when is_list(H) -> string:join([H|T], ",");
comma_join([H|T]) when is_binary(H) -> join([H|T], <<",">>).

comma_join_test() ->
    ?assertEqual([], comma_join([])),
    ?assertEqual("A", comma_join(["A"])),
    ?assertEqual("A,B,C", comma_join(["A", "B", "C"])),
    ?assertEqual([<<"A">>], comma_join([<<"A">>])),
    ?assertEqual([<<"A">>, <<",">>, <<"B">>, <<",">>, <<"C">>], comma_join([<<"A">>, <<"B">>, <<"C">>])).

%% @doc join elements of list with Sep
%% e.g. join([1,2,3], 0) -> [1,0,2,0,3]
join([], _Sep) ->
    [];
join([H|T], Sep) ->
    [H] ++ lists:append([[Sep] ++ [X] || X <- T]).

join_test() ->
    ?assertEqual([], join([], 0)),
    ?assertEqual([1], join([1], 0)),
    ?assertEqual([1, 0, 2, 0, 3], join([1,2,3], 0)),
    ?assertEqual([<<"1">>, <<",">>, <<"2">>, <<",">>, <<"3">>], join([<<"1">>, <<"2">>, <<"3">>], <<",">>)).

