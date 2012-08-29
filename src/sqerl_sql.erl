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

-export([select_in/4]).

-include_lib("eunit/include/eunit.hrl").

%%%
%%% SQL generation
%%%

%% SELECT ReportField1, ReportField2, ... FROM Table WHERE MatchField IN (?, ?, ?, ...)
select_in(ReportFields, Table, MatchField, NumValues) when is_integer(NumValues), NumValues > 0 ->
  %% This version generates a statement with parameters that be prepared
  %% Input parameters should all conform to safe values
  safe_values(ReportFields),
  safe_value(Table),
  safe_value(MatchField),
  %% Generate SQL
  FieldsString = string:join(ReportFields, ","),
  ArgumentStrings = parameter_strings(NumValues),
  InString = string:join(ArgumentStrings, ","),
  lists:flatten(
	io_lib:format(
	  "SELECT ~s FROM ~s WHERE ~s IN (~s)",
      [FieldsString, Table, MatchField, InString]
    )
  );

select_in(ReportFields, Table, MatchField, MatchValues) ->
  %% This version generates a query ready to execute
  safe_values(ReportFields),
  safe_value(Table),
  safe_value(MatchField),
  ConvertedValues = convert_values(MatchValues), %% also validates safe values
  %% Generate SQL
  FieldsString = string:join(ReportFields, ","),
  InString = string:join(ConvertedValues, ","),
  lists:flatten(
	io_lib:format(
	  "SELECT ~s FROM ~s WHERE ~s IN (~s)",
      [FieldsString, Table, MatchField, InString]
    )
  ).

select_in_strings_test() -> 
	ExpectedSQL = "SELECT Field1,Field2 FROM Table1 WHERE MatchField IN ('1','5','3','4')",
	GeneratedSQL = select_in(["Field1", "Field2"], "Table1", "MatchField", ["1","5","3","4"]),
	?assertEqual(ExpectedSQL, GeneratedSQL).

select_in_ints_test() -> 
	ExpectedSQL = "SELECT Field1,Field2 FROM Table1 WHERE MatchField IN (1,5,3,4)",
	GeneratedSQL = select_in(["Field1", "Field2"], "Table1", "MatchField", [1,5,3,4]),
	?assertEqual(ExpectedSQL, GeneratedSQL).

select_in_param_mysql_test() -> 
	%% Set db type to mysql to check mysql-style (qmark).
	set_db_type(mysql),
	ExpectedSQL = "SELECT Field1,Field2 FROM Table1 WHERE MatchField IN (?,?,?,?)",
	GeneratedSQL = select_in(["Field1", "Field2"], "Table1", "MatchField", 4),
	?assertEqual(ExpectedSQL, GeneratedSQL).

select_in_param_pgsql_test() -> 
	%% Set db type to pgsql to check pgsql-style (dollarn).
	set_db_type(pgsql),
	ExpectedSQL = "SELECT Field1,Field2 FROM Table1 WHERE MatchField IN ($1,$2,$3,$4)",
	GeneratedSQL = select_in(["Field1", "Field2"], "Table1", "MatchField", 4),
	?assertEqual(ExpectedSQL, GeneratedSQL).


%%
%% Value conversion
%%
-define(SQB, <<"'">>).

convert_values([]) -> [];
convert_values([H|T]) -> CH = convert_value(H), CT = convert_values(T), [CH|CT].

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

convert_value(V) when is_list(V) -> safe_value(V), "'" ++ V ++ "'"; %% strings are lists of ints
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
safe_value(Value) -> {match, _} = re:run(Value, ?SAFE_VALUE_RE), true.

safe_value_string_test() ->
	%% Good values
	?assertEqual(true, safe_value("ABCdef123_")).

safe_value_binary_test() ->
	%% Good values
	?assertEqual(true, safe_value(<<"ABCdef123_">>)).

safe_value_bad_values_test() ->
	%% Bad values
	BadValues = "`-=[]\;',./~!@#$%^&*()+{}|:\"<>?",
	safe_value_bad_values_test(BadValues).

safe_value_bad_values_test([]) -> ok;
safe_value_bad_values_test([H|T]) -> safe_value_bad_value_test([H]), safe_value_bad_values_test(T).

safe_value_bad_value_test(BadValue) ->
	?assertException(error, {badmatch, nomatch}, safe_value(BadValue)).


%% To check multiple values
safe_values([]) -> true;
safe_values([H|T]) -> safe_value(H), safe_values(T).

safe_values_test() ->
	safe_values(["toto", "mytable", "_Field_1", <<"binary">>]).

safe_values_bad_values_test() ->
	BadValuesSets = [
					 ["good", "`bad`", "ok"],
					 ["ok", "'bad'", "bad;"]],
	safe_values_bad_values_test(BadValuesSets).

safe_values_bad_values_test([]) -> ok;
safe_values_bad_values_test([H|T]) ->
	?assertException(error, {badmatch, nomatch}, safe_values(H)),
	safe_values_bad_values_test(T).



%% Prepared statement parameter strings.
%% They're DB-specific so we need to check the DB type.
%% Returns a list of strings to use in the generated SQL.
parameter_strings(NumValues) when NumValues > 0 ->
	Mod = sqerl_client:drivermod(),
	Style = Mod:sql_parameter_style(),
	parameter_strings(Style, NumValues).

parameter_strings(Style, NumValues) when NumValues > 0 ->
	[parameter_string(Style, N) || N <- lists:seq(1, NumValues)].

%% parameter_strings tests
parameter_strings_mysql_test() ->
	set_db_type(mysql),
	?assertEqual(["?", "?", "?"], parameter_strings(3)).
	
parameter_strings_pgsql_test() ->
	set_db_type(pgsql),
	?assertEqual(["$1", "$2", "$3"], parameter_strings(3)).
		
parameter_strings_qmark_test() ->
	?assertEqual(["?", "?", "?"], parameter_strings(qmark, 3)),
	?assertEqual(["?", "?", "?", "?", "?"], parameter_strings(qmark, 5)).

parameter_strings_dollarn_test() ->
	?assertEqual(["$1", "$2", "$3"], parameter_strings(dollarn, 3)),
	?assertEqual(["$1", "$2", "$3", "$4", "$5", "$6"], parameter_strings(dollarn, 6)).

parameter_strings_le0_test() ->
	% NumValues <= 0 should throw an error
	?assertException(error, function_clause, parameter_strings(qmark, 0)),
	?assertException(error, function_clause, parameter_strings(qmark, -2)).

%% Logic for one parameter
parameter_string(qmark, _N)   -> "?";
parameter_string(dollarn, N) -> "$" ++ integer_to_list(N).

parameter_string_qmark_test() ->
    %% For qmark style, it should always be a qmark
	?assertEqual("?", parameter_string(qmark, 1)),
	?assertEqual("?", parameter_string(qmark, 2)),
	?assertEqual("?", parameter_string(qmark, 120)).

parameter_string_dollarn_test() ->
    %% For dollarn style it should be "$n"
	?assertEqual("$1", parameter_string(dollarn, 1)),
	?assertEqual("$2", parameter_string(dollarn, 2)),
	?assertEqual("$5", parameter_string(dollarn, 5)),
	?assertEqual("$120", parameter_string(dollarn, 120)).

parameter_string_bad_style_test() ->
	%% Anything else should cause an error
	?assertException(error, function_clause, parameter_string(unsupported, 1)),
	?assertException(error, function_clause, parameter_string(toto, 4)).



%% Just to see if eunit test is setup properly...
eunit_check_test() -> ?assertEqual(ok, ok).

%% Utilities
set_db_type(DBType) ->
	application:set_env(sqerl, db_type, DBType).

