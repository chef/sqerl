%% -*- erlang-indent-level: 4;indent-tabs-mode: nil; fill-column: 92-*-
%% ex: ts=4 sw=4 et
%% @author Jean-Philippe Langlois <jpl@opscode.com>
%% @doc SQL generation library
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

-export([select/4,
         delete/3,
         insert/4,
         update/4]).

-ifdef(TEST).
-compile([export_all]).
-endif.

-include_lib("eunit/include/eunit.hrl").

%% @doc Generates SELECT SQL depending on Where form.
%% Returns {SQL, ParameterValues} which can be passed on to be executed.
%% SQL generated uses parameter place holders so query can be
%% prepared.
%%
%% Note: Validates that parameters are safe.
%%
%% Where = all
%% Does not generate a WHERE clause. Matches all records in table.
%%
%% 1> select([<<"*">>], <<"users">>, all, qmark).
%% {<<"SELECT * FROM users">>, []}
%%
%% Where = {Field, equals, Values}
%% Generates SELECT ... WHERE Field = ?
%%
%% 1> select([<<"name">>], <<"users">>, {<<"id">>, equals, 1}, qmark).
%% {<<"SELECT name FROM users WHERE id = ?">>, [1]}
%%
%% Where = {Field, in, Values}
%% Generates SELECT ... IN SQL with parameter strings (not values),
%% which can be prepared and executed.
%%
%% 1> select([<<"name">>], <<"users">>, {<<"id">>, in, [1,2,3]}, qmark).
%% {<<"SELECT name FROM users WHERE id IN (?, ?, ?)">>, [1,2,3]}
%%
%% ParamStyle is qmark (?, ?, ... for e.g. mysql) 
%% or dollarn ($1, $2, etc. for e.g. pgsql)
%%
-spec select([binary()], binary(), term(), atom()) -> binary().
select(Columns, Table, all, _ParamStyle) ->
    %% "all" is an exception because we don't generate a where clause at all.
    ensure_safe([Columns, Table]),
    Parts = [<<"SELECT">>, 
             column_parts(Columns),
             <<"FROM">>, 
             Table],
    SQL = list_to_binary(join(Parts, <<" ">>)),
    {SQL, []};
select(Columns, Table, Where, ParamStyle) ->
    ensure_safe([Columns, Table]),
    {WhereSQL, Values} = where_parts(Where, ParamStyle),
    Parts = [<<"SELECT">>, 
             column_parts(Columns),
             <<"FROM">>, 
             Table,
             <<"WHERE">>,
             WhereSQL],
    SQL = list_to_binary(join(Parts, <<" ">>)),
    {SQL, Values}.


%% @doc Generate UPDATE statement
%% Update is given under the form of a Row (proplist).
%% Uses the same Where specifications as select/4 except for "all" which is 
%% not supported.
%%
%% 1> update(<<"users">>, [{<<"last_name">>, <<"Toto">>}], {<<"id">>, equals, 1}, qmark).
%% {<<"UPDATE users SET last_name = ? WHERE id = ?">>, [<<"Toto">>, 1]}
%%
-spec update(binary(), list(), any(), atom()) -> {binary(), list()}.
update(Table, Row, Where, ParamStyle) ->
    ensure_safe(Table),
    {SetSQL, SetValues} = set_parts(Row, ParamStyle),
    {WhereSQL, WhereValues} = where_parts(Where, ParamStyle, length(SetValues)),
    Parts = [<<"UPDATE">>,
             Table,
             <<"SET">>,
             SetSQL,
             <<"WHERE">>,
             WhereSQL],
    SQL = list_to_binary(join(Parts, <<" ">>)),
    {SQL, SetValues ++ WhereValues}.

%% @doc Generate set parts of update query.
set_parts(Row, ParamStyle) when length(Row) > 0 ->
    set_parts(Row, ParamStyle, 0).

set_parts(Row, ParamStyle, ParamPosOffset) ->
    set_parts(Row, ParamStyle, ParamPosOffset, [], []).

set_parts([], _ParamStyle, _ParamPosOffset, PartsAcc, ValuesAcc) ->
    {join(lists:reverse(PartsAcc), <<", ">>), lists:reverse(ValuesAcc)};

set_parts([{Field, Value}|T], ParamStyle, ParamPosOffset, PartsAcc, ValuesAcc) ->
    ensure_safe(Field),
    Parts = [Field, <<" = ">>, placeholder(1 + ParamPosOffset, ParamStyle)],
    set_parts(T, ParamStyle, ParamPosOffset + 1, [Parts|PartsAcc], [Value|ValuesAcc]).


%% @doc Generate DELETE statement
%% Uses the same Where specifications as select/4.
%% See select/4 for details about the "Where" parameter.
%%
%% 1> delete(<<"users">>, {<<"id">>, equals, 1}, qmark).
%% {<<"DELETE FROM users WHERE id = ?">>, [1]}
%%
-spec delete(binary(), any(), atom()) -> binary().
delete(Table, Where, ParamStyle) ->
    ensure_safe(Table),
    {WhereSQL, Values} = where_parts(Where, ParamStyle),
    Parts = [<<"DELETE FROM">>,
             Table,
             <<"WHERE">>,
             WhereSQL],
    SQL = list_to_binary(join(Parts, <<" ">>)),
    {SQL, Values}.


%% @doc Generate INSERT statement for N rows.
%%
%% 1> insert(<<"users">>, [<<"id">>, <<"name">>], 3, qmark).
%% <<"INSERT INTO users (name) VALUES (?,?),(?,?),(?,?)">>
%%
-spec insert(binary(), [binary()], integer(), atom()) -> binary().
insert(Table, Columns, NumRows, ParamStyle) when
    NumRows > 0,
    length(Columns) > 0 ->
    ensure_safe([Table, Columns]),
    Parts = [<<"INSERT INTO">>,
             Table,
             <<"(">>,
             column_parts(Columns),
             <<")">>,
             <<"VALUES">>,
             values_parts(length(Columns), NumRows, ParamStyle)],
    Query = join(Parts, <<" ">>),
    list_to_binary(lists:flatten(Query)).

%% @doc Generate values parts of insert query
-spec values_parts(integer(), integer(), atom()) -> [binary()].
values_parts(NumColumns, NumRows, ParamStyle) ->
    values_parts(NumColumns, NumRows, ParamStyle, 0).

%% @doc Generate values parts with parameter placeholders offset.
%% Offset is useful for parameter placeholder styles that use numbers
%% e.g. dollarn for e.g. postgresql.
-spec values_parts(integer(), integer(), atom(), integer()) -> [binary()].
values_parts(NumColumns, NumRows, ParamStyle, Offset) ->
    ValuesPartF = fun(RowIndex) -> values_part(NumColumns, RowIndex * NumColumns + Offset, ParamStyle) end,
    Parts = [ValuesPartF(RowIndex) || RowIndex <- lists:seq(0, NumRows-1)],
    join(Parts, <<",">>).

%% @doc Generates values part for one row
-spec values_part(integer(), integer(), atom()) -> [binary()].
values_part(NumColumns, Offset, ParamStyle) ->
    [<<"(">>]
    ++ join(placeholders(NumColumns, Offset, ParamStyle), <<",">>)
    ++ [<<")">>].

%% @doc Generate columns parts of query
%% (Just joins them with comma).
column_parts(Columns) -> join(Columns, <<",">>).

%% @doc Generate "WHERE" parts of query.
%% See select/4 for more details.
where_parts(Where, ParamStyle) ->
    where_parts(Where, ParamStyle, 0).

where_parts({'not', Where}, ParamStyle, ParamPosOffset) ->
    where_unary(<<"NOT">>, Where, ParamStyle, ParamPosOffset);
where_parts({Field, equals, Value}, ParamStyle, ParamPosOffset) ->
    where_binary(Field, <<"=">>, Value, ParamStyle, ParamPosOffset);
where_parts({Field, nequals, Value}, ParamStyle, ParamPosOffset) ->
    where_binary(Field, <<"!=">>, Value, ParamStyle, ParamPosOffset);
where_parts({Field, gt, Value}, ParamStyle, ParamPosOffset) ->
    where_binary(Field, <<">">>, Value, ParamStyle, ParamPosOffset);
where_parts({Field, gte, Value}, ParamStyle, ParamPosOffset) ->
    where_binary(Field, <<">=">>, Value, ParamStyle, ParamPosOffset);
where_parts({Field, lt, Value}, ParamStyle, ParamPosOffset) ->
    where_binary(Field, <<"<">>, Value, ParamStyle, ParamPosOffset);
where_parts({Field, lte, Value}, ParamStyle, ParamPosOffset) ->
    where_binary(Field, <<"<=">>, Value, ParamStyle, ParamPosOffset);
where_parts({Field, in, Values}, ParamStyle, ParamPosOffset) ->
  where_values(Field, <<"IN">>, Values, ParamStyle, ParamPosOffset);
where_parts({Field, notin, Values}, ParamStyle, ParamPosOffset) ->
  where_values(Field, <<"NOT IN">>, Values, ParamStyle, ParamPosOffset);
where_parts({'and', WhereList}, ParamStyle, ParamPosOffset) ->
    where_logic(<<"AND">>, WhereList, ParamStyle, ParamPosOffset);
where_parts({'or', WhereList}, ParamStyle, ParamPosOffset) ->
    where_logic(<<"OR">>, WhereList, ParamStyle, ParamPosOffset).

%% @doc Generate where part for a unary operator
%% e.g. NOT (...)
where_unary(Op, Where, ParamStyle, ParamPosOffset) ->
    {WhereSub, Values} = where_parts(Where, ParamStyle, ParamPosOffset),
    {[Op, <<" (">>, WhereSub, <<")">>], Values}.

%% @doc Generate where part for a binary operator
%% e.g. Field = Value
where_binary(Field, Op, Value, ParamStyle, ParamPosOffset) ->
    ensure_safe([Field]),
    {[Field, <<" ">>, Op, <<" ">>, placeholder(1 + ParamPosOffset, ParamStyle)], [Value]}.

%% @doc Generate where part for a logic operator
%% e.g. Where1 AND Where2 AND Where3
where_logic(Op, WhereList, ParamStyle, ParamPosOffset)
  when length(WhereList) > 1 ->
    {WhereSubs, Values} = where_subs(WhereList, ParamStyle, ParamPosOffset),
    Parts = [<<"(">>,
             join(WhereSubs, <<" ", Op/binary, " ">>),
             <<")">>],
    {Parts, Values}.

%% @doc Generate where part for a values operator
%% e.g. Field IN (Value1, Value2, ...)
where_values(Field, Op, Values, ParamStyle, ParamPosOffset)
  when length(Values) > 0 ->
    ensure_safe([Field]),
    PlaceHolders = join(placeholders(length(Values), ParamPosOffset, ParamStyle), <<",">>),
    {[Field, <<" ">>, Op, <<" (">>, PlaceHolders, <<")">>], Values}.

%% @doc Generate where parts for a where list.
%% Results can be 'joined' with the intended operator.
where_subs(WhereList, ParamStyle, ParamPosOffset) when length(WhereList) > 1 ->
    where_subs(WhereList, ParamStyle, ParamPosOffset, [], []).

where_subs([], _ParamStyle, _ParamOffset, WherePartsAcc, ValuesAcc) ->
    {lists:reverse(WherePartsAcc), lists:reverse(ValuesAcc)};

where_subs([Where|WhereList], ParamStyle, ParamPosOffset, WherePartsAcc, ValuesAcc) ->
    {WherePart, Values} = where_parts(Where, ParamStyle, ParamPosOffset),
    ParamPosOffset2 = ParamPosOffset + length(Values),
    where_subs(WhereList, ParamStyle, ParamPosOffset2,
               [WherePart|WherePartsAcc],
               lists:reverse(Values) ++ ValuesAcc).

%%
%% SQL Value Safety
%% Uses RE so can take string and binary as input
%%

-define(SAFE_VALUE_RE, <<"^[A-Za-z0-9_\*]*$">>).

%% @doc Checks that value(s) is(are) safe to use while generating SQL.
%% Walks io lists.
-spec ensure_safe(binary()|string()) -> true.
ensure_safe(Value) when is_binary(Value) ->
    {match, _} = re:run(Value, ?SAFE_VALUE_RE), 
    true;
ensure_safe([H|T]) when is_integer(H) ->
    %% string
    {match, _} = re:run([H|T], ?SAFE_VALUE_RE), 
    true;
ensure_safe([H]) -> 
    ensure_safe(H);
ensure_safe([H|T]) ->
    ensure_safe(H),
    ensure_safe(T).


%%
%% Parameter placeholders
%%

%% @doc Generates a list of placeholders with the
%% given parameter style.
%%
%% 1> placeholders(3, qmark).
%% [<<"?">>,<<"?">>,<<"?">>]
%%
%% 1> placeholders(3, dollarn).
%% [<<"$1">>,<<"$2">>,<<"$3">>]
%%
%% The 3-argument version takes an offset argument.
%%
%% 1> placeholders(3, 4, dollarn).
%% [<<"$5">>,<<"$6">>,<<"$7">>]
%%
-spec placeholders(integer(), integer(), atom()) -> [binary()].
placeholders(NumValues, Offset, Style) when NumValues > 0 ->
    [placeholder(N + Offset, Style) || N <- lists:seq(1, NumValues)].

%% @doc Returns a parameter string for the given style
%% at position N in the statement.
-spec placeholder(integer(), qmark | dollarn) -> binary().
placeholder(_Pos, qmark) ->
    <<"?">>;
placeholder(Pos, dollarn) ->
    list_to_binary([<<"$">>, integer_to_list(Pos)]).


%%
%% Utilities
%%

%% @doc Join elements of list with Sep
%%
%% 1> join([1,2,3], 0).
%% [1,0,2,0,3]
%%
%%join([], _Sep) -> [];
%%join([H|T], Sep) -> [H] ++ lists:append([[Sep] ++ [X] || X <- T]).

join([], _Sep) -> [];
join(L, Sep) -> join(L, Sep, []).

join([H], _Sep, Acc)  -> lists:reverse([H|Acc]);
join([H|T], Sep, Acc) -> join(T, Sep, [Sep, H|Acc]).
