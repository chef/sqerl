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
         insert/4]).

-ifdef(TEST).
-compile([export_all]).
-endif.

-include_lib("eunit/include/eunit.hrl").

%% @doc Generates SELECT SQL depending on Where form.
%% SQL generated uses parameter place holders so query can be
%% prepared.
%%
%% Note: Validates that parameters are safe.
%%
%% Where = all
%% Does not generate a WHERE clause. Returns all records in table.
%%
%% 1> select([<<"*">>], <<"users">>, all).
%% <<"SELECT * FROM users">>
%%
%% Where = {Field, equals, ParamStyle}
%% Generates SELECT ... WHERE Field = ?
%%
%% 1> select([<<"name">>], <<"users">>, {<<"id">>, equals, qmark}).
%% <<"SELECT name FROM users WHERE id = ?">>
%%
%% Where = {Field, in, NumValues, ParamStyle}
%% Generates SELECT ... IN SQL with parameter strings (not values),
%% which can be prepared and executed.
%%
%% 1> select([<<"name">>], <<"users">>, {<<"id">>, in, [1,2,3], qmark}).
%% <<"SELECT name FROM users WHERE id IN (?, ?, ?)">>
%%
%% ParamStyle is qmark (?, ?, ... for e.g. mysql) 
%% or dollarn ($1, $2, etc. for e.g. pgsql)
%%
-spec select([binary()], binary(), term(), atom()) -> binary().
select(Columns, Table, all, _ParamStyle) ->
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


%% @doc Generate DELETE statement
%%
%% See select/3 for details about the "Where" parameter.
%%
%% 1> delete(<<"users">>, {<<"id">>, equals, qmark}).
%% <<"DELETE FROM users WHERE id = ?">>
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


%% @doc Generate INSERT statement.
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
%% See select/3 for more details.
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

where_unary(Op, Where, ParamStyle, ParamPosOffset) ->
    {WhereSub, Values} = where_parts(Where, ParamStyle, ParamPosOffset),
    {[Op, <<" (">>, WhereSub, <<")">>], Values}.

where_binary(Field, Op, Value, ParamStyle, ParamPosOffset) ->
    ensure_safe([Field]),
    {[Field, <<" ">>, Op, <<" ">>, placeholder(1 + ParamPosOffset, ParamStyle)], [Value]}.

where_logic(Op, WhereList, ParamStyle, ParamPosOffset)
  when length(WhereList) > 1 ->
    {WhereSubs, Values} = where_subs(WhereList, ParamStyle, ParamPosOffset),
    Parts = [<<"(">>,
             join(WhereSubs, <<" ", Op/binary, " ">>),
             <<")">>],
    {Parts, Values}.

where_values(Field, Op, Values, ParamStyle, ParamPosOffset)
  when length(Values) > 0 ->
    ensure_safe([Field]),
    PlaceHolders = join(placeholders(length(Values), ParamPosOffset, ParamStyle), <<",">>),
    {[Field, <<" ">>, Op, <<" (">>, PlaceHolders, <<")">>], Values}.


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
