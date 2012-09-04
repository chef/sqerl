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

-export([select/3,
         delete/2,
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
-spec select([binary()], binary(),
             all |
             {binary(), equals, qmark | dollarn} |
             {binary(), in, integer(), qmark | dollarn}) -> binary().
select(Columns, Table, all) ->
    ensure_safe([Columns, Table]),
    Parts = [<<"SELECT">>, 
             column_parts(Columns),
             <<"FROM">>, 
             Table],
    Query = join(Parts, <<" ">>),
    list_to_binary(lists:flatten(Query));
select(Columns, Table, Where) ->
    ensure_safe([Columns, Table]),
    Parts = [<<"SELECT">>, 
             column_parts(Columns),
             <<"FROM">>, 
             Table,
             <<"WHERE">>,
             where_parts(Where)],
    Query = join(Parts, <<" ">>),
    list_to_binary(Query).


%% @doc Generate DELETE statement
%%
%% See select/3 for details about the "Where" parameter.
%%
%% 1> delete(<<"users">>, {<<"id">>, equals, qmark}).
%% <<"DELETE FROM users WHERE id = ?">>
%%
-spec delete(binary(), any()) -> binary().
delete(Table, Where) ->
    ensure_safe(Table),
    Parts = [<<"DELETE FROM">>,
             Table,
             <<"WHERE">>,
             where_parts(Where)],
    Query = join(Parts, <<" ">>),
    list_to_binary(lists:flatten(Query)).


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
where_parts({Field, equals, ParamStyle}) ->
    ensure_safe([Field]),
    [Field, <<" = ">>, placeholder(1, ParamStyle)];
where_parts({Field, in, NumValues, ParamStyle})
  when is_integer(NumValues), NumValues > 0 ->
    ensure_safe([Field]),
    PlaceHolders = join(placeholders(NumValues, ParamStyle), <<",">>),
    [Field, <<" IN (">>, PlaceHolders,<<")">>].


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
-spec placeholders(integer(), atom()) -> [binary()].
placeholders(NumValues, Style) ->
    placeholders(NumValues, 0, Style).
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
