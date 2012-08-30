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

-export([select/3]).

-ifdef(TEST).
-compile([export_all]).
-endif.

-include_lib("eunit/include/eunit.hrl").

%% @doc Generates SELECT SQL depending on Where form.
%% Validates that parameters are safe.
%%
%% Where = {Field, in, NumValues, ParamStyle}
%% Generates SELECT ... IN SQL with parameter strings (not values),
%% which can be prepared and executed.
%%
%% e.g. SELECT Field1 FROM Table WHERE MatchField IN (?, ?, ?, ...)
%%
%% ParamStyle is qmark (?, ?, ... for e.g. mysql) 
%% or dollarn ($1, $2, etc. for e.g. pgsql)
%%
-spec select([binary()], binary(),
             {binary(), in, integer(), qmark | dollarn}) -> binary().
select(Columns, Table, {Field, in, NumValues, ParamStyle})
  when is_integer(NumValues), NumValues > 0 ->
    ensure_safe([Columns, Table, Field]),
    ColumnsPart = join(Columns, <<",">>),
    InPart = [<<"(">>,
              join(placeholders(NumValues, ParamStyle), <<",">>),
              <<")">>],
    Parts = [<<"SELECT">>, 
             ColumnsPart, 
             <<"FROM">>, 
             Table, 
             <<"WHERE">>, 
             Field, 
             <<"IN">>, 
             InPart],
    Query = join(Parts, <<" ">>),
    list_to_binary(lists:flatten(Query)).


%%
%% SQL Value Safety
%% Uses RE so can take string and binary as input
%%

-define(SAFE_VALUE_RE, <<"^[A-Za-z0-9_]*$">>).

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
-spec placeholders(integer(), atom()) -> [binary()].
placeholders(NumValues, Style) when NumValues > 0 ->
    [placeholder(N, Style) || N <- lists:seq(1, NumValues)].

%% @doc Returns a parameter string for the given style
%% at position N in the statement.
-spec placeholder(integer(), qmark | dollarn) -> binary().
placeholder(_N, qmark) ->
    <<"?">>;
placeholder(N, dollarn) ->
    list_to_binary("$" ++ integer_to_list(N)).


%%
%% Utilities
%%

%% @doc Join elements of list with Sep
%%
%% 1> join([1,2,3], 0).
%% [1,0,2,0,3]
%%
join([], _Sep) ->
    [];
join([H|T], Sep) ->
    [H] ++ lists:append([[Sep] ++ [X] || X <- T]).

