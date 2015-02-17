%% -*- erlang-indent-level: 4;indent-tabs-mode: nil; fill-column: 92 -*-
%% ex: ts=4 sw=4 et
%% @author Kevin Smith <kevin@opscode.com>
%% @author Christopher Maier <cm@opscode.com>
%% @doc Abstraction around interacting with database results
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


-module(sqerl_transformers).
-export([rows/0,
         rows_as_records/2,
         rows_as_scalars/1,
         count/0,
         first/0,
         first_as_scalar/1,
         first_as_record/2,
         identity/0,
         parse_timestamp_to_datetime/1,
         convert_YMDHMS_tuple_to_datetime/1,
         convert_integer_to_boolean/1,
         by_column_name/2]).

-ifdef(TEST).
-compile([export_all]).
-endif.

% reference libpsql - this 'magic number' is the fixed value of
% 'infinity' for postgres - below from libpq/timestamp.h. Note that
% there will be some incompatibility here if run against a build that does not
% have INT64 timestamp support.
% #ifdef HAVE_INT64_TIMESTAMP
% #define DT_NOEND	(INT64CONST(0x7fffffffffffffff))
-define(INFINITY_TIMESTAMP, {{294277,1,9},{4,0,54.775807}}).

-include_lib("eunit/include/eunit.hrl").

rows() ->
    fun(Rows) -> rows(Rows) end.

rows_as_records(RecName, RecInfo) ->
    fun(Result) -> rows_as_records(RecName, RecInfo, Result) end.

rows_as_scalars(Field) ->
    fun (Results) -> rows_as_scalars(Field, Results) end.

count() ->
    fun(Result) -> count(Result) end.

first() ->
    fun(Rows) -> first(Rows) end.

first_as_scalar(Field) ->
    fun(Results) ->
            first_as_scalar(Field, Results) end.

first_as_record(RecName, RecInfo) ->
    fun(Result) -> first_as_record(RecName, RecInfo, Result) end.

identity() ->
    fun(Result) -> {ok, Result} end.

%% Internal functions

rows(none) ->
    {ok, none};
rows([]) ->
    {ok, none};
rows(Result) ->
    {ok, [ [ Val || {_Key, Val} <- Row] || Row <- Result ] }.

rows_as_records(_RecName, _RecordInfo, []) ->
    {ok, none};
rows_as_records(_RecName, _RecordInfo, none) ->
    {ok, none};
rows_as_records(RecName, RecordInfo, Rows) ->
    {ok, [ begin
               Vals = [ proplists:get_value(erlang:atom_to_binary(Field, utf8), Row)
                        || Field <- RecordInfo ],
               list_to_tuple([RecName | Vals])
           end || Row <- Rows ]}.

rows_as_scalars(_Field, none) ->
    {ok, none};
rows_as_scalars(_Field, []) ->
    {ok, none};
rows_as_scalars(Field, Results) ->
    FieldBin = atom_to_binary(Field, utf8),
    {ok, [ proplists:get_value(FieldBin, Row) || Row <- Results ]}.

first_as_scalar(_Field, []) ->
    {ok, none};
first_as_scalar(Field, Results) ->
    {ok, [H|_]} = rows_as_scalars(Field, Results),
    {ok, H}.

first(none) ->
    {ok, none};
first([]) ->
    {ok, none};
first([H|_]) ->
    {ok, H}.

first_as_record(_RecName, _RecInfo, none) ->
    {ok, none};
first_as_record(_RecName, _RecInfo, []) ->
    {ok, none};
first_as_record(RecName, RecInfo, [H|_]) ->
    {ok, [First|_]} = rows_as_records(RecName, RecInfo, [H]),
    {ok, First}.

count(Count) ->
    {ok, Count}.

b2i(X) when is_binary(X) ->
    list_to_integer(binary_to_list(X)).

parse_timestamp_to_datetime(<<"infinity">>) ->
    ?INFINITY_TIMESTAMP;
parse_timestamp_to_datetime(TS) when is_binary(TS) ->
    {match, [_, Year,Month,Day,Hour,Min,Sec]} =
        re:run(TS, "^(\\d+)-(\\d+)-(\\d+)\s(\\d+):(\\d+):(\\d+)",
               [{capture, all, binary}]),
    {{b2i(Year),b2i(Month),b2i(Day)}, {b2i(Hour),b2i(Min),b2i(Sec)}}.

convert_YMDHMS_tuple_to_datetime(?INFINITY_TIMESTAMP) ->
    {datetime, <<"infinity">>};
convert_YMDHMS_tuple_to_datetime({{Y,Mo,D}, {H,Mi,S}}) ->
    {datetime, {{Y,Mo,D}, {H,Mi,trunc(S)}}}.

-spec convert_integer_to_boolean(non_neg_integer()) -> boolean().
%% @doc helper column transformer for mysql where booleans are represented as
%% tinyint(1) and can take on the value of 0 or 1.
convert_integer_to_boolean(0) ->
    false;
convert_integer_to_boolean(N) when is_integer(N), N > 0 ->
    true.

%% single_column({Name, Data}, Transforms) when is_record(Transforms,dict, 9) ->
%%     case dict:find(Name, Transforms) of
%%         {ok, Transform} ->
%%             {Name, Transform(Data)};
%%         error ->
%%             {Name, Data}
%%    end;
single_column({Name, Data}, Transforms) when is_list(Transforms) ->
    case proplists:get_value(Name, Transforms) of
        undefined ->
            {Name, Data};
        Fun when is_function(Fun) ->
            {Name, Fun(Data)};
        {Module, Fun} ->
            {Name, apply(Module, Fun, [Data])}
    end.

by_column_name(Rows, undefined) ->
    Rows;
by_column_name(Rows, Transforms) ->
    [[ single_column(E,Transforms) || E <- Row ] || Row <- Rows].
