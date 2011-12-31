%% -*- erlang-indent-level: 4;indent-tabs-mode: nil; fill-column: 92 -*-
%% ex: ts=4 sw=4 et
%% @author Kevin Smith <kevin@opscode.com>
%% @author Christopher Maier <cm@opscode.com>
%% @copyright Copyright 2011 Opscode, Inc.
%% @end
%% @doc Abstraction around interacting with database results

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
         by_column_name/2]).

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
    {ok, Result}.

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

parse_timestamp_to_datetime(TS) when is_binary(TS) ->
    {match, [_, Year,Month,Day,Hour,Min,Sec]} =
        re:run(TS, "^(\\d+)-(\\d+)-(\\d+)\s(\\d+):(\\d+):(\\d+)",
               [{capture, all, binary}]),
    {{b2i(Year),b2i(Month),b2i(Day)}, {b2i(Hour),b2i(Min),b2i(Sec)}}.

convert_YMDHMS_tuple_to_datetime({{Y,Mo,D}, {H,Mi,S}}) ->
    {datetime, {{Y,Mo,D}, {H,Mi,trunc(S)}}}.

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
        Transform ->
            {Name, Transform(Data)}
    end.

by_column_name(Rows, undefined) ->
    Rows;
by_column_name(Rows, Transforms) ->
    [[ single_column(E,Transforms) || E <- Row ] || Row <- Rows].
