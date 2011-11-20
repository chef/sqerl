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
         first_as_record/2,
         identity/0]).

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
    {ok, [ proplists:get_value(atom_to_binary(Field, utf8), Row) || Row <- Results ]}.

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

count(none) ->
    {ok, 0};
count(Count) ->
    {ok, Count}.

