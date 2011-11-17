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
         count/0,
         first/0,
         first_as_record/2]).

rows() ->
    fun rows/1.

rows_as_records(RecName, RecInfo) ->
    fun(Result) -> rows_as_records(RecName, RecInfo, Result) end.

count() ->
    fun count/1.

first() ->
    fun first/1.

first_as_record(RecName, RecInfo) ->
    fun(Result) -> first_as_record(RecName, RecInfo, Result) end.

%% Internal functions

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

count(Count) ->
    {ok, Count}.

first([]) ->
    {ok, none};
first([H|_]) ->
    {ok, H}.

first_as_record(_RecName, _RecInfo, []) ->
    {ok, none};
first_as_record(RecName, RecInfo, [H|_]) ->
    {ok, [First|_]} = rows_as_records(RecName, RecInfo, [H]),
    {ok, First}.
