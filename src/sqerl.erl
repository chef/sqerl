%% -*- erlang-indent-level: 4;indent-tabs-mode: nil; fill-column: 92 -*-
%% ex: ts=4 sw=4 et
%% @author Seth Falcon <seth@opscode.com>
%% @author Mark Anderson <mark@opscode.com>
%% @copyright 2011 Opscode Inc.

-module(sqerl).

-export([checkout/0,
         checkin/1,
         with_db/1]).

checkout() ->
    poolboy:checkout(sqerl).

checkin(Connection) ->
    poolboy:checkin(sqerl, Connection).

with_db(Call) ->
    case poolboy:checkout(sqerl) of
        {error, timeout} ->
            {error, timeout};
        Cn when is_pid(Cn) ->
            Result = Call(Cn),
            poolboy:checkin(sqerl, Cn),
            Result
    end.
