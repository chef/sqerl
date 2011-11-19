%% -*- erlang-indent-level: 4;indent-tabs-mode: nil; fill-column: 92 -*-
%% ex: ts=4 sw=4 et
%% @author Seth Falcon <seth@opscode.com>
%% @author Mark Anderson <mark@opscode.com>
%% @copyright 2011 Opscode Inc.

-module(sqerl).

-export([checkout/0,
         checkin/1,
         with_db/1,
         select/2,
         select/3,
         select/4,
         statement/2,
         statement/3,
         statement/4]).

-include_lib("eunit/include/eunit.hrl").

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

select(StmtName, StmtArgs) ->
    select(StmtName, StmtArgs, identity, []).

select(StmtName, StmtArgs, {XformName, XformArgs}) ->
    select(StmtName, StmtArgs, XformName, XformArgs);
select(StmtName, StmtArgs, XformName) ->
    select(StmtName, StmtArgs, XformName, []).

select(StmtName, StmtArgs, XformName, XformArgs) ->
    execute_statement(StmtName, StmtArgs, XformName, XformArgs, exec_prepared_select).

statement(StmtName, StmtArgs) ->
    statement(StmtName, StmtArgs, identity, []).

statement(StmtName, StmtArgs, XformName) ->
    statement(StmtName, StmtArgs, XformName, []).

statement(StmtName, StmtArgs, XformName, XformArgs) ->
    execute_statement(StmtName, StmtArgs, XformName, XformArgs, exec_prepared_statement).

execute_statement(StmtName, StmtArgs, XformName, XformArgs, Executor) ->
    Xformer = erlang:apply(sqerl_transformers, XformName, XformArgs),
    F = fun(Cn) ->
                case sqerl_client:Executor(Cn, StmtName, StmtArgs) of
                    {ok, Results} ->
                        Xformer(Results);
                    Error ->
                        Error
                end end,
    with_db(F).

