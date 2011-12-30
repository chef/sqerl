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

-define(MAX_RETRIES, 5).

checkout() ->
    poolboy:checkout(sqerl).

checkin(Connection) ->
    poolboy:checkin(sqerl, Connection).

with_db(Call) ->
    with_db(Call, ?MAX_RETRIES).

with_db(_Call, 0) ->
    {error, no_connections};
with_db(Call, Retries) ->
    case poolboy:checkout(sqerl) of
        {error, timeout} ->
            {error, timeout};
        Cn when is_pid(Cn) ->
            case Call(Cn) of
                {error, closed} ->
                    with_db(Call, Retries - 1);
                Result ->
                    poolboy:checkin(sqerl, Cn),
                    Result
            end
    end.

select(StmtName, StmtArgs) ->
    select(StmtName, StmtArgs, identity, []).

select(StmtName, StmtArgs, {XformName, XformArgs}) ->
    select(StmtName, StmtArgs, XformName, XformArgs);
select(StmtName, StmtArgs, XformName) ->
    select(StmtName, StmtArgs, XformName, []).

select(StmtName, StmtArgs, XformName, XformArgs) ->
    case execute_statement(StmtName, StmtArgs, XformName, XformArgs,
                           exec_prepared_select) of
        {ok, []} ->
            {ok, none};
        {ok, Results} ->
            {ok, Results};
        {error, Reason} ->
            {error, Reason}
    end.

statement(StmtName, StmtArgs) ->
    statement(StmtName, StmtArgs, identity, []).

statement(StmtName, StmtArgs, XformName) ->
    statement(StmtName, StmtArgs, XformName, []).

statement(StmtName, StmtArgs, XformName, XformArgs) ->
    case execute_statement(StmtName, StmtArgs, XformName, XformArgs,
                           exec_prepared_statement) of
        {ok, 0} ->
            {ok, none};
        {ok, N} when is_number(N) ->
            {ok, N};
        {error, Reason} ->
            {error, Reason}
    end.

execute_statement(StmtName, StmtArgs, XformName, XformArgs, Executor) ->
    Xformer = erlang:apply(sqerl_transformers, XformName, XformArgs),
    F = fun(Cn) ->
                case sqerl_client:Executor(Cn, StmtName, StmtArgs) of
                    {ok, Results} ->
                        Xformer(Results);
                    {error, closed} ->
                        sqerl_client:close(Cn),
                        {error, closed};
                    Error ->
                        Error
                end end,
    with_db(F).
