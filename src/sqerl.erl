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
    pooler:take_member("sqerl").

checkin(Connection) ->
    pooler:return_member(Connection).

with_db(Call) ->
    with_db(Call, ?MAX_RETRIES).

with_db(_Call, 0) ->
    {error, no_connections};
with_db(Call, Retries) ->
    case pooler:take_member("sqerl") of
        error_no_members ->
            {error, no_members};
        error_no_pool ->
            {error, {no_pool, "sqerl"}};
        Cn when is_pid(Cn) ->
            %% We don't need a try/catch around Call(Cn) because pooler links both the
            %% connection and the process that has the connection checked out (this
            %% process). So a crash here will not leak a connection.
            case Call(Cn) of
                {error, closed} ->
                    with_db(Call, Retries - 1);
                Result ->
                    pooler:return_member(Cn),
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
            parse_error(Reason)
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


%% @doc Utility for generating specific message tuples from database-specific error
%% messages.  The 1-argument form determines which database is being used by querying
%% Sqerl's configuration at runtime, while the 2-argument form takes the database type as a
%% parameter directly.
-spec parse_error(
        string() |                       %% MySQL error
        {error, {error, error, _, _, _}} %% PostgreSQL error
       ) -> {'conflict',_} |
            {'foreign_key',_} |
            {'error',_}.
parse_error(Reason) ->
    {ok, DbType} = application:get_env(sqerl, db_type),
    parse_error(DbType, Reason).

-spec parse_error(mysql | pgsql, string() | {error, {error, error, _, _, _}}) ->
                         {'conflict',_} | {'foreign_key',_} | {'error',_}.
parse_error(mysql, Reason) ->
    case string:str(Reason, "Duplicate entry") of
        0 ->
            case string:str(Reason, "Cannot delete or update a parent row") of
                0 ->
                    case string:str(Reason, "a foreign key constraint fails") of
                        0 -> {error, Reason};
                        _ -> {foreign_key, Reason}
                    end;
                _ ->
                    {foreign_key, Reason}
            end;
        _ ->
            {conflict, Reason}
    end;
parse_error(pgsql, {error,                      % error from sqerl
                    {error,                     % error record marker from epgsql
                     error,                     % Severity
                     Code, Message, _Extra}}) ->
    %% See http://www.postgresql.org/docs/current/static/errcodes-appendix.html
    case Code of
        <<"23505">> -> % unique constraint violation
            {conflict, Message};
        <<"23503">> -> % foreign_key_violation
            {foreign_key, Message};
        _ ->
            {error, Message}
    end.

