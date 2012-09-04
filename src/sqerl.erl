%% -*- erlang-indent-level: 4;indent-tabs-mode: nil; fill-column: 92 -*-
%% ex: ts=4 sw=4 et
%% @author Seth Falcon <seth@opscode.com>
%% @author Mark Anderson <mark@opscode.com>
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


-module(sqerl).

-export([checkout/0,
         checkin/1,
         with_db/1,
         select/2,
         select/3,
         select/4,
         statement/2,
         statement/3,
         statement/4,
         execute/1,
         execute/2,
         adhoc_select/3,
         adhoc_delete/2,
         adhoc_update/3,
         adhoc_insert/2,
         adhoc_insert/3,
         extract_insert_data/1]).

-include_lib("sqerl.hrl").

-define(MAX_RETRIES, 5).

%% See http://dev.mysql.com/doc/refman/5.0/en/error-messages-server.html
-define(MYSQL_ERROR_CODES, [{1062, conflict}, {1451, foreign_key}, {1452, foreign_key}]).
%% See http://www.postgresql.org/docs/current/static/errcodes-appendix.html
-define(PGSQL_ERROR_CODES, [{<<"23505">>, conflict}, {<<"23503">>, foreign_key}]).

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
            {error, no_connections};
        error_no_pool ->
            {error, {no_pool, "sqerl"}};
        Cn when is_pid(Cn) ->
            %% We don't need a try/catch around Call(Cn) because pooler links both the
            %% connection and the process that has the connection checked out (this
            %% process). So a crash here will not leak a connection.
            case Call(Cn) of
                {error, closed} ->
                    %% TODO: sqerl_client:close(Cn)?
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
            parse_error(Reason)
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


%% @doc Execute query or statement with no parameters
%% See execute/2 for return info.
-spec execute(binary() | string() | atom()) -> {ok, any()} | {error, any()}.
execute(QueryOrStatement) ->
    execute(QueryOrStatement, []).

%% @doc Execute query or statement with parameters.
%% Returns:
%% - {ok, Result}
%% - {error, ErrorInfo}
%%
%% Result depends on the query being executed, and can be
%% - Rows
%% - Count
%%
%% Row is a proplist, e.g. [{<<"id">>, 1}, {<<"name">>, <<"John">>}]
%%
-spec execute(binary() | string() | atom(), [any()]) -> {ok, any()} | {error, any()}.
execute(QueryOrStatement, Parameters) ->
    F = fun(Cn) -> sqerl_client:execute(Cn, QueryOrStatement, Parameters) end,
    with_connection(F).

%% @doc Execute an adhoc query: adhoc_select(Columns, Table, Where)
%% Returns Columns from Table for records matching Where specifications.
%% Note that input is validated for safe values.
%%
%% Where: all
%% Returns all records.
%%
%% Where: {Field, equals, Value}
%% SELECT ... Field = Value
%%
%% Where: {Field, in, Values}
%% SELECT ... Field IN (Values)
%%
%% Returns:
%% - {ok, Rows}
%% - {error, ErrorInfo}
%%
%% See execute/2 for more details on return data.
%%
-spec adhoc_select([binary()],
                   binary(),
                   {all} |
                   {binary(), equals, any()} |
                   {binary(), in, [any()]}) ->
    {ok, list()} | {error, any()}.
adhoc_select(Columns, Table, Where) ->
    {SQL, Values} = sqerl_adhoc:select(Columns, Table, Where, param_style()),
    %%error_logger:info_msg("~p <- ~p~n", [SQL, Values]),
    execute(SQL, Values).

%% @doc Adhoc delete.
%% Uses the same Where specifications as adhoc_select/3.
%% Returns {ok, Count} or {error, ErrorInfo}.
%%
-spec adhoc_delete(binary(), term()) -> {ok, integer()} | {error, any()}.
adhoc_delete(Table, Where) ->
    {SQL, Values} = sqerl_adhoc:delete(Table, Where, param_style()),
    execute(SQL, Values).

%% @doc Adhoc update.
%% Updates records matching Where specifications with
%% fields and values in given Row.
%% Uses the same Where specifications as adhoc_select/3.
%% Returns {ok, Count} or {error, ErrorInfo}.
%%
-spec adhoc_update(binary(), list(), term()) -> {ok, integer()} | {error, any()}.
adhoc_update(Table, Row, Where) ->
    {SQL, Values} = sqerl_adhoc:update(Table, Row, Where, param_style()),
    execute(SQL, Values).

%% @doc Insert records.
%%
%% Prepares a statement and call it repeatedly.
%% Inserts ?BULK_SIZE records at a time until
%% there are fewer records and it inserts
%% the rest at one time.
%%
%% - Columns, RowsValues
%%   e.g. {[<<"first_name">>, <<"last_name">>],
%%         [[<<"Joe">>, <<"Blow">>],
%%          [<<"John">>, <<"Doe">>]]}
%%
%% - Rows: list of proplists (such as returned by a select)
%%   e.g. [
%%         [{<<"id">>, 1},{<<"first_name">>, <<"Kevin">>}],
%%         [{<<"id">>, 2},{<<"first_name">>, <<"Mark">>}]
%%        ]
%% Returns {ok, Count}
%%
%% 1> adhoc_insert(<<"users">>,
%%        {[<<"first_name">>, <<"last_name">>],
%%         [[<<"Joe">>, <<"Blow">>],
%%          [<<"John">>, <<"Doe">>]]}).
%% {ok, 2}
%%
-define(BULK_SIZE, 10). %% small for now for test purposes
-define(ADHOC_INSERT_STMT_ATOM, '__adhoc_insert').

adhoc_insert(Table, Rows) ->
    %% reformat Rows to desired format
    {Columns, RowsValues} = extract_insert_data(Rows),
    adhoc_insert(Table, Columns, RowsValues).

adhoc_insert(Table, Columns, RowsValues) when length(RowsValues) < ?BULK_SIZE,
                                              length(Columns) > 0 ->
    %% Do one bulk insert since we have less than BULK_SIZE rows
    SQL = sqerl_adhoc:insert(Table, Columns, length(RowsValues), param_style()),
    execute(SQL, lists:flatten(RowsValues));

adhoc_insert(Table, Columns, RowsValues) when length(RowsValues) >= ?BULK_SIZE ->
    %% Prepare a bulk insert statement and execute as many times as needed.
    SQL = sqerl_adhoc:insert(Table, Columns, ?BULK_SIZE, param_style()),
    ok = prepare(?ADHOC_INSERT_STMT_ATOM, SQL),
    {ok, Count, RemainingRowsValues} = adhoc_prepared_insert(RowsValues),
    ok = unprepare(?ADHOC_INSERT_STMT_ATOM),
    {ok, RemainingCount} = adhoc_insert(Table, Columns, RemainingRowsValues),
    {ok, Count + RemainingCount}.

prepare(Name, SQL) ->
    F = fun(Cn) -> sqerl_client:prepare(Cn, Name, SQL) end,
    with_connection(F).

unprepare(Name) ->
    F = fun(Cn) -> sqerl_client:unprepare(Cn, Name) end,
    with_connection(F).

%% @doc Insert data with insert statement already prepared
adhoc_prepared_insert(RowsValues) ->
    adhoc_prepared_insert(RowsValues, 0).

adhoc_prepared_insert(RowsValues, CountSoFar) when length(RowsValues) >= ?BULK_SIZE ->
    {RowsValuesToInsert, Rest} = lists:split(?BULK_SIZE, RowsValues),
    {ok, Count} = statement(?ADHOC_INSERT_STMT_ATOM, lists:flatten(RowsValuesToInsert)),
    adhoc_prepared_insert(Rest, CountSoFar + Count);
adhoc_prepared_insert(RowsValues, CountSoFar) when length(RowsValues) < ?BULK_SIZE ->
    {ok, CountSoFar, RowsValues}.

%% @doc Extract insert data from Rows (list of proplists).
%% Assumes all rows have the same format.
%% Returns {Columns, RowsValues}.
%%
%% 1> extract_insert_data([
%%                         [{<<"id">>, 1}, {<<"name">>, <<"Joe">>}],
%%                         [{<<"id">>, 2}, {<<"name">>, <<"Jeff">>}],
%%                        ]).
%% {[<<"id">>,<<"name">>],[[1,<<"Joe">>],[2,<<"Jeff">>]]}
%%
-spec extract_insert_data([[{binary(), any()}]]) -> {[binary()], [[any()]]}.
extract_insert_data(Rows) ->
    FirstRow = lists:nth(1, Rows),
    Columns = [C || {C, _V} <- FirstRow],
    RowsValues = [[V || {_C, V} <- Row] || Row <- Rows],
    {Columns, RowsValues}.

%% @doc Shortcut for sqerl_client:parameter_style()
-spec param_style() -> atom().
param_style() -> sqerl_client:sql_parameter_style().


%% @doc Call function with a DB connection.
%% Function must take one DB connection argument.
%% It should return {ok, Results} or {error, ErrorInfo}.
%% Call will be retried where possible (e.g. DB connection was closed).
%% Returns {ok, Results} or {error, ErrorInfo}.
%% ErrorInfo is no_connection if all attempts have failed, or whatever
%% is provided by the DB client.
%%
%% TODO: yes, this is almost the same as with_db. Both will be merged
%% down the road after clarifying some things...
with_connection(F) ->
    with_connection(F, ?MAX_RETRIES).
with_connection(_F, 0) ->
    {error, no_connection};
with_connection(F, Retries) ->
    case checkout() of
        Cn when is_pid(Cn) ->
            %% We don't need a try/catch around Call(Cn) because pooler links both the
            %% connection and the process that has the connection checked out (this
            %% process). So a crash here will not leak a connection.
            case F(Cn) of
                %% The only case we retry: a 'closed' error
                {error, closed} ->
                    sqerl_client:close(Cn), %% should it be closed?
                    %% TODO: the connection is bad and not checked back in.
                    %% How does the pooler handle that?
                    with_connection(F, Retries - 1);
                Result ->
                    checkin(Cn),
                    Result
            end;
        Other ->
            {error, Other}
    end.

%% @doc Utility for generating specific message tuples from database-specific error
%% messages.  The 1-argument form determines which database is being used by querying
%% Sqerl's configuration at runtime, while the 2-argument form takes the database type as a
%% parameter directly.
-spec parse_error(
        {term(), term()} |               %% MySQL error
        {error, {error, error, _, _, _}} %% PostgreSQL error
    ) -> sqerl_error().
parse_error(Reason) ->
    {ok, DbType} = application:get_env(sqerl, db_type),
    parse_error(DbType, Reason).

-spec parse_error(mysql | pgsql, atom() | {term(), term()}
                        | {error, {error, error, _, _, _}}) -> sqerl_error().
parse_error(_DbType, no_connections) ->
    {error, no_connections};
parse_error(_DbType, {no_pool, Type}) ->
    {error, {no_pool, Type}};

parse_error(mysql, Error) ->
    do_parse_error(Error, ?MYSQL_ERROR_CODES);

parse_error(pgsql, {error,               % error from sqerl
                    {error,              % error record marker from epgsql
                     error,              % Severity
                     Code, Message, _Extra}}) ->
    do_parse_error({Code, Message}, ?PGSQL_ERROR_CODES).

do_parse_error({Code, Message}, CodeList) ->
    case lists:keyfind(Code, 1, CodeList) of
        {_, ErrorType} ->
            {ErrorType, Message};
        false ->
            {error, Message}
    end.
