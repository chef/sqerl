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
         adhoc_select/4,
         adhoc_insert/2,
         adhoc_insert/3,
         adhoc_insert/4,
         extract_insert_data/1,
         adhoc_delete/2]).

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

%% @doc Call function with a DB connection.
%% Function must take one DB connection argument.
%% It should return {ok, Results} or {error, ErrorInfo}.
%% Call will be retried where possible (e.g. DB connection was closed).
%% ErrorInfo is no_connections if all attempts have failed, or whatever
%% is provided by the DB client.
with_db(Call) ->
    with_db(Call, ?MAX_RETRIES).

with_db(_Call, 0) ->
    {error, no_connections};
with_db(Call, Retries) ->
    case checkout() of
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
                    sqerl_client:close(Cn),
                    with_db(Call, Retries - 1);
                Result ->
                    checkin(Cn),
                    Result
            end;
        Other ->
            {error, Other}
    end.

select(StmtName, StmtArgs) ->
    select(StmtName, StmtArgs, identity, []).

select(StmtName, StmtArgs, {XformName, XformArgs}) ->
    select(StmtName, StmtArgs, XformName, XformArgs);
select(StmtName, StmtArgs, XformName) ->
    select(StmtName, StmtArgs, XformName, []).

select(StmtName, StmtArgs, XformName, XformArgs) ->
    case execute_statement(StmtName, StmtArgs, XformName, XformArgs) of
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
    case execute_statement(StmtName, StmtArgs, XformName, XformArgs) of
        {ok, 0} ->
            {ok, none};
        {ok, N} when is_number(N) ->
            {ok, N};
        {error, Reason} ->
            parse_error(Reason)
    end.

execute_statement(StmtName, StmtArgs, XformName, XformArgs) ->
    Xformer = erlang:apply(sqerl_transformers, XformName, XformArgs),
    case execute(StmtName, StmtArgs) of
        {ok, Results} -> Xformer(Results);
        Other         -> Other
    end.

%% @doc Execute query or statement with no parameters
%% See execute/2 for return info.
-spec execute(sql_query() | atom()) -> sql_result().
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
-spec execute(sql_query() | atom(), [] | [term()]) -> sql_result().
execute(QueryOrStatement, Parameters) ->
    F = fun(Cn) -> sqerl_client:execute(Cn, QueryOrStatement, Parameters) end,
    with_db(F).

%% @doc Execute an adhoc query: adhoc_select(Columns, Table, Where)
%% or adhoc_select(Columns, Table, Where, Clauses)
%%
%% Returns:
%% - {ok, Rows}
%% - {error, ErrorInfo}
%%
%% See execute/2 for more details on return data.
%%
%% Where Clause
%% -------------
%% Form: {where, Where}
%%
%% Where = all|undefined -- Does not generate a WHERE clause.
%%                          Matches all records in table.
%% Where = {Field, equals|nequals|gt|gte|lt|lte, Value}
%% Where = {Field, in|notin, Values}
%% Where = {'and'|'or', WhereList} -- Composes WhereList with AND or OR
%%
%% adhoc_select/4 takes an additional Clauses argument which
%% is a list of additional clauses for the query.
%%
%% Order By Clause
%% ---------------
%% Form: {orderby, Fields | {Fields, asc|desc}}
%%
%% Limit/Offset Clause
%% --------------------
%% Form: {limit, Limit} | {limit, {Limit, offset, Offset}}
%%
%% See itest:adhoc_select_complex/0 for an example of a complex query
%% that uses several clauses.
%%
%% -spec adhoc_select([binary() | string()], binary() | string(), sql_clause()) -> sql_result().
adhoc_select(Columns, Table, Where) ->
    adhoc_select(Columns, Table, Where, []).

%% -spec adhoc_select([binary() | string()], binary() | string(), sql_clause(), [] | [sql_clause]) -> sql_result().
adhoc_select(Columns, Table, Where, Clauses) ->
    {SQL, Values} = sqerl_adhoc:select(Columns, Table, 
                      [{where, Where}|Clauses], param_style()),
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
-define(DEFAULT_BATCH_SIZE, 100).
-define(ADHOC_INSERT_STMT_ATOM, '__adhoc_insert').

%% TODO: What if some inserts in a batch fail? Error out or continue?
%% TODO: Transactionality? Retries?
%% TODO: parallel inserts?

adhoc_insert(Table, Rows) ->
    adhoc_insert(Table, Rows, ?DEFAULT_BATCH_SIZE).

adhoc_insert(Table, Rows, BatchSize) ->
    %% reformat Rows to desired format
    {Columns, RowsValues} = extract_insert_data(Rows),
    adhoc_insert(Table, Columns, RowsValues, BatchSize).

adhoc_insert(Table, Columns, RowsValues, BatchSize) ->
    NumRows = length(RowsValues),
    bulk_insert(Table, Columns, RowsValues, NumRows, BatchSize).

bulk_insert(Table, Columns, RowsValues, NumRows, BatchSize) when NumRows < BatchSize ->
    %% Do one bulk insert since we have less than BULK_SIZE rows
    SQL = sqerl_adhoc:insert(Table, Columns, length(RowsValues), param_style()),
    execute(SQL, lists:flatten(RowsValues));
bulk_insert(Table, Columns, RowsValues, NumRows, BatchSize) when NumRows >= BatchSize ->
    %% Prepare a bulk insert statement and execute as many times as needed.
    SQL = sqerl_adhoc:insert(Table, Columns, BatchSize, param_style()),
    PrepInsertUnprepare = fun(Cn) ->
        ok = sqerl_client:prepare(Cn, ?ADHOC_INSERT_STMT_ATOM, SQL),
        try adhoc_prepared_insert(Cn, RowsValues, NumRows, BatchSize)
        after sqerl_client:unprepare(Cn, ?ADHOC_INSERT_STMT_ATOM)
        end
    end,
    {ok, Count, RemainingRowsValues} = with_db(PrepInsertUnprepare),
    RemainingNumRows = NumRows - Count,
    {ok, RemainingCount} = bulk_insert(Table, Columns, RemainingRowsValues, RemainingNumRows, BatchSize),
    {ok, Count + RemainingCount}.

%% @doc Insert data with insert statement already prepared
adhoc_prepared_insert(Cn, RowsValues, NumRows, BatchSize) ->
    adhoc_prepared_insert(Cn, RowsValues, NumRows, BatchSize, 0).

adhoc_prepared_insert(Cn, RowsValues, NumRows, BatchSize, CountSoFar) when NumRows >= BatchSize ->
    {RowsValuesToInsert, Rest} = lists:split(BatchSize, RowsValues),
    {ok, Count} = sqerl_client:execute(Cn, ?ADHOC_INSERT_STMT_ATOM, lists:flatten(RowsValuesToInsert)),
    adhoc_prepared_insert(Cn, Rest, NumRows - Count, BatchSize, CountSoFar + Count);
adhoc_prepared_insert(_Cn, RowsValues, NumRows, BatchSize, CountSoFar) when NumRows < BatchSize ->
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


%% @doc Adhoc delete.
%% Uses the same Where specifications as adhoc_select/3.
%% Returns {ok, Count} or {error, ErrorInfo}.
%%
-spec adhoc_delete(binary(), term()) -> {ok, integer()} | {error, any()}.
adhoc_delete(Table, Where) ->
    {SQL, Values} = sqerl_adhoc:delete(Table, Where, param_style()),
    execute(SQL, Values).

%% The following illustrates how we could also implement adhoc update
%% if ever desired.
%%
%% @doc Adhoc update.
%% Updates records matching Where specifications with
%% fields and values in given Row.
%% Uses the same Where specifications as adhoc_select/3.
%% Returns {ok, Count} or {error, ErrorInfo}.
%%
%%-spec adhoc_update(binary(), list(), term()) -> {ok, integer()} | {error, any()}.
%%adhoc_update(Table, Row, Where) ->
%%    {SQL, Values} = sqerl_adhoc:update(Table, Row, Where, param_style()),
%%    execute(SQL, Values).


%% @doc Shortcut for sqerl_client:parameter_style()
-spec param_style() -> atom().
param_style() -> sqerl_client:sql_parameter_style().


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
parse_error(_DbType, error_no_members) ->
    {error, error_no_members};
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
