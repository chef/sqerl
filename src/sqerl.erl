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
         select/5,
         statement/2,
         statement/3,
         statement/4,
         execute/1,
         execute/2,
         execute/3,
         adhoc_select/3,
         adhoc_select/4,
         adhoc_select/5,
         adhoc_insert/2,
         adhoc_insert/3,
         adhoc_insert/4,
         adhoc_insert/5,
         extract_insert_data/1,
         adhoc_delete/2,
         adhoc_delete/3]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("sqerl.hrl").

-define(MAX_RETRIES, 5).

%% See http://www.postgresql.org/docs/current/static/errcodes-appendix.html
-define(PGSQL_ERROR_CODES, [{<<"23505">>, conflict}, {<<"23503">>, foreign_key}]).

checkout() ->
    pooler:take_member(sqerl, envy:get(sqerl, pooler_timeout, 0, integer)).

checkin(Connection) ->
    pooler:return_member(sqerl, Connection).

with_db(Call) ->
    with_db(Call, ?MAX_RETRIES).

with_db(_Call, 0) ->
    {error, no_connections};
with_db(Call, Retries) ->
    case checkout() of
        error_no_members ->
            {error, no_connections};
        Cn when is_pid(Cn) ->
            %% We don't need a try/catch around Call(Cn) because pooler links both the
            %% connection and the process that has the connection checked out (this
            %% process). So a crash here will not leak a connection.
            case Call(Cn) of
                {error, closed} ->
                    %% Closing the connection will cause the process
                    %% to shutdown. pooler will get notified and
                    %% remove the connection from the pool.
                    sqerl_client:close(Cn),
                    with_db(Call, Retries - 1);
                Result ->
                    checkin(Cn),
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
    with_db(fun (Cn) -> select(Cn, StmtName, StmtArgs, XformName, XformArgs) end).

select(Cn, StmtName, StmtArgs, XformName, XformArgs) ->
    case execute_statement(Cn, StmtName, StmtArgs, XformName, XformArgs) of
        {ok, []} ->
            {ok, none};
        {ok, Results} ->
            {ok, Results};
        {ok, Count, Results} ->
            {ok, Count, Results};
        {error, Reason} ->
            parse_error(Reason)
    end.

statement(StmtName, StmtArgs) ->
    statement(StmtName, StmtArgs, identity, []).

statement(StmtName, StmtArgs, XformName) ->
    statement(StmtName, StmtArgs, XformName, []).

statement(StmtName, StmtArgs, XformName, XformArgs) ->
    with_db(fun (Cn) ->
                    statement(Cn, StmtName, StmtArgs, XformName, XformArgs)
            end).

statement(Cn, StmtName, StmtArgs, XformName, XformArgs) ->
    case execute_statement(Cn, StmtName, StmtArgs, XformName, XformArgs) of
        {ok, 0} ->
            {ok, none};
        {ok, N} when is_number(N) ->
            {ok, N};
        % response from execute of raw sql
        {ok, {N, Rows}} when is_number(N) ->
            {ok, N, Rows};
        {ok, N, Rows} when is_number(N) ->
            {ok, N, Rows};
        {error, Reason} ->
            parse_error(Reason)
    end.

execute_statement(Cn, StmtName, StmtArgs, XformName, XformArgs) ->
    case execute(Cn, StmtName, StmtArgs) of
        {ok, Results} ->
            Xformer = erlang:apply(sqerl_transformers, XformName, XformArgs),
            Xformer(Results);
        {ok, Count, Results} ->
            %% we'll get here for an INSERT ... RETURNING query
            Xformer = erlang:apply(sqerl_transformers, XformName, XformArgs),
            {ok, XResult} = Xformer(Results),
            {ok, Count, XResult};
        Other ->
            Other
    end.

%% @doc Execute query or statement with no parameters.
%% See execute/2 for return info.
-spec execute(sqerl_query()) -> sqerl_results().
execute(QueryOrStatement) ->
    execute(QueryOrStatement, []).

%% @doc Execute query or statement with parameters.
%% ```
%% Returns:
%% - {ok, Result}
%% - {error, ErrorInfo}
%%
%% Result depends on the query being executed, and can be
%% - Rows
%% - Count
%%
%% Row is a proplist-like array, e.g. [{<<"id">>, 1}, {<<"name">>, <<"John">>}]
%%
%% Note that both a simple query and a prepared statement can take
%% parameters.
%% '''
%%
-spec execute(sqerl_query(), [] | [term()]) -> sqerl_results().
execute(QueryOrStatement, Parameters) ->
    F = fun(Cn) -> sqerl_client:execute(Cn, QueryOrStatement, Parameters) end,
    with_db(F).

-spec execute(pid(), sqerl_query(), [] | [term()]) -> sqerl_results().
execute(Cn, QueryOrStatement, Parameters) ->
    sqerl_client:execute(Cn, QueryOrStatement, Parameters).

%% @doc Execute an adhoc select query.
%% ```
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
%% '''
-spec adhoc_select([binary() | string()], binary() | string(), atom() | tuple()) -> sqerl_results().
adhoc_select(Columns, Table, Where) ->
    adhoc_select(Columns, Table, Where, []).

%% @doc Execute an adhoc select query with additional clauses.
%% ```
%% Group By Clause
%% ---------------
%% Form: {group_by, Fields}
%%
%% Order By Clause
%% ---------------
%% Form: {order_by, Fields | {Fields, asc|desc}}
%%
%% Limit/Offset Clause
%% --------------------
%% Form: {limit, Limit} | {limit, {Limit, offset, Offset}}
%% '''
%% See itest:adhoc_select_complex/0 for an example of a complex query
%% that uses several clauses.
-spec adhoc_select([binary() | string()], binary() | string(), atom() | tuple(), [] | [atom() | tuple()]) -> sqerl_results().
adhoc_select(Columns, Table, Where, Clauses) ->
    with_db(fun (Cn) -> adhoc_select(Cn, Columns, Table, Where, Clauses) end).

-spec adhoc_select(pid(), [binary() | string()], binary() | string(), atom() | tuple(), [] | [atom() | tuple()]) -> sqerl_results().
adhoc_select(Cn, Columns, Table, Where, Clauses) ->
    {SQL, Values} = sqerl_adhoc:select(Columns, Table,
                      [{where, Where}|Clauses], param_style()),
    execute(Cn, SQL, Values).


-define(SQERL_DEFAULT_BATCH_SIZE, 100).
%% Backend DBs limit what we can name a statement.
%% No upper case, no $...
-define(SQERL_ADHOC_INSERT_STMT_ATOM, '__adhoc_insert').

%% @doc Insert Rows into Table with default batch size.
%% @see adhoc_insert/3.
adhoc_insert(Table, Rows) ->
    adhoc_insert(Table, Rows, ?SQERL_DEFAULT_BATCH_SIZE).

%% @doc Insert Rows into Table with given batch size.
%%
%% Reformats input data to {Columns, RowsValues} and
%% calls adhoc_insert/4.
%% ```
%% - Rows: list of proplists (such as returned by a select) e.g.
%% [
%%     [{<<"id">>, 1},{<<"first_name">>, <<"Kevin">>}],
%%     [{<<"id">>, 2},{<<"first_name">>, <<"Mark">>}]
%%   ]
%% '''
%% Returns {ok, InsertCount}
%%
%% @see adhoc_insert/4.
adhoc_insert(Table, Rows, BatchSize) ->
    %% reformat Rows to desired format
    {Columns, RowsValues} = extract_insert_data(Rows),
    adhoc_insert(Table, Columns, RowsValues, BatchSize).

%% @doc Insert records defined by {Columns, RowsValues}
%% into Table using given BatchSize.
%% ```
%% - Columns, RowsValues e.g.
%%   {[<<"first_name">>, <<"last_name">>],
%%      [
%%        [<<"Joe">>, <<"Blow">>],
%%        [<<"John">>, <<"Doe">>]
%%      ]}
%%
%% Returns {ok, InsertedCount}.
%%
%% 1> adhoc_insert(<<"users">>,
%%        {[<<"first_name">>, <<"last_name">>],
%%         [[<<"Joe">>, <<"Blow">>],
%%          [<<"John">>, <<"Doe">>]]}).
%% {ok, 2}
%% '''
%%
adhoc_insert(Table, Columns, RowsValues, BatchSize) ->
    with_db(fun (Cn) -> adhoc_insert(Cn, Table, Columns, RowsValues, BatchSize) end).

adhoc_insert(_Cn, _Table, _Columns, [], _BatchSize) ->
    %% empty list of rows means nothing to do
    {ok, 0};
adhoc_insert(Cn, Table, Columns, RowsValues, BatchSize) when BatchSize > 0 ->
    NumRows = length(RowsValues),
    %% Avoid the case where NumRows < BatchSize
    EffectiveBatchSize = erlang:min(NumRows, BatchSize),
    bulk_insert(Cn, Table, Columns, RowsValues, NumRows, EffectiveBatchSize).

%% @doc Bulk insert rows. Returns {ok, InsertedCount}.
bulk_insert(Cn, Table, Columns, RowsValues, NumRows, BatchSize) when NumRows >= BatchSize ->
    Inserter = make_batch_inserter(Table, Columns, RowsValues, NumRows, BatchSize),
    Inserter(Cn).

%% @doc Returns a function to call via with_db/1.
%%
%% Function prepares an insert statement, inserts all the batches and
%% remaining rows, unprepares the statement, and returns
%% {ok, InsertedCount}.
%%
%% We need to use this approach because preparing, inserting,
%% unpreparing must all be done against the same DB connection.
%%
make_batch_inserter(Table, Columns, RowsValues, NumRows, BatchSize) ->
    SQL = sqerl_adhoc:insert(Table, Columns, BatchSize, param_style()),
    fun(Cn) ->
        ok = sqerl_client:prepare(Cn, ?SQERL_ADHOC_INSERT_STMT_ATOM, SQL),
        try
            insert_batches(Cn, ?SQERL_ADHOC_INSERT_STMT_ATOM,
                           Table, Columns, RowsValues, NumRows, BatchSize)
        after
            sqerl_client:unprepare(Cn, ?SQERL_ADHOC_INSERT_STMT_ATOM)
        end
    end.

%% @doc Insert data with insert statement already prepared.
insert_batches(Cn, StmtName, Table, Columns, RowsValues, NumRows, BatchSize) ->
    insert_batches(Cn, StmtName, Table, Columns, RowsValues, NumRows, BatchSize, 0).

%% @doc Tail-recursive function iterates over batches and inserts them.
%% Also inserts the remaining rows (if any) in one shot.
%% Returns {ok, InsertedCount}.
insert_batches(Cn, StmtName, Table, Columns, RowsValues, NumRows, BatchSize, CountSoFar)
        when NumRows >= BatchSize ->
    {RowsValuesToInsert, Rest} = lists:split(BatchSize, RowsValues),
    {ok, Count} = insert_oneshot(Cn, StmtName, RowsValuesToInsert),
    insert_batches(Cn, StmtName, Table, Columns, Rest, NumRows - Count, BatchSize, CountSoFar + Count);
insert_batches(Cn, _StmtName, Table, Columns, RowsValues, _NumRows, _BatchSize, CountSoFar) ->
    %% We have fewer rows than fit in a batch, so we'll do a one-shot insert for those.
    {ok, InsertCount} = adhoc_insert_oneshot(Cn, Table, Columns, RowsValues),
    {ok, CountSoFar + InsertCount}.

%% @doc Insert all given rows in one shot.
%% Creates one SQL statement to insert all the rows at once,
%% then executes.
%% Returns {ok, InsertedCount}.
adhoc_insert_oneshot(_Cn, _Table, _Columns, []) ->
    %% 0 rows means nothing to do!
    {ok, 0};
adhoc_insert_oneshot(Cn, Table, Columns, RowsValues) ->
    SQL = sqerl_adhoc:insert(Table, Columns, length(RowsValues), param_style()),
    insert_oneshot(Cn, SQL, RowsValues).

%% @doc Insert all rows at once using given
%% prepared statement or SQL.
%% Returns {ok, InsertCount}.
insert_oneshot(_Cn, _StmtOrSQL, []) ->
    %% 0 rows means nothing to do!
    {ok, 0};
insert_oneshot(Cn, StmtOrSQL, RowsValues) ->
    %% Need to flatten list of row data (list of lists)
    %% to a flat list of parameters to the query
    Parameters = lists:flatten(RowsValues),
    sqerl_client:execute(Cn, StmtOrSQL, Parameters).

%% @doc Extract insert data from Rows.
%%
%% Assumes all rows have the same format.
%% Returns {Columns, RowsValues}.
%%
%% ```
%% 1> extract_insert_data([
%%                         [{<<"id">>, 1}, {<<"name">>, <<"Joe">>}],
%%                         [{<<"id">>, 2}, {<<"name">>, <<"Jeff">>}],
%%                        ]).
%% {[<<"id">>,<<"name">>],[[1,<<"Joe">>],[2,<<"Jeff">>]]}
%% '''
%%
-spec extract_insert_data(sqerl_rows()) -> {[binary()], [[term()]]}.
extract_insert_data([]) ->
    {[], []};
extract_insert_data(Rows) ->
    Columns = [C || {C, _V} <- hd(Rows)],
    RowsValues = [[V || {_C, V} <- Row] || Row <- Rows],
    {Columns, RowsValues}.


%% @doc Adhoc delete.
%% Uses the same Where specifications as adhoc_select/3.
%% Returns {ok, Count} or {error, ErrorInfo}.
%%
-spec adhoc_delete(binary(), term()) -> {ok, integer()} | {error, any()}.
adhoc_delete(Table, Where) ->
    with_db(fun (Cn) -> adhoc_delete(Cn, Table, Where) end).

adhoc_delete(Cn, Table, Where) ->
    {SQL, Values} = sqerl_adhoc:delete(Table, Where, param_style()),
    execute(Cn, SQL, Values).

%% The following illustrates how we could also implement adhoc update
%% if ever desired.
%%
%% %@doc Adhoc update.
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
    parse_error(pgsql, Reason).

-spec parse_error(pgsql,
                  'no_connections' |
                  {'error', 'error', _, _, _} |
                  {'error', {'error', _, _, _, _}}) -> sqerl_error().
parse_error(_DbType, no_connections) ->
    {error, no_connections};
parse_error(_DbType, {error, Reason} = Error) when is_atom(Reason) ->
    Error;
parse_error(pgsql, {error, error, Code, Message, _Extra}) ->
    do_parse_error({Code, Message}, ?PGSQL_ERROR_CODES);
parse_error(pgsql, {error,               % error from sqerl
                    {error,              % error record marker from epgsql
                     _Severity,          % Severity
                     Code, Message, _Extra}}) ->
    do_parse_error({Code, Message}, ?PGSQL_ERROR_CODES);
parse_error(_, Error) ->
    case Error of
        {error, _} ->
            Error;
        _ ->
            {error, Error}
    end.

do_parse_error({Code, Message}, CodeList) ->
    case lists:keyfind(Code, 1, CodeList) of
        {_, ErrorType} ->
            {ErrorType, Message};
        false ->
            %% People of the Future!
            %% For Postgres, sqerl_pgsql_errors:translate_code is available
            %% turning Postgres codes to human-readable tuples
            {error, {Code, Message}}
    end.
