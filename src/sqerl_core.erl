%% -*- erlang-indent-level: 4;indent-tabs-mode: nil; fill-column: 92 -*-
%% ex: ts=4 sw=4 et
%% @author Marc Paradise <marc.paradise@chef.io>
%% @author Seth Falcon <seth@chef.io>
%% @author Mark Anderson <mark@chef.io>
%% Copyright 2011-2016 Chef Software, Inc.
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
-module(sqerl_core).

-include("sqerl.hrl").
-define(MAX_RETRIES, 5).

%% See http://www.postgresql.org/docs/current/static/errcodes-appendix.html
-define(PGSQL_ERROR_CODES, [{<<"23505">>, conflict}, {<<"23503">>, foreign_key}]).

-export([checkout/1,
         checkin/2,
         with_db/2,
         with_db/3,
         execute/3,
         execute_statement/5,
         bulk_insert/6,
         parse_error/1,
         parse_statement_results/1,
         parse_select_results/1,
         extract_insert_data/1]).

checkout(#sqerl_ctx{pool = Pool}) ->
    pooler:take_member(Pool, envy:get(sqerl, pooler_timeout, 0, integer)).

checkin(#sqerl_ctx{pool = Pool}, Connection) ->
    pooler:return_member(Pool, Connection).

with_db(Context, Call) ->
    with_db(Context, Call, ?MAX_RETRIES).

with_db(_Context, _Call, 0) ->
    {error, no_connections};
with_db(#sqerl_ctx{} = Context, Call, Retries) ->
    case checkout(Context) of
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
                    checkin(Context, Cn),
                    Result
            end
    end.

execute_statement(Context, StmtName, StmtArgs, XformName, XformArgs) ->
    case execute(Context, StmtName, StmtArgs) of
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
-spec execute(sqerl_ctx(), sqerl_query(), [] | [term()]) -> sqerl_results().
execute(Context, QueryOrStatement, Parameters) ->
    F = fun(Cn) -> sqerl_client:execute(Cn, QueryOrStatement, Parameters) end,
    with_db(Context, F).

bulk_insert(Context, Table, Columns, RowsValues, NumRows, BatchSize) when NumRows >= BatchSize ->
    Inserter = make_batch_inserter(Table, Columns, RowsValues, NumRows, BatchSize),
    with_db(Context, Inserter).

%% @doc Returns a function to call via sqerl_core:with_db/1.
%%
%% Function prepares an insert statement, inserts all the batches and
%% remaining rows, unprepares the statement, and returns
%% {ok, InsertedCount}.
%%
%% We need to use this approach because preparing, inserting,
%% unpreparing must all be done against the same DB connection.
%%
make_batch_inserter(Table, Columns, RowsValues, NumRows, BatchSize) ->
    ParamStyle = sqerl_client:sql_parameter_style(),
    SQL = sqerl_adhoc:insert(Table, Columns, BatchSize, ParamStyle),

    fun(Cn) ->
        ok = sqerl_client:prepare(Cn, ?SQERL_ADHOC_INSERT_STMT_ATOM, SQL),
        try
            insert_batches(Cn, ?SQERL_ADHOC_INSERT_STMT_ATOM,
                           Table, Columns, RowsValues, NumRows, BatchSize, ParamStyle)
        after
            sqerl_client:unprepare(Cn, ?SQERL_ADHOC_INSERT_STMT_ATOM)
        end
    end.


%% @doc Insert data with insert statement already prepared.
insert_batches(Cn, StmtName, Table, Columns, RowsValues, NumRows, BatchSize, ParamStyle) ->
    insert_batches(Cn, StmtName, Table, Columns, RowsValues, NumRows, BatchSize, 0, ParamStyle).

%% @doc Tail-recursive function iterates over batches and inserts them.
%% Also inserts the remaining rows (if any) in one shot.
%% Returns {ok, InsertedCount}.
insert_batches(Cn, StmtName, Table, Columns, RowsValues, NumRows, BatchSize, CountSoFar, ParamStyle)
        when NumRows >= BatchSize ->
    {RowsValuesToInsert, Rest} = lists:split(BatchSize, RowsValues),
    {ok, Count} = insert_oneshot(Cn, StmtName, RowsValuesToInsert),
    insert_batches(Cn, StmtName, Table, Columns, Rest, NumRows - Count, BatchSize, CountSoFar + Count, ParamStyle);
insert_batches(Cn, _StmtName, Table, Columns, RowsValues, _NumRows, _BatchSize, CountSoFar, ParamStyle) ->
    %% We have fewer rows than fit in a batch, so we'll do a one-shot insert for those.
    {ok, InsertCount} = adhoc_insert_oneshot(Cn, Table, Columns, RowsValues, ParamStyle),
    {ok, CountSoFar + InsertCount}.

%% @doc Insert all given rows in one shot.
%% Creates one SQL statement to insert all the rows at once,
%% then executes.
%% Returns {ok, InsertedCount}.
adhoc_insert_oneshot(_Cn, _Table, _Columns, [], _ParamStyle) ->
    %% 0 rows means nothing to do!
    {ok, 0};
adhoc_insert_oneshot(Cn, Table, Columns, RowsValues, ParamStyle) ->
    SQL = sqerl_adhoc:insert(Table, Columns, length(RowsValues), ParamStyle),
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

parse_select_results({ok, []}) ->
    {ok, none};
parse_select_results({ok, Results}) ->
    {ok, Results};
parse_select_results({ok, Count, Results}) ->
    {ok, Count, Results};
parse_select_results({error, Reason}) ->
    parse_error(Reason).

parse_statement_results({ok, 0}) ->
    {ok, none};
parse_statement_results({ok, N}) when is_number(N) ->
    {ok, N};
parse_statement_results({ok, {N, Rows}}) when is_number(N) ->
    {ok, N, Rows};
parse_statement_results({ok, N, Rows}) when is_number(N) ->
    {ok, N, Rows};
parse_statement_results({error, Reason}) ->
    parse_error(Reason).

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



