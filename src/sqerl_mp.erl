%% -*- erlang-indent-level: 4;indent-tabs-mode: nil; fill-column: 92 -*-
%% ex: ts=4 sw=4 et
%% @author Marc Paradise <marc.paradise@chef.io>
%% Copyright 2015 Chef Software, Inc.
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
-module(sqerl_mp).
-include("sqerl.hrl").

-export([select/3,
         select/4,
         select/5,
         statement/3,
         statement/4,
         statement/5,
         execute/3,
         execute/2,
         adhoc_select/4,
         adhoc_select/5,
         adhoc_insert/3,
         adhoc_insert/4,
         adhoc_insert/5,
         adhoc_delete/3]).

select(Pool, StmtName, StmtArgs) ->
    select(Pool, StmtName, StmtArgs, identity, []).

select(Pool, StmtName, StmtArgs, {XformName, XformArgs}) ->
    select(Pool, StmtName, StmtArgs, XformName, XformArgs);
select(Pool, StmtName, StmtArgs, XformName) ->
    select(Pool, StmtName, StmtArgs, XformName, []).

select(Pool, StmtName, StmtArgs, XformName, XformArgs) ->
    Results = sqerl_core:execute_statement(Pool, StmtName, StmtArgs, XformName, XformArgs),
    sqerl_core:parse_select_results(Results).


statement(Pool, StmtName, StmtArgs) ->
    statement(Pool, StmtName, StmtArgs, identity, []).

statement(Pool, StmtName, StmtArgs, XformName) ->
    statement(Pool, StmtName, StmtArgs, XformName, []).

statement(Pool, StmtName, StmtArgs, XformName, XformArgs) ->
    Results = sqerl_core:execute_statement(Pool, StmtName, StmtArgs, XformName, XformArgs),
    sqerl_core:parse_statement_results(Results).


%% @doc Execute query or statement with no parameters.
%% See execute/2 for return info.
-spec execute(atom(), sqerl_query()) -> sqerl_results().
execute(Pool, QueryOrStatement) ->
    sqerl_core:execute(Pool, QueryOrStatement, []).

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
-spec execute(atom(), sqerl_query(), [] | [term()]) -> sqerl_results().
execute(Pool, QueryOrStatement, Parameters) ->
    sqerl_core:execute(Pool, QueryOrStatement, Parameters).


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
-spec adhoc_select(atom(), [binary() | string()], binary() | string(), atom() | tuple()) -> sqerl_results().
adhoc_select(Pool, Columns, Table, Where) ->
    adhoc_select(Pool, Columns, Table, Where, []).

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
-spec adhoc_select(atom(), [binary() | string()], binary() | string(), atom() | tuple(), [] | [atom() | tuple()]) -> sqerl_results().
adhoc_select(Pool, Columns, Table, Where, Clauses) ->
    {SQL, Values} = sqerl_adhoc:select(Columns,
                                       Table,
                                       [{where, Where}|Clauses],
                                       sqerl_client:sql_parameter_style()),
    sqerl_core:execute(Pool, SQL, Values).


%% @doc Utility for generating specific message tuples from database-specific error
%% messages.  The 1-argument form determines which database is being used by querying
%% Sqerl's configuration at runtime, while the 2-argument form takes the database type as a
%% parameter directly.
%% @doc Insert Rows into Table with default batch size.
%% @see adhoc_insert/3.
adhoc_insert(Pool, Table, Rows) ->
    adhoc_insert(Pool, Table, Rows, ?SQERL_DEFAULT_BATCH_SIZE).

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
adhoc_insert(Pool, Table, Rows, BatchSize) ->
    %% reformat Rows to desired format
    {Columns, RowsValues} = sqerl_core:extract_insert_data(Rows),
    adhoc_insert(Pool, Table, Columns, RowsValues, BatchSize).

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
adhoc_insert(_Pool, _Table, _Columns, [], _BatchSize) ->
    %% empty list of rows means nothing to do
    {ok, 0};
adhoc_insert(Pool, Table, Columns, RowsValues, BatchSize) when BatchSize > 0 ->
    NumRows = length(RowsValues),
    %% Avoid the case where NumRows < BatchSize
    EffectiveBatchSize = erlang:min(NumRows, BatchSize),
    bulk_insert(Pool, Table, Columns, RowsValues, NumRows, EffectiveBatchSize).

%% @doc Bulk insert rows. Returns {ok, InsertedCount}.
bulk_insert(Pool, Table, Columns, RowsValues, NumRows, BatchSize) when NumRows >= BatchSize ->
    sqerl_core:bulk_insert(Pool, Table, Columns, RowsValues, NumRows, BatchSize) .





%% @doc Adhoc delete.
%% Uses the same Where specifications as adhoc_select/3.
%% Returns {ok, Count} or {error, ErrorInfo}.
%%
-spec adhoc_delete(atom(), binary(), term()) -> {ok, integer()} | {error, any()}.
adhoc_delete(Pool, Table, Where) ->
    {SQL, Values} = sqerl_adhoc:delete(Table, Where, sqerl_client:sql_parameter_style()),
    sqerl_core:execute(Pool, SQL, Values).
