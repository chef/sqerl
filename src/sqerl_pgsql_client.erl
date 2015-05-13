%% -*- erlang-indent-level: 4;indent-tabs-mode: nil; fill-column: 92 -*-
%% ex: ts=4 sw=4 et
%% @author Mark Anderson <mark@opscode.com>
%% @doc Abstraction around interacting with pgsql databases
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

-module(sqerl_pgsql_client).

-behaviour(sqerl_client).

-include_lib("sqerl.hrl").
-include_lib("epgsql/include/epgsql.hrl").

%% sqerl_client callbacks
-export([init/1,
         prepare/3,
         unprepare/3,
         execute/3,
         is_connected/1,
         sql_parameter_style/0]).

-define(EPGSQL_TIMEOUT_ERROR, {error,error,<<"57014">>,
                                  <<"canceling statement due to statement timeout">>,
                                  []}).
-ifdef(TEST).
-compile([export_all]).
-endif.

-record(state,  {cn = undefined :: pid() | undefined,
                 %% The statements dict is the backing store of meta data for prepared
                 %% queries on this connection. It maps named queries (atoms) to either a
                 %% `#prepared_statement{}' record or the SQL needed to create the prepared
                 %% query as a binary. The dict should only be manipulated via `pqc_*'
                 %% functions.
                 statements = dict:new() :: sqerl_dict(),
                 ctrans :: sqerl_dict() | undefined,
                 default_timeout = 0 :: non_neg_integer() }).

-record(prepared_statement,
        {name :: string(),
         input_types  :: any(),
         output_fields :: any(),
         stmt :: any() } ).

-type connection() :: pid().

-define(PING_QUERY, <<"SELECT 'pong' as ping LIMIT 1">>).


%% @doc Execute query or prepared statement.
%% If a binary is provided, it is interpreted as an SQL query.
%% If an atom is provided, it is interpreted as a prepared statement name.
%%
%% Returns:{Result, State}
%%
%% Result:
%% - {ok, Rows}
%% - {ok, Count}
%% - {ok, {Count, Rows}}
%% - {error, ErrorInfo}
%%
%% Row:  proplist e.g. `[{<<"id">>, 1}, {<<"name">>, <<"Toto">>}]'
%%
-spec execute(StatementOrQuery :: sqerl_query(),
              Parameters :: [any()],
              State :: #state{}) ->
                  {sqerl_results(), #state{}}.
execute(SQL, Parameters, #state{cn=Cn}=State) when is_binary(SQL) ->
    TParameters = input_transforms(Parameters),
    case epgsql:equery(Cn, SQL, TParameters) of
        {ok, Columns, Rows} ->
            {{ok, format_result(Columns, Rows)}, State};
        {ok, Count} ->
            {{ok, Count}, State};
        {ok, Count, Columns, Rows} ->
            {{ok, {Count, format_result(Columns, Rows)}}, State};
        {error, ?EPGSQL_TIMEOUT_ERROR} ->
            {{error, timeout}, State};
        {error, Error} ->
            {{error, Error}, State}
    end;
%% Prepared statement execution
execute(StatementName, Parameters, #state{cn = Cn, statements = Statements} = State)
  when is_atom(StatementName) ->
    execute_prepared(pqc_fetch(StatementName, Statements, Cn), Parameters, State).

execute_prepared({#prepared_statement{} = PrepStmt, Statements}, Parameters,
                 #state{cn = Cn, ctrans = CTrans} = State) ->
    Stmt = PrepStmt#prepared_statement.stmt,
    TParameters = input_transforms(Parameters, PrepStmt, State),
    ok = epgsql:bind(Cn, Stmt, TParameters),
    Result = try epgsql:execute(Cn, Stmt) of
        {ok, Count} when is_integer(Count) ->
            % returned for update, delete, so sync db
            epgsql:sync(Cn),
            {ok, Count};
        {ok, RowData} when is_list(RowData) ->
            % query results, read-only, so no sync needed...?
            % call it just in case, it shouldn't hurt
            epgsql:sync(Cn),
            Rows = unpack_rows(PrepStmt, RowData),
            TRows = sqerl_transformers:by_column_name(Rows, CTrans),
            {ok, TRows};
        {ok, Count, RowData} when is_list(RowData), is_integer(Count) ->
            epgsql:sync(Cn),
            Rows = unpack_rows(PrepStmt, RowData),
            TRows = sqerl_transformers:by_column_name(Rows, CTrans),
            {ok, Count, TRows};
        {error, ?EPGSQL_TIMEOUT_ERROR} ->
            epgsql:sync(Cn),
            {error, timeout};
        Other ->
            epgsql:sync(Cn),
            {error, Other}
        catch _:X ->
            epgsql:sync(Cn),
            {error, X}
        end,
    {Result, State#state{statements = Statements}};
execute_prepared(Error, _Parameters, State) ->
    %% There was an error preparing the query or the named query was not found.
    {Error, State}.


%% @doc Prepare a new statement.
-spec prepare(atom(), binary(), #state{}) -> {ok, #state{}}.
prepare(Name, SQL, #state{cn=Cn, statements=Statements}=State) ->
    {ok, UpdatedStatements} = load_statement(Cn, Name, SQL, Statements),
    UpdatedState = State#state{statements=UpdatedStatements},
    {ok, UpdatedState}.

%% @doc Unprepare a previously prepared statement
%% Protocol between sqerl_client and db-specific modules
%% uses 3 parameters (QueryOrName, Args, State) for all
%% calls for simplicity. For an unprepare call, there are
%% no arguments, so the second parameter of the function
%% is unused.
-spec unprepare(atom(), [], #state{}) -> {ok, #state{}}.
unprepare(Name, _, State) ->
    unprepare(Name, State).

-spec unprepare(atom(), #state{}) -> {ok, #state{}}.
unprepare(Name, #state{cn=Cn, statements=Statements}=State) ->
    {ok, UpdatedStatements} = unload_statement(Cn, Name, Statements),
    UpdatedState = State#state{statements=UpdatedStatements},
    {ok, UpdatedState}.

%% @see sqerl_adhoc:select/4.
-spec sql_parameter_style() -> dollarn.
sql_parameter_style() -> dollarn.

is_connected(#state{cn=Cn}=State) ->
    case catch epgsql:squery(Cn, ?PING_QUERY) of
        {ok, _, _} ->
            {true, State};
        _ ->
            false
    end.

init(Config) ->
    {host, Host} = lists:keyfind(host, 1, Config),
    {port, Port} = lists:keyfind(port, 1, Config),
    {user, User} = lists:keyfind(user, 1, Config),
    {pass, Pass} = lists:keyfind(pass, 1, Config),
    {timeout, Timeout} = lists:keyfind(timeout, 1, Config),
    {db, Db} = lists:keyfind(db, 1, Config),
    {prepared_statements, Statements} = lists:keyfind(prepared_statements, 1, Config),
    Opts = [{database, Db}, {port, Port}, {timeout, Timeout}],
    CTrans =
        case lists:keyfind(column_transforms, 1, Config) of
            {column_transforms, CT} -> CT;
            false -> undefined
        end,
    case epgsql:connect(Host, User, Pass, Opts) of
        %% epgsql/epgsql no longer handles connect timeouts. It's listed as a TODO in the
        %% source
        %% {error, timeout} ->
        %%     {stop, timeout};
        {ok, Connection} ->
            %% Link to pid so if this process dies we clean up
            %% the socket
            erlang:link(Connection),
            {ok, Prepared} = load_statements(Statements),
            set_statement_timeout(Connection, Timeout),
            {ok, #state{cn=Connection, statements=Prepared, ctrans=CTrans, default_timeout=Timeout}};
        %% I [jd] can't find any evidence of this clause in the wg/epgsql
        %%{error, {syntax, Msg}} ->
        %%    {stop, {syntax, Msg}};
        {error, Error} ->
            ErrorMsg = sqerl_pgsql_errors:translate(Error),
            error_logger:error_msg("Unable to start database connection: ~p~n", [ErrorMsg]),
            ErrorMsg
    end.

%% Internal functions

%% @doc Load prepared queries. Note that this function does not prepare the queries on the
%% connection. Instead, the query names and corresponding SQL are stored in the client's
%% state to be prepared on-demand when `execute' is called.
-spec load_statements([{atom(), binary()}]) -> {ok, sqerl_dict()}.
load_statements(Statements) ->
    AddFun = fun({Name, SQL}, Dict) when is_atom(Name), is_binary(SQL) ->
                     pqc_add(Name, SQL, Dict);
                (Entry, _) ->
                     erlang:error({invalid_statement, Entry})
             end,
    {ok, lists:foldl(AddFun, dict:new(), Statements)}.

%% @doc Load a statement. Adds the statement to client state. The statement will be prepared
%% on the connection on-demand when execute is called. If a query with the same `Name' is in
%% the query cache and has been previously prepared, it will be unloaded; this function
%% overwrites queries of same name only taking care to make sure that previously prepared
%% queries are unloaded from the connection.
-spec load_statement(connection(), atom(), sqerl_sql(), sqerl_dict()) -> {ok, sqerl_dict()}.
load_statement(Connection, Name, SQL, Dict) ->
    %% prevent leak of previously prepared query with same name
    Dict1 = case dict:find(Name, Dict) of
                error ->
                    Dict;
                {ok, #prepared_statement{}} ->
                    {ok, CleanDict} = unload_statement(Connection, Name, Dict),
                    CleanDict;
                _SQL ->
                    %% if we just have previous SQL here, we can safely ignore and overwrite it
                    Dict
            end,
    {ok, pqc_add(Name, SQL, Dict1)}.

%% PQC - Prepared Query Cache

%% @doc Add a named query to the cache blindly overwritting any existing entries.
-spec pqc_add(atom(), binary(), sqerl_dict()) -> sqerl_dict().
pqc_add(Name, Query, Cache) ->
    dict:store(Name, Query, Cache).

%% @doc Remove the named query from the cache (no cleanup of query prepared on the
%% connection will occur).
-spec pqc_remove(atom(), sqerl_dict()) -> sqerl_dict().
pqc_remove(Name, Cache) ->
    dict:erase(Name, Cache).

%% @doc Obtain a prepared query to execute against. If the query has already been prepared
%% on the connection, its record is returned. Otherwise, the cached SQL statement associated
%% with `Name' is prepared on the connection (this is the on-demand part) and the prepared
%% query record is cached.
%%
%% There are three possibilities: 1. Prepared query is in cache ready to go 2. Prepared
%% query is in raw SQL form, needs to be prepared before it's ready to use 3. We don't know
%% about such a query For case #2, we have to talk to the db and errors may result.
%%
%% NB: Take care to update client state with the returned `sqerl_dict()' value as it may contain a
%% newly cached entry.
-spec pqc_fetch(atom(), sqerl_dict(), connection()) -> {#prepared_statement{}, sqerl_dict()} |
                                                 {error, _}.
pqc_fetch(Name, Cache, Con) ->
    pqc_fetch(Name, Cache, Con, fun prepare_statement/3).

%% this version makes testing the cache functionality much easier
pqc_fetch(Name, Cache, Con, PrepareFun) ->
    pqc_fetch_internal(Name, dict:find(Name, Cache), Cache, Con, PrepareFun).

-spec pqc_fetch_internal(Name, Found, Cache, Con, PrepareFun) -> Result when
      Name :: atom(),
      Found :: {ok, binary() | PrepQ},
      Cache :: sqerl_dict(),
      Con :: connection(),
      PrepareFun :: fun((Con, Name, binary()) -> {ok, PrepQ} | {error, term()}),
      Result :: {PrepQ, sqerl_dict()} | {error, term()},
      PrepQ :: term().
pqc_fetch_internal(Name, error, _Cache, _Con, _PrepareFun) ->
    {error, {query_not_found, Name}};
pqc_fetch_internal(Name, {ok, SQL}, Cache, Con, PrepareFun) when is_binary(SQL) ->
    %% prepare it, store it, return it
    case PrepareFun(Con, Name, SQL) of
        {ok, P} ->
            {P, dict:store(Name, P, Cache)};
        Error ->
            Error
    end;
pqc_fetch_internal(_Name, {ok, PrepQ}, Cache, _Con, _PrepareFun) ->
    {PrepQ, Cache}.

%% @doc Prepare a statement on the connection. Does not manage
%% state.
-spec prepare_statement(connection(), atom(), sqerl_sql()) ->
    {ok, #prepared_statement{}} | {error, term()}.
prepare_statement(Connection, Name, SQL) when is_atom(Name) ->
    case epgsql:parse(Connection, atom_to_list(Name), SQL, []) of
        {ok, Statement} ->
            {ok, {statement, SName, Desc, DataTypes}} = epgsql:describe(Connection, Statement),
            ColumnData = [ {CN, CT} || {column, CN, CT, _, _, _} <- Desc ],
            P = #prepared_statement{
              name = SName,
              input_types = DataTypes,
              output_fields = ColumnData,
              stmt = Statement},
            {ok, P};
        {error, {error, error, _ErrorCode, Msg, Position}} ->
            {error, {syntax, {Msg, Position}}};
        Error ->
            %% TODO: Discover what errors can flow out of this, and write tests.
            {error, Error}
    end.

%% @doc Unload statement: unprepare in DB, then update statement
%% state dict.
%% Returns {ok, UpdatedDict}.
-spec unload_statement(connection(), atom(), sqerl_dict()) -> {ok, sqerl_dict()}.
unload_statement(Connection, Name, Dict) ->
        unprepare_statement(Connection, Name),
        {ok, pqc_remove(Name, Dict)}.

%% @doc Call DB to unprepare a previously prepared statement.
-spec unprepare_statement(connection(), atom()) -> ok.
unprepare_statement(Connection, Name) when is_atom(Name) ->
    SQL = list_to_binary([<<"DEALLOCATE ">>, atom_to_binary(Name, latin1)]),
    %% Have to do squery here (execute/3 uses equery which will try to prepare)
    {ok, _, _} = epgsql:squery(Connection, SQL),
    ok.


%%%
%%% Data format conversion
%%%

%% @doc Format results of a query to expected standard (list of proplists).
format_result(Columns, Rows) ->
    %% Results from simple queries return Columns, Rows
    %% Columns are records
    %% Rows are tuples
    Names = extract_column_names({result_column_data, Columns}),
    unpack_rows(Names, Rows).

%% Converts contents of result_packet into our "standard"
%% representation of a list of proplists. In other words,
%% each row is converted into a proplist and then collected
%% up into a list containing all the converted rows for
%% a given query result.
%% Result packet can be from a prepared statement execution
%% (column data is embedded in prepared statement record),
%% or from a simple query.
-spec unpack_rows(#prepared_statement{} | [sqerl_sql()], [tuple()]) -> sqerl_rows().
unpack_rows(#prepared_statement{output_fields=ColumnData}, Rows) ->
    %% Takes in a prepared statement record that
    %% holds column data that holds column names
    Columns = extract_column_names({prepared_column_data, ColumnData}),
    unpack_rows(Columns, Rows);
unpack_rows(ColumnNames, Rows) ->
    %% Takes in a list of colum names
    [lists:zip(ColumnNames, tuple_to_list(Row)) || Row <- Rows].

%% @doc Extract column names from column data.
%% Column data comes in two forms: as part of a result set,
%% or as part of a prepared statement.
%% With column data from a result set, call as
%% extract_column_names({result_column_data, Columns}).
%% With column data from a prepared statement, call as
%% extract_column_names({prepared_column_data, ColumnData}).
-spec extract_column_names({atom(), [#column{}]}) -> [any()].
extract_column_names({result_column_data, Columns}) ->
    %% For column data coming from a query result
    [Name || {column, Name, _Type, _Size, _Modifier, _Format} <- Columns];
extract_column_names({prepared_column_data, ColumnData}) ->
    %% For column data coming from a prepared statement
    [Name || {Name, _Type} <- ColumnData].

%%%
%%% Simple hooks to support coercion inputs to match the type expected by pgsql
%%%
transform(timestamp, {datetime, X}) ->
    X;
transform(timestamp, X) when is_binary(X) ->
    sqerl_transformers:parse_timestamp_to_datetime(X);
transform(_Type, X) ->
    X.

%% @doc Transform input for prepared statements.
%% Prepared statements have type data which we use
%% to transform the input.
input_transforms(Data, #prepared_statement{input_types=Types}, _State) ->
    [ transform(T, E) || {T,E} <- lists:zip(Types, Data) ].

%% @doc Transform input without query parameter type data
%% i.e. not a prepared statement. In this case we do
%% not have expected data types, so we do a more limited
%% transform (e.g. datetime type).
-spec input_transforms(list()) -> list().
input_transforms(Parameters) ->
    [transform(Parameter) || Parameter <- Parameters].

%% @doc Transform input data where applicable.
-spec transform(any()) -> any().
transform({datetime, X}) -> X;
transform(X) -> X.

-spec set_statement_timeout(connection(), integer()) -> term().
set_statement_timeout(Connection, Timeout) ->
    SQL = list_to_binary(
            lists:flatten(io_lib:format("set statement_timeout=~p", [Timeout]))),
    epgsql:squery(Connection, SQL).
