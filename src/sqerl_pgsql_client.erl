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

-include_lib("eunit/include/eunit.hrl").
-include_lib("epgsql/include/pgsql.hrl").

%% sqerl_client callbacks
-export([init/1,
         execute/3,
         is_connected/1,
         sql_parameter_style/0,
         prepare/3,
         unprepare/3]).

-record(state,  {cn,
                 statements = dict:new() :: dict(),
                 ctrans :: dict() | undefined }).

-record(prepared_statement,
        {name :: string(),
         input_types  :: any(),
         output_fields :: any(),
         stmt :: any() } ).

-type connection() :: pid().
-type state() :: any().

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
%% Row:  proplist e.g. [{<<"id">>, 1}, {<<"name">>, <<"Toto">>}]
%%
-spec execute(binary() | atom(), [any()], any()) ->
    {ok, [[{atom(), any()}]] | integer()} | {error, any()}.
execute(SQL, Parameters, #state{cn=Cn}=State) when is_binary(SQL) ->
    TParameters = input_transforms(Parameters),
    Result = pgsql:equery(Cn, SQL, TParameters),
    case Result of
        {ok, Columns, Rows} ->
            {{ok, format_result(Columns, Rows)}, State};
        {ok, Count} ->
            {{ok, Count}, State};
        {ok, Count, Columns, Rows} ->
            {{ok, {Count, format_result(Columns, Rows)}}, State};
        {error, Error} ->
            {{error, Error}, State};
        Other ->
            {{error, {unexpected_result, Other}}, State}
    end;
%% Prepared statement execution
execute(StatementName, Parameters, #state{cn=Cn, statements=Statements, ctrans=CTrans}=State) when is_atom(StatementName) ->
    PrepStmt = dict:fetch(StatementName, Statements),
    Stmt = PrepStmt#prepared_statement.stmt,
    TParameters = input_transforms(Parameters, PrepStmt, State),
    ok = pgsql:bind(Cn, Stmt, TParameters),
    %% Note: we might get partial results here for big selects!
    Result =
        try
            case pgsql:execute(Cn, Stmt) of
                {ok, Count} when is_integer(Count) ->
                    commit(Cn, Count, State);
                {ok, RowData} when is_list(RowData) ->
                    Rows = unpack_rows(PrepStmt, RowData),
                    TRows = sqerl_transformers:by_column_name(Rows, CTrans),
                    {{ok, TRows}, State};
                Other ->
                    rollback(Cn, {error, Other}, State)
            end
        catch
            _:X ->
                rollback(Cn, {error, X}, State)
        end,
    Result.

%% @doc Prepare a new statement.
-spec prepare(atom(), binary() | string(), state()) -> {ok, state()}.
prepare(Name, SQL, #state{cn=Cn, statements=Statements}=State) ->
    {ok, UpdatedStatements} = load_statement(Cn, Name, SQL, Statements),
    UpdatedState = State#state{statements=UpdatedStatements},
    {ok, UpdatedState}.

%% @doc Unprepare a previously prepared statement
%% Interface uses 3 parameters. In this case the 
%% second one is unused.
unprepare(Name, _, State) ->
    unprepare(Name, State).

-spec unprepare(atom(), state()) -> {ok, state()}.
unprepare(Name, #state{cn=Cn, statements=Statements}=State) ->
    {ok, UpdatedStatements} = unload_statement(Cn, Name, Statements),
    UpdatedState = State#state{statements=UpdatedStatements},
    {ok, UpdatedState}.

%% @doc Return SQL parameter placeholder style.
%% See sqerl_sql:placedholder
sql_parameter_style() -> dollarn.


is_connected(#state{cn=Cn}=State) ->
    case catch pgsql:squery(Cn, ?PING_QUERY) of
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
    {db, Db} = lists:keyfind(db, 1, Config),
    {prepared_statements, Statements} = lists:keyfind(prepared_statements, 1, Config),
    Opts = [{database, Db}, {port, Port}],
    CTrans =
        case lists:keyfind(column_transforms, 1, Config) of
            {column_transforms, CT} -> CT;
            false -> undefined
        end,
    case pgsql:connect(Host, User, Pass, Opts) of
        {error, timeout} ->
            {stop, timeout};
        {ok, Connection} ->
            %% Link to pid so if this process dies we clean up
            %% the socket
            erlang:link(Connection),
            erlang:process_flag(trap_exit, true),
            {ok, Prepared} = load_statements(Connection, Statements, dict:new()),
            {ok, #state{cn=Connection, statements=Prepared, ctrans=CTrans}};
        {error, {syntax, Msg}} ->
            {stop, {syntax, Msg}};
        X ->
            error_logger:error_report(X),
            {stop, X}
    end.

%% Internal functions

-spec load_statements(connection(), [tuple()], dict()) -> {ok, dict()} |  {error, any()}.
load_statements(_Connection, [], Dict) ->
    {ok, Dict};
load_statements(Connection, [{Name, SQL}|T], Dict) when is_atom(Name) ->
    case load_statement(Connection, Name, SQL, Dict) of
        {ok, UpdatedDict} -> load_statements(Connection, T, UpdatedDict);
        Other -> {error, Other}
    end.

%% @doc Load a statement: prepare and store in statement state dict.
%% Returns {ok, UpdatedDict} or {error, ErrorInfo}
-spec load_statement(connection(), atom(), binary(), dict()) -> {ok, dict()}.
load_statement(Connection, Name, SQL, Dict) ->
    case prepare_statement(Connection, Name, SQL) of
        {ok, {Name, P}} ->
            {ok, dict:store(Name, P, Dict)};
        {error, {error, error, _ErrorCode, Msg, Position}} ->
            {error, {syntax, {Msg, Position}}};
        Error ->
            %% TODO: Discover what errors can flow out of this, and write tests.
            {error, Error}
    end.

%% @doc Prepare a statement on the connection. Does not manage
%% state.
-spec prepare_statement(connection(), atom(), binary()) ->
    {ok, {atom(), term()}} | {error, term()}.
prepare_statement(Connection, Name, SQL) when is_atom(Name) ->
    case pgsql:parse(Connection, atom_to_list(Name), SQL, []) of
        {ok, Statement} ->
            {ok, {statement, SName, Desc, DataTypes}} = pgsql:describe(Connection, Statement),
            ColumnData = [ {CN, CT} || {column, CN, CT, _, _, _} <- Desc ],
            P = #prepared_statement{
              name = SName,
              input_types = DataTypes,
              output_fields = ColumnData,
              stmt = Statement},
            {ok, {Name, P}};
        {error, {error, error, _ErrorCode, Msg, Position}} ->
            {error, {syntax, {Msg, Position}}};
        Error ->
            %% TODO: Discover what errors can flow out of this, and write tests.
            {error, Error}
    end.

%% @doc Unload statement: unprepare in DB, then update statement
%% state dict.
%% Returns {ok, UpdatedDict}.
unload_statement(Connection, Name, Dict) ->
        unprepare_statement(Connection, Name),
        UpdatedDict = dict:erase(Name, Dict),
        {ok, UpdatedDict}.

%% @doc Call DB to unprepare a previously prepared statement.
-spec unprepare_statement(connection(), atom()) -> ok.
unprepare_statement(Connection, Name) when is_atom(Name) ->
    SQL = list_to_binary([<<"DEALLOCATE ">>, atom_to_binary(Name, latin1)]),
    %% Have to do squery here (execute/3 uses equery which will try to prepare)
    {ok, _, _} = pgsql:squery(Connection, SQL),
    ok.


%%%
%%% Data format conversion
%%%

%% @doc Format results of a query to expected standard (list of proplists).
-spec format_result([term()], [term()]) -> any().
format_result(Columns, Rows) ->
    %% Results from simple queries return Columns, Rows
    %% Columns are records
    %% Rows are tuples
    Names = extract_column_names({result_column_data, Columns}),
    unpack_rows(Names, Rows).

format_result_test() ->
    Columns = [{column, <<"id">>, int4, 4, -1, 0},
               {column, <<"first_name">>, varchar, -1, 84, 0}],
    Rows = [{<<1>>, <<"Kevin">>},
            {<<2>>, <<"Mark">>}],
    Output = format_result(Columns, Rows),
    ExpectedOutput = [[{<<"id">>, <<1>>},
                       {<<"first_name">>, <<"Kevin">>}],
                      [{<<"id">>, <<2>>},
                       {<<"first_name">>, <<"Mark">>}]],
    ?assertEqual(ExpectedOutput, Output).

%% Converts contents of result_packet into our "standard"
%% representation of a list of proplists. In other words,
%% each row is converted into a proplist and then collected
%% up into a list containing all the converted rows for
%% a given query result., 
%% Result packet can be from a prepared statement execution
%% (column data is embedded in prepared statement record),
%% or from a simple query
-spec unpack_rows(any(), [[any()]]) -> [[{any(), any()}]].
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
-spec extract_column_names({atom(), [tuple()]}) -> [any()].
extract_column_names({result_column_data, Columns}) ->
    %% For column data coming from a query result
    [Name || {column, Name, _Type, _Size, _Modifier, _Format} <- Columns];
extract_column_names({prepared_column_data, ColumnData}) ->
    %% For column data coming from a prepared statement
    [Name || {Name, _Type} <- ColumnData].

extract_column_names_test() ->
    Type = result_column_data,
    Columns = [{column,<<"id">>,int4,4,-1,0},
               {column,<<"first_name">>,varchar,-1,84,0}],
    ExpectedOutput = [<<"id">>, <<"first_name">>],
    Output = extract_column_names({Type, Columns}),
    ?assertEqual(ExpectedOutput, Output).

%%%
%%% Simple hooks to support coercion inputs to match the type expected by pgsql
%%%
transform(timestamp, {{_Y, _M, _D}, {_H, _M, _S}}=TS) ->
    sqerl_transformers:convert_YMDHMS_tuple_to_datetime(TS);
transform(timestamp, {datetime, X}) ->
    X;
transform(timestamp, X) when is_binary(X) ->
    sqerl_transformers:parse_timestamp_to_datetime(X);
transform(_Type, X) ->
    X.

input_transforms(Data, #prepared_statement{input_types=Types}, _State) ->
    [ transform(T, E) || {T,E} <- lists:zip(Types, Data) ].

%% @doc Transform input without query parameter type data
%% (i.e. not a prepared statement).
-spec input_transforms(list()) -> list().
input_transforms(Parameters) ->
    [transform(Parameter) || Parameter <- Parameters].

%% @doc Transform input data where applicable.
-spec transform(any()) -> any().
transform({datetime, X}) -> X;
transform(X) -> X.

commit(Cn, Result, State) ->
    case pgsql:squery(Cn, "COMMIT") of
        {ok, [], []} ->
            {{ok, Result}, State};
        Error when is_tuple(Error) ->
            {Error, State}
    end.

rollback(Cn, Error, State) ->
    case pgsql:squery(Cn, "ROLLBACK") of
        {ok, [], []} ->
            {Error, State};
        _Err ->
            {Error, State}
    end.
