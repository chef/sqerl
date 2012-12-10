%% -*- erlang-indent-level: 4;indent-tabs-mode: nil; fill-column: 92 -*-
%% ex: ts=4 sw=4 et
%% @author Kevin Smith <kevin@opscode.com>
%% @author James Casey <james@opscode.com>
%% @doc Abstraction around interacting with mysql databases
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

-module(sqerl_mysql_client).

-behaviour(sqerl_client).

-include_lib("sqerl.hrl"). % for types
-include_lib("emysql/include/emysql.hrl").

%% sqerl_client callbacks
-export([init/1,
         prepare/3,
         unprepare/3,
         execute/3,
         is_connected/1,
         sql_parameter_style/0]).

-record(state, {cn,
                ctrans :: dict() | undefined }).

-define(PING_QUERY, <<"SELECT 'pong' as ping LIMIT 1">>).

%% See sqerl_adhoc:placeholder
-spec sql_parameter_style() -> qmark.
sql_parameter_style() -> qmark.

%% @doc Prepare a statement
%%
-spec prepare(atom(), binary(), term()) -> {ok, term()}.
prepare(Name, SQL, #state{cn=_Cn}=State) ->
    ok = emysql:prepare(Name, SQL),
    {ok, State}.

%% @doc Unprepare a previously prepared statement
%% Protocol between sqerl_client and db-specific modules
%% uses 3 parameters (QueryOrName, Args, State) for all
%% calls for simplicity. For an unprepare call, there are
%% no arguments, so the second parameter of the function
%% is unused.
-spec unprepare(atom(), [], term()) -> {ok, term()}.
unprepare(_Name, _Args, #state{cn=_Cn}=State) ->
    %% unsupported by emysql.
    %% but emysql can re-prepare statements,
    %% so as long as we re-use statements
    %% it should be ok.
    {ok, State}.

%% @doc execute query or prepared statement.
%% Emysql has a common interface for both queries and prepared statements.
-spec execute(StatementOrQuery :: sqerl_query(), 
              Parameters :: [any()], 
              State :: #state{}) -> {sqerl_results(), #state{}}.
execute(NameOrQuery, Args, #state{cn=Cn}=State) ->
    TArgs = input_transforms(Args, State),
    try  emysql_conn:execute(Cn, NameOrQuery, TArgs) of
        #result_packet{}=Result ->
            process_result_packet(Result, State);
        #ok_packet{affected_rows=Count} ->
            {{ok, Count}, State};
        #error_packet{code=StatusCode, msg=Message} ->
            {{error, {StatusCode, Message}}, State};
        %% This next clause is because MySQL's stored procedures are ridiculous and can
        %% actually return multiple result sets.
        %%
        %% NOTE: As formulated, this works for our current stored procedure needs, in which
        %% SPs treated like 'select' statements return a single, well-defined table as a
        %% result; that 'select' is returned as a result_packet, and then an 'ok_packet' for
        %% the termination of the SP call itself is returned (thus, the list).
        %%
        %% This case clause WILL NOT WORK with SPs that return multiple result sets (and thus,
        %% multiple result_packets), but we will never write something that does that, so I
        %% don't foresee this as being a problem.
        [#result_packet{}=Result, #ok_packet{}] ->
            process_result_packet(Result, State)
    catch
        exit:{_, closed} ->
            %% Socket was unexpectedly closed by server
            {{error, closed}, State};
        _:Error ->
            exit(Error)
    end.

is_connected(#state{cn=Cn}=State) ->
    case catch emysql_conn:execute(Cn, ?PING_QUERY, []) of
        #result_packet{} ->
            {true, State};
        _R ->
            false
    end.

init(Config) ->
    {host, Host} = lists:keyfind(host, 1, Config),
    {port, Port} = lists:keyfind(port, 1, Config),
    {user, User} = lists:keyfind(user, 1, Config),
    {pass, Pass} = lists:keyfind(pass, 1, Config),
    {db, Db} = lists:keyfind(db, 1, Config),
    {prepared_statements, Statements} = lists:keyfind(prepared_statements, 1, Config),
    CTrans =
        case lists:keyfind(column_transforms, 1, Config) of
            {column_transforms, CT} -> CT;
            false -> undefined
        end,
    %% Need this hokey pool record to create a database connection
    %% Note: we use encoding=latin1 to force Emysql to not do any encoding.
    %% We would normally use utf8 but it causes Emysql to barf on binary
    %% data (e.g. gzipped).
    PoolDescriptor = #pool{host=Host, port=Port, user=User, password=Pass,
                           database=Db, encoding=latin1},
    case catch emysql_conn:open_connection(PoolDescriptor) of
        {'EXIT', Error} ->
            error_logger:error_report(Error),
            {stop, Error};
        #emysql_connection{socket=Sock}=Connection ->
            %% Link to socket so if this process dies we clean up
            %% the socket
            erlang:link(Sock),
            erlang:process_flag(trap_exit, true),
            ok = load_statements(Statements),
            {ok, #state{cn=Connection, ctrans=CTrans}}
    end.

%% Internal functions
load_statements([]) ->
    ok;
load_statements([{Name, SQL}|T]) ->
    case emysql:prepare(Name, SQL) of
        ok ->
            load_statements(T);
        Error ->
            error_logger:error_msg("Error preparing statement ~s ~p~n",[Name, Error]),
            Error
    end.

%% Converts contents of result_packet into our "standard"
%% representation of a list of proplists. In other words,
%% each row is converted into a proplist and then collected
%% up into a list containing all the converted rows for
%% a given query result.
unpack_rows(#result_packet{field_list=Fields, rows=Rows}) ->
    FieldNames = [ Field#field.name || Field <- Fields ],
    unpack_rows(FieldNames, Rows, []).

unpack_rows(_FieldNames, [], []) ->
    [];
unpack_rows(_FieldNames, [], Accum) ->
    lists:reverse(Accum);
unpack_rows(FieldNames, [Values|T], Accum) ->
    Row = lists:zip(FieldNames, Values),
    unpack_rows(FieldNames, T, [Row|Accum]).

%% unpack_rows(Fields, [Values|T], Accum) ->
%%     F = fun(Field, {Idx, Row}) ->
%%                 {Idx + 1, [{Field#field.name, lists:nth(Idx, Values)}|Row]} end,
%%     {_, Row} = lists:foldl(F, {1, []}, Fields),
%%     unpack_rows(Fields, T, [lists:reverse(Row)|Accum]).

transform(false) ->
    0;
transform(true) ->
    1;
transform(X) ->
    X.

input_transforms(Data, _State) ->
    [ transform(C) || C <- Data ].

%% @doc Utility function for munging a MySQL result packet.  Used for SELECTs as well as for
%% stored procedure calls that return a result set.
process_result_packet(#result_packet{}=Result,
                      #state{ctrans=CTrans}=State) ->
    Rows = unpack_rows(Result),
    TRows = sqerl_transformers:by_column_name(Rows, CTrans),
    {{ok, TRows}, State}.
