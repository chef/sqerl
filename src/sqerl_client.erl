%% -*- erlang-indent-level: 4;indent-tabs-mode: nil; fill-column: 92 -*-
%% ex: ts=4 sw=4 et
%% @author Kevin Smith <kevin@opscode.com>
%% @doc Abstraction around interacting with SQL databases
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

-module(sqerl_client).

-include_lib("sqerl.hrl").

-behaviour(gen_server).

-define(LOG_STATEMENT(Name, Args), case application:get_env(sqerl, log_statements) of
                                       undefined ->
                                           ok;
                                       {ok, false} ->
                                           ok;
                                       {ok, true} ->
                                           error_logger:info_msg("(~p) Executing statement ~p with args ~p~n", [self(), Name, Args])
                                   end).

-define(LOG_RESULT(Result), case application:get_env(sqerl, log_statements) of
                                undefined ->
                                    ok;
                                {ok, false} ->
                                    ok;
                                {ok, true} ->
                                    error_logger:info_msg("(~p) Result: ~p~n", [self(), Result])
                            end).

%% API
-export([start_link/0,
         start_link/1,
         execute/2,
         execute/3,
         close/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

%% behavior callback
-export([behaviour_info/1]).

-record(state, {cb_mod,
                cb_state,
                timeout}).

%% @hidden
behaviour_info(callbacks) ->
    [{init, 1},
     {execute, 3},
     {is_connected, 1}];
behaviour_info(_) ->
    undefined.

%% @doc Execute SQL or prepared statement with no parameters.
%% See execute/3 for return values.
-spec execute(pid(), sqerl_query()) -> sqerl_results().
execute(Cn, QueryOrStatement) ->
    execute(Cn, QueryOrStatement, []).

%% @doc Execute SQL or prepared statement with parameters.
-spec execute(pid(), sqerl_query(), [any()]) -> sqerl_results().
execute(Cn, QueryOrStatement, Parameters) when is_pid(Cn) ->
    gen_server:call(Cn, {execute, QueryOrStatement, Parameters}, infinity).

%%% Close a connection
-spec close(pid()) -> ok.
close(Cn) ->
    gen_server:call(Cn, close).


start_link() ->
    gen_server:start_link(?MODULE, [], []).
start_link(DbType) ->
    gen_server:start_link(?MODULE, [DbType], []).

init([]) ->
    init(ev(db_type));
init(DbType) ->
    CallbackMod = case DbType of
                      pgsql -> sqerl_pgsql_client;
                      mysql -> sqerl_mysql_client
                  end,
    IdleCheck = ev(idle_check, 1000),
    Config = [{host, ev(db_host)},
              {port, ev(db_port)},
              {user, ev(db_user)},
              {pass, ev(db_pass)},
              {db, ev(db_name)},
              {idle_check, IdleCheck},
              {prepared_statements, read_statements(ev(prepared_statements))},
              {column_transforms, ev(column_transforms)}],
    case CallbackMod:init(Config) of
        {ok, CallbackState} ->
            Timeout = IdleCheck,
            {ok, #state{cb_mod=CallbackMod, cb_state=CallbackState,
                        timeout=Timeout}, Timeout};
        Error ->
            {stop, Error}
    end.

handle_call({Call, QueryOrStatementName, Args}, From, State) ->
    exec_driver({Call, QueryOrStatementName, Args}, From, State);
handle_call(close, _From, State) ->
    {stop, normal, ok, State};
handle_call(_Request, _From, #state{timeout=Timeout}=State) ->
    {reply, ignored, State, Timeout}.

handle_cast(_Msg, #state{timeout=Timeout}=State) ->
    {noreply, State, Timeout}.

handle_info(timeout, #state{cb_mod=CBMod, cb_state=CBState, timeout=Timeout}=State) ->
    case CBMod:is_connected(CBState) of
        {true, CBState1} ->
            {noreply, State#state{cb_state=CBState1}, Timeout};
        false ->
            error_logger:warning_msg("Failed to verify idle connection. Shutting down ~p~n",
                                     [self()]),
            {stop, shutdown, State}
    end;
handle_info(_Info, #state{timeout=Timeout}=State) ->
    {noreply, State, Timeout}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% @doc Call DB driver process
exec_driver({Call, QueryOrName, Args}, _From, #state{cb_mod=CBMod, cb_state=CBState, timeout=Timeout}=State) ->
    ?LOG_STATEMENT(QueryOrName, Args),
    {Result, NewCBState} = apply(CBMod, Call, [QueryOrName, Args, CBState]),
    ?LOG_RESULT(Result),
    {reply, Result, State#state{cb_state=NewCBState}, Timeout}.

-spec read_statements([{atom(), term()}]
                      | string()
                      | {atom(), atom(), list()}) -> [{atom(), binary()}].
%% @doc Prepared statements can be provides as a list of `{atom(), binary()}' tuples, as a
%% path to a file that can be consulted for such tuples, or as `{M, F, A}' such that
%% `apply(M, F, A)' returns the statements tuples.
read_statements({Mod, Fun, Args}) ->
    apply(Mod, Fun, Args);
read_statements(L = [{Label, SQL}|_T]) when is_atom(Label) andalso is_binary(SQL) ->
    L;
read_statements(Path) when is_list(Path) ->
    {ok, Statements} = file:consult(Path),
    Statements.

%% Short for "environment value", just provides some sugar for grabbing config values
ev(Key) ->
    case application:get_env(sqerl, Key) of
        {ok, V} -> V;
        undefined ->
            Msg = {missing_application_config, sqerl, Key},
            error_logger:error_report(Msg),
            error(Msg)
    end.
ev(Key, Default) ->
    case application:get_env(sqerl, Key) of
        undefined -> Default;
        {ok, V} -> V
    end.
