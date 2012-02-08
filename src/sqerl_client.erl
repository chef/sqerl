%% -*- erlang-indent-level: 4;indent-tabs-mode: nil; fill-column: 92 -*-
%% ex: ts=4 sw=4 et
%% @author Kevin Smith <kevin@opscode.com>
%% @copyright Copyright 2011 Opscode, Inc.
%% @end
%% @doc Abstraction around interacting with SQL databases
-module(sqerl_client).

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
-export([start_link/2,
         exec_prepared_select/3,
         exec_prepared_statement/3,
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
     {exec_prepared_statement, 3},
     {exec_prepared_select, 3},
     {is_connected, 1}];
behaviour_info(_) ->
    undefined.

%%% A select statement returns a list of tuples, or an error.
%%% The prepared statement to use is named by an atom.
-spec exec_prepared_select(pid(), atom(), [any()]) -> [] | [{any(), any()}] | {error, any()}.
exec_prepared_select(Cn, Name, Args) when is_pid(Cn),
                                          is_atom(Name) ->
    gen_server:call(Cn, {exec_prepared_select, Name, Args}, infinity).

%%% Unlike a select statement, this just returns an integer or an error.
-spec exec_prepared_statement(pid(), atom(), []) -> integer() | {error, any()}.
exec_prepared_statement(Cn, Name, Args) when is_pid(Cn),
                                             is_atom(Name) ->
    gen_server:call(Cn, {exec_prepared_stmt, Name, Args}, infinity).


%%% Close a connection
-spec close(pid()) -> ok.
close(Cn) ->
    gen_server:call(Cn, close).


start_link(mysql, Config) ->
    gen_server:start_link(?MODULE, [sqerl_mysql_client, Config], []);
start_link(pgsql, Config) ->
    gen_server:start_link(?MODULE, [sqerl_pgsql_client, Config], []);
start_link(CallbackMod, Config) ->
    gen_server:start_link(?MODULE, [CallbackMod, Config], []).

init([CallbackMod, Config]) ->
    case CallbackMod:init(Config) of
        {ok, CallbackState} ->
            {ok, #state{cb_mod=CallbackMod, cb_state=CallbackState,
                        timeout=proplists:get_value(idle_check, Config, 1000)}};
        Error ->
            {stop, Error}
    end.

handle_call({exec_prepared_select, Name, Args}, _From, #state{cb_mod=CBMod,
                                                              cb_state=CBState,
                                                              timeout=Timeout}=State) ->
    ?LOG_STATEMENT(Name, Args),
    {Result, NewCBState} = CBMod:exec_prepared_select(Name, Args, CBState),
    ?LOG_RESULT(Result),
    {reply, Result, State#state{cb_state=NewCBState}, Timeout};
handle_call({exec_prepared_stmt, Name, Args}, _From, #state{cb_mod=CBMod,
                                                            cb_state=CBState,
                                                            timeout=Timeout}=State) ->
    ?LOG_STATEMENT(Name, Args),
    {Result, NewCBState} = CBMod:exec_prepared_statement(Name, Args, CBState),
    ?LOG_RESULT(Result),
    {reply, Result, State#state{cb_state=NewCBState}, Timeout};
handle_call(close, _From, State) ->
    {stop, normal, ok, State};

handle_call(_Request, _From, State) ->
    {reply, ignored, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(timeout, #state{cb_mod=CBMod, cb_state=CBState}=State) ->
    case CBMod:is_connected(CBState) of
        {true, CBState1} ->
            {noreply, State#state{cb_state=CBState1}};
        false ->
            {stop, {error, bad_client}, State}
    end;
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

