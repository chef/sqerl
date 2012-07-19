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
-export([start_link/0,
         start_link/1,
         exec_prepared_select/3,
         exec_prepared_statement/3,
         parse_error/2,
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

-include_lib("sqerl.hrl").


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
    DbConfig = read_db_config(ev(db_config)),
    Config = lists:append(
              [{host, ev(db_host)},
               {port, ev(db_port)},
               {user, ev(db_user)},
               {pass, ev(db_pass)},
               {db, ev(db_name)},
               {idle_check, IdleCheck},
               {column_transforms, ev(column_transforms)}],
              DbConfig),
    case CallbackMod:init(Config) of
        {ok, CallbackState} ->
            Timeout = IdleCheck,
            {ok, #state{cb_mod=CallbackMod, cb_state=CallbackState,
                        timeout=Timeout}, Timeout};
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

read_db_config({Mod, Fun, Args}) ->
    apply(Mod, Fun, Args);
read_db_config(Path) when is_list(Path) ->
    {ok, Config} = file:consult(Path),
    Config.

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



%% @doc Utility for generating specific message tuples from database-specific error
%% messages. Used to generate descriptive atoms from database-specific error codes.
%% Utilized by the database specific client implementations.

-spec parse_error(term(), term()) -> sqerl_error().
parse_error(StatusCode, ErrorCodes) ->
    case lists:keyfind(StatusCode, 1, ErrorCodes) of
        {_, ErrorType} ->
            ErrorType;
        false ->
            error
    end.


