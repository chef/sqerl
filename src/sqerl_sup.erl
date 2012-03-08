%% -*- erlang-indent-level: 4;indent-tabs-mode: nil; fill-column: 92-*-
%% ex: ts=4 sw=4 et
%% @author Kevin Smith <kevin@opscode.com>
%% @author Christopher Maier <cm@opscode.com>
%% @copyright 2012 Opscode, Inc.

-module(sqerl_sup).

-behaviour(supervisor).

%% API

-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

init([]) ->
    error_logger:info_msg("Starting sqerl supervisor~n"),
    %% Pooler is an included_application and is started as part of the sqerl supervision
    %% tree. The database client application must be started before pooler. When pooler
    %% starts, it will create client connections. However, since pooler provides a generic
    %% pooling mechanism, it does not depend (via pooler.app) on the database client
    %% applications.
    PoolerSup = {pooler_sup, {pooler_sup, start_link, []},
                 permanent, infinity, supervisor, [pooler_sup]},
    {ok, {{one_for_one, 5, 10}, [PoolerSup]}}.
