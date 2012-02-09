%% -*- erlang-indent-level: 4;indent-tabs-mode: nil; fill-column: 92-*-
%% ex: ts=4 sw=4 et
%% @author Kevin Smith <kevin@opscode.com>
%% @author Christopher Maier <cm@opscode.com>
%% @copyright 2012 Opscode, Inc.

-module(sqerl_sup).

-behaviour(supervisor).

%% API
-export([start_link/1]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

start_link(Config) ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, Config).

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

init(PoolerConfig) ->
    error_logger:info_msg("Starting sqerl supervisor~n"),
    PoolerSup = {pooler_sup, {pooler_sup, start_link, [PoolerConfig]},
                 permanent, infinity, supervisor, [pooler_sup]},

    {ok, {{one_for_one, 5, 10}, [PoolerSup]}}.
