-module(sqerl_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

init([]) ->
    {ok, {{one_for_one, 60, 10}, [{pooler_sup, {pooler_sup, start_link, []}, permanent,
                                   infinity, supervisor, [pooler_sup]}]}}.
