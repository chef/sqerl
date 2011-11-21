-module(sqerl_app).

-behaviour(application).

-include_lib("eunit/include/eunit.hrl").

%% Application callbacks
-export([start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    sqerl_sup:start_link().

stop(_State) ->
    ok.
