%% Copyright 2017 CHEF Software, Inc. All Rights Reserved.
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

-module(sqerl_client_tests).

-include_lib("eunit/include/eunit.hrl").

-compile([export_all]).

-define(DB_HOST, "localhost").
-define(DB_PORT, 5432).
-define(DB_USER, "elmo").
-define(DB_PASS, "sesame").
-define(DB_NAME, "street").
-define(DB_TIMEOUT, 200).
-define(COLUMN_TRANSFORMS, []).
-define(IDLE_CHECK, 201).

-define(EXPECTED_CONFIG, [{idle_check, ?DB_TIMEOUT}]).
config(args) -> ?EXPECTED_CONFIG.

%% for first test case
init(?EXPECTED_CONFIG) -> {ok, expected_driver_state};
%% for second test case
init(Config) ->
    [
     ?assertEqual(Expected, proplists:get_value(Key, Config)) ||
                  {Expected, Key} <- [{?DB_USER, user},
                                      {?DB_NAME, db},
                                      {?DB_TIMEOUT, timeout},
                                      {?DB_PORT, port},
                                      {?IDLE_CHECK, idle_check},
                                      %% host_to_ip special treatment
                                      {{127,0,0,1}, host}
                                     ]
    ],
    {ok, expected_driver_state}.

-record(state, {cb_mod,
                cb_state,
                timeout = 5000 :: pos_integer()}).

init_fetches_driver_mod_and_config_mod_from_env_and_calls_them_test() ->
    application:set_env(sqerl, db_driver_mod, ?MODULE),
    application:set_env(sqerl, config_cb, {?MODULE, config, [args]}),
    Actual = sqerl_client:init([]),
    ExpectedState = #state{cb_mod = ?MODULE,
                           cb_state = expected_driver_state,
                           timeout = ?DB_TIMEOUT},
    ?assertEqual({ok, ExpectedState, ?DB_TIMEOUT}, Actual),
    cleanup_env().

init_when_using_default_config_mod_uses_application_environment_to_get_configuration_values_test() ->
    application:set_env(sqerl, db_driver_mod, ?MODULE),
    application:unset_env(sqerl, config_cb),
    application:set_env(sqerl, db_host, ?DB_HOST),
    application:set_env(sqerl, db_port, ?DB_PORT),
    application:set_env(sqerl, db_user, ?DB_USER),
    application:set_env(sqerl, db_pass, ?DB_PASS),
    application:set_env(sqerl, db_name, ?DB_NAME),
    application:set_env(sqerl, db_timeout, ?DB_TIMEOUT),
    application:set_env(sqerl, column_transforms, ?COLUMN_TRANSFORMS),
    application:set_env(sqerl, prepared_statements, {sqerl_rec, statements, [[kitchen, cook]]}),
    application:set_env(sqerl, idle_check, ?IDLE_CHECK),
    Actual = sqerl_client:init([]),
    ExpectedState = #state{cb_mod = ?MODULE,
                           cb_state = expected_driver_state,
                           timeout = ?IDLE_CHECK},
    ?assertEqual({ok, ExpectedState, ?IDLE_CHECK}, Actual),
    cleanup_env().

cleanup_env() ->
    application:unset_env(sqerl, config_cb),
    application:unset_env(sqerl, db_driver_mod).
