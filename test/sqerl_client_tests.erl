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

init(_) -> expected.

init_when_given_driver_mod_and_secrets_fun_calls_it_and_calls_drivermod_init_test() ->
     Fun = fun(db_user) -> "username";
              (db_pass) -> "password"
           end,
     Actual = sqerl_client:init(?MODULE, Fun),
     ?assertEqual(expected, Actual).
