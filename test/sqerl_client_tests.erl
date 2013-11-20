%% -*- erlang-indent-level: 4;indent-tabs-mode: nil; fill-column: 92-*-
%% ex: ts=4 sw=4 et
%% Copyright 2013 Opscode, Inc. All Rights Reserved.
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

parse_host_ipv6_enabled_test_() ->
    {setup,
     fun() ->
             application:set_env(sqerl, enable_ipv6, true),
             ok
     end,
     fun(_) -> ok end,
     [ begin
           Tests = [{"127.0.0.1",       {127, 0, 0, 1}},
                    {"localhost",       {0,0,0,0,0,0,0,1}},
                    {"192.168.208.208", {192,168,208,208}},
                    {"::1",             {0,0,0,0,0,0,0,1}}],
           [ ?_assertEqual(Addr, sqerl_client:parse_host(H)) || {H, Addr} <- Tests ]
       end]}.

parse_host_ipv6_disabled_test_() ->
    {setup,
     fun() ->
             application:set_env(sqerl, enable_ipv6, false),
             ok
     end,
     fun(_) -> ok end,
     [ begin
           Tests = [{"127.0.0.1",       {127, 0, 0, 1}},
                    {"localhost",       {127, 0, 0, 1}},
                    {"192.168.208.208", {192,168,208,208}}],
           [
            ?_assertError({bad_host, _}, sqerl_client:parse_host("::1")),
            [ ?_assertEqual(Addr, sqerl_client:parse_host(H)) || {H, Addr} <- Tests ]
           ]
       end]}.
