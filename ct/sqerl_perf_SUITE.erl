%% Copyright 2012 Opscode, Inc. All Rights Reserved.
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

-module(sqerl_perf_SUITE).

-exports([setup_env/0, basic_test_/0]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-compile([export_all]).

all() -> [perf_test].

init_per_testcase(_, Config) ->
    pgsql_test_buddy:clean(),
    pgsql_test_buddy:create(Config),
    pgsql_test_buddy:setup_env(),
    Config.

perf_test(_Config) ->
    adhoc_insert(10000, 1),
    adhoc_insert(10000, 10),
    adhoc_insert(10000, 100),
    adhoc_insert(100000, 100),
    adhoc_insert(100000, 1000).

adhoc_insert(N, BatchSize) ->
    Table = <<"users">>,
    Columns = [<<"first_name">>,
               <<"last_name">>],
    F = fun(I) -> list_to_binary(integer_to_list(I)) end,
    Data = [[F(I), F(I)] || I <- lists:seq(1, N)],
    adhoc_insert_delete_test(Table, Columns, Data, BatchSize).

adhoc_insert_delete_test(Table, Columns, Data, BatchSize) ->
    {Microsecs, {ok, InsertCount}} = timer:tc(sqerl, adhoc_insert, [Table, Columns, Data, BatchSize]),
    ?assertEqual(length(Data), InsertCount),
    info_msg(InsertCount, BatchSize, Microsecs),
    %% clean up
    {ok, _DeleteCount} = sqerl:adhoc_delete(Table, all).

info_msg(Count, BatchSize, Microsecs) ->
    ct:pal("Inserted ~p rows at batch size ~p in ~p microsec: ~p microsec/record~n",
                          [Count, BatchSize, Microsecs, Microsecs/Count]).
