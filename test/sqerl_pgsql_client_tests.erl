%% -*- erlang-indent-level: 4;indent-tabs-mode: nil; fill-column: 92-*-
%% ex: ts=4 sw=4 et
%% @author Jean-Philippe Langlois <jpl@opscode.com>
%% Copyright 2011-2012 Opscode, Inc. All Rights Reserved.
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

-module(sqerl_pgsql_client_tests).

-include_lib("eunit/include/eunit.hrl").

format_result_test() ->
    Columns = [{column, <<"id">>, int4, 4, -1, 0},
               {column, <<"first_name">>, varchar, -1, 84, 0}],
    Rows = [{<<1>>, <<"Kevin">>},
            {<<2>>, <<"Mark">>}],
    Output = sqerl_pgsql_client:format_result(Columns, Rows),
    ExpectedOutput = [[{<<"id">>, <<1>>},
                       {<<"first_name">>, <<"Kevin">>}],
                      [{<<"id">>, <<2>>},
                       {<<"first_name">>, <<"Mark">>}]],
    ?assertEqual(ExpectedOutput, Output).

extract_column_names_test() ->
    Type = result_column_data,
    Columns = [{column,<<"id">>,int4,4,-1,0},
               {column,<<"first_name">>,varchar,-1,84,0}],
    ExpectedOutput = [<<"id">>, <<"first_name">>],
    Output = sqerl_pgsql_client:extract_column_names({Type, Columns}),
    ?assertEqual(ExpectedOutput, Output).

prepared_query_cache_test_() ->
    [
     {"pqc_fetch query_not_found",
      fun() ->
              Ans = sqerl_pgsql_client:pqc_fetch(no_query, dict:new(), self()),
              ?assertEqual({error, query_not_found}, Ans)
      end},

     {"pqc_remove",
      fun() ->
              Cache = dict:new(),
              Cache1 = sqerl_pgsql_client:pqc_add(my_query, <<"SELECT 1">>, Cache),
              Cache2 = sqerl_pgsql_client:pqc_remove(my_query, Cache1),
              %% verify removing something not found is ok
              Cache3 = sqerl_pgsql_client:pqc_remove(my_query, Cache2),
              Ans = sqerl_pgsql_client:pqc_fetch(my_query, Cache3, self()),
              ?assertEqual({error, query_not_found}, Ans)
      end},

     {"pqc_fetch",
      fun() ->
              Cache0 = dict:new(),
              MyQuery = <<"SELECT 1">>,
              Cache1 = sqerl_pgsql_client:pqc_add(my_query, MyQuery, Cache0),

              %% First call should cause a statement to be prepared on the connection
              {P, Cache2} = sqerl_pgsql_client:pqc_fetch(my_query, Cache1, stub_pid,
                                                         fun stub_prepare_statement/3),
              ?assertEqual({stub_prep_q, MyQuery}, P),
              %% Calling pqc_fetch again for the same query, should just pull from the
              %% cache. By passing crash_pid, we ensure that the test only passes if
              %% stub_prepare_statement is not called.
              {P2, _} = sqerl_pgsql_client:pqc_fetch(my_query, Cache2, crash_pid,
                                                     fun stub_prepare_statement/3),
              ?assertEqual(P, P2),

              %% Finally, we test that adding the same query again, resets the cache. Note
              %% that cleaning up prepared queries is currently handled outside of pqc_add.
              NewQuery = <<"SELECT 2">>,
              Cache3 = sqerl_pgsql_client:pqc_add(my_query, NewQuery, Cache2),
              {P3, _} = sqerl_pgsql_client:pqc_fetch(my_query, Cache3, stub_pid,
                                                     fun stub_prepare_statement/3),
              ?assertEqual({stub_prep_q, NewQuery}, P3)
      end},

     {"pqc_fetch bad query syntax",
      fun() ->
              Cache0 = dict:new(),
              MyQuery = <<"DO ERROR">>,
              Cache1 = sqerl_pgsql_client:pqc_add(my_query, MyQuery, Cache0),
              Ans = sqerl_pgsql_client:pqc_fetch(my_query, Cache1, stub_pid,
                                                 fun stub_prepare_statement/3),
              ?assertEqual({error, stub_error}, Ans)
      end}
    ].

stub_prepare_statement(stub_pid, _Name, <<"DO ERROR">>) ->
    {error, stub_error};
stub_prepare_statement(stub_pid, Name, Query) ->
    {ok, {Name, {stub_prep_q, Query}}};
stub_prepare_statement(_, _, _) ->
    error(unexpect_stub_call).
