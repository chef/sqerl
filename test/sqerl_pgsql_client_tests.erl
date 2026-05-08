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
              ?assertEqual({error, {query_not_found, no_query}}, Ans)
      end},

     {"pqc_remove",
      fun() ->
              Cache = dict:new(),
              Cache1 = sqerl_pgsql_client:pqc_add(my_query, <<"SELECT 1">>, Cache),
              Cache2 = sqerl_pgsql_client:pqc_remove(my_query, Cache1),
              %% verify removing something not found is ok
              Cache3 = sqerl_pgsql_client:pqc_remove(my_query, Cache2),
              Ans = sqerl_pgsql_client:pqc_fetch(my_query, Cache3, self()),
              ?assertEqual({error, {query_not_found, my_query}}, Ans)
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
stub_prepare_statement(stub_pid, _Name, Query) ->
    {ok, {stub_prep_q, Query}};
stub_prepare_statement(_, _, _) ->
    error(unexpect_stub_call).

%% Tests for handle_error_response/1 with the new epgsql 6-element #error tuple format.
%%
%% Old epgsql #error record had 4 fields (severity, code, message, extra) -> 5-element tuple.
%% New epgsql #error record has 5 fields (severity, code, codename, message, extra) -> 6-element tuple.
%%
%% execute_batch/3 returns results as a list, so errors arrive as the list form:
%%   [{error, #error{}}]  =>  [{error, {error, Sev, Code, Codename, Msg, Extra}}]
%%
%% The list-form catch-all previously returned the raw 6-element tuple, which then
%% failed all downstream pattern matches in sqerl and bifrost_db, causing a
%% case_clause crash and a 500 instead of a 404 for non-existent authz targets.
handle_error_response_6tuple_test_() ->
    Msg22004 = <<"null value cannot be assigned to variable \"target_id\" declared NOT NULL">>,
    Extra22004 = [{file, <<"pl_exec.c">>}, {line, <<"5081">>},
                  {routine, <<"exec_assign_value">>}, {severity, <<"ERROR">>}],

    [
     %% --- list form (execute_batch path) ---

     {"list-form 6-tuple: generic code (22004) extracts {Code, Message}",
      fun() ->
          Input = [{error, {error, error, <<"22004">>, null_value_not_allowed,
                            Msg22004, Extra22004}}],
          ?assertEqual({error, {<<"22004">>, Msg22004}},
                       sqerl_pgsql_client:handle_error_response(Input))
      end},

     {"list-form 6-tuple: not_null_violation (23502) extracts {Code, Message}",
      fun() ->
          Msg = <<"null value in column \"foo\" violates not-null constraint">>,
          Input = [{error, {error, error, <<"23502">>, not_null_violation, Msg, []}}],
          ?assertEqual({error, {<<"23502">>, Msg}},
                       sqerl_pgsql_client:handle_error_response(Input))
      end},

     {"list-form 6-tuple: unique_violation (23505) returns {conflict, Message}",
      fun() ->
          Msg = <<"duplicate key value violates unique constraint \"foo_pkey\"">>,
          Input = [{error, {error, error, <<"23505">>, unique_violation, Msg, []}}],
          ?assertEqual({conflict, Msg},
                       sqerl_pgsql_client:handle_error_response(Input))
      end},

     {"list-form 6-tuple: foreign_key_violation (23503) returns {foreign_key, Message}",
      fun() ->
          Msg = <<"insert or update on table \"foo\" violates foreign key constraint">>,
          Input = [{error, {error, error, <<"23503">>, foreign_key_violation, Msg, []}}],
          ?assertEqual({foreign_key, Msg},
                       sqerl_pgsql_client:handle_error_response(Input))
      end},

     %% --- single form (non-batch path) - regression guard ---

     {"single-form 6-tuple: generic code (22004) extracts {Code, Message}",
      fun() ->
          Input = {error, {error, error, <<"22004">>, null_value_not_allowed,
                           Msg22004, Extra22004}},
          ?assertEqual({error, {<<"22004">>, Msg22004}},
                       sqerl_pgsql_client:handle_error_response(Input))
      end},

     {"single-form 6-tuple: unique_violation (23505) returns {conflict, Message}",
      fun() ->
          Msg = <<"duplicate key value violates unique constraint \"foo_pkey\"">>,
          Input = {error, {error, error, <<"23505">>, unique_violation, Msg, []}},
          ?assertEqual({conflict, Msg},
                       sqerl_pgsql_client:handle_error_response(Input))
      end}
    ].
