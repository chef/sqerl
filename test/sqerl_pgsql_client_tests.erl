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
    {foreach,
     fun() ->
             meck:new(pgsql),
             ok
     end,
     fun(_) ->
             true = meck:validate(pgsql),
             meck:unload(pgsql),
             ok
     end,
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
               %% use self pid as a stand-in for the connection pid. It's mocked out, but
               %% seems sensible to pass the right data type.
               Self = self(),
               mock_prepare_statement(Self, my_query, MyQuery),
               %% First call should cause a statement to be prepared on the connection and
               %% the mocked functions to be called.
               {P, Cache2} = sqerl_pgsql_client:pqc_fetch(my_query, Cache1, Self),
               ?assert(is_prepared_statement_record(P)),

               %% Calling pqc_fetch again for the same query, should just pull from the
               %% cache. This isn't a great test, since we can't verify that the mocked
               %% functions are not getting called. As a hack, specifying no_pid should
               %% ensure a crash if the mocked function is called.
               {P2, _} = sqerl_pgsql_client:pqc_fetch(my_query, Cache2, no_pid),
               ?assertEqual(P, P2)
       end},

      {"pqc_fetch bad query syntax",
       fun() ->
               Cache0 = dict:new(),
               MyQuery = <<"DO ERROR">>,
               Cache1 = sqerl_pgsql_client:pqc_add(my_query, MyQuery, Cache0),
               %% use self pid as a stand-in for the connection pid. It's mocked out, but
               %% seems sensible to pass the right data type.
               Self = self(),
               mock_prepare_statement(Self, my_query, MyQuery),
               Ans = sqerl_pgsql_client:pqc_fetch(my_query, Cache1, Self),
               ?assertEqual({error, {syntax,{<<"stub error msg">>,20}}}, Ans)
       end}
      ]}.

%% The `#prepared_statement{}' record is private to sqerl_pgsql_client. For now, we use this
%% hack for testing purposes.
is_prepared_statement_record(X) when is_tuple(X) ->
    element(1, X) == prepared_statement;
is_prepared_statement_record(_) ->
    false.

%% Generate meck expect calls on pgsql module needed when calling
%% sqerl_pgsql_client:prepare_statement/3. It's a bummer that this ties the test to internal
%% implementation detail, but for now, at least it allows us to test the pqc_* functions.
%%
%% If `Query' is `<<"DO ERROR">>', then the `prepare_statement' call will behave as if
%% invalid SQL was passed in.
mock_prepare_statement(Con, Name, Query) ->
    NameStr = atom_to_list(Name),
    meck:expect(pgsql, parse,
                fun(Pid, QName, SQL, []) when
                          Pid =:= Con;
                          QName =:= NameStr;
                          SQL =:= Query ->
                        %% dumb magic query to trigger error state
                        case SQL of
                            <<"DO ERROR">> ->
                                {error, {error, error, 34, <<"stub error msg">>, 20}};
                            _ ->
                                {ok, stub_statement}
                        end
                end),

    meck:expect(pgsql, describe,
                fun(Pid, stub_statement) when Pid =:= Con ->
                        {ok, {statement, Name, [], stub_data_types}}
                end).
