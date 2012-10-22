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

