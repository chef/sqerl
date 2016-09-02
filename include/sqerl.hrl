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

-define(ALL(Record), {rows_as_records, [Record, record_info(fields, Record)]}).

-define(FIRST(Record), {first_as_record, [Record, record_info(fields, Record)]}).

-type sqerl_error() :: {error, term()} |
                        {conflict, term()} |
                        {foreign_key, term()}.

-type sqerl_row() :: [{binary(), term()}].
-type sqerl_rows() :: [sqerl_row()].
-type sqerl_sql() :: string() | binary().
-type sqerl_query() :: atom() | sqerl_sql().
-type sqerl_results() :: {ok, integer() | sqerl_rows()} |
                         {ok, integer(), sqerl_rows()} |
                         {error, atom() | tuple()}.

-ifdef(namespaced_types).
-type sqerl_dict() :: dict:dict().
-else.
-type sqerl_dict() :: dict().
-endif.

-define(SQERL_DEFAULT_BATCH_SIZE, 100).
-define(SQERL_ADHOC_INSERT_STMT_ATOM, '__adhoc_insert').
