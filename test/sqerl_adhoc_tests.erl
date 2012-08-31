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

-module(sqerl_adhoc_tests).

-include_lib("eunit/include/eunit.hrl").

%% select tests
%%
select_all_test() ->
    Expected = <<"SELECT * FROM users">>,
    Generated = sqerl_adhoc:select([<<"*">>], <<"users">>, all),
    ?assertEqual(Expected, Generated).

select_equals_test() ->
    Expected = <<"SELECT * FROM users WHERE id = ?">>,
    Generated = sqerl_adhoc:select([<<"*">>], <<"users">>, {<<"id">>, equals, qmark}),
    ?assertEqual(Expected, Generated).

select_in_qmark_test() -> 
    Expected = <<"SELECT Field1,Field2 FROM Table1 WHERE Field IN (?,?,?,?)">>,
    Generated = sqerl_adhoc:select([<<"Field1">>, <<"Field2">>], 
                                    <<"Table1">>, 
                                    {<<"Field">>, in, 4, qmark}),
    ?assertEqual(Expected, Generated).

select_in_dollarn_test() -> 
    Expected = <<"SELECT Field1,Field2 FROM Table1 WHERE Field IN ($1,$2,$3,$4)">>,
    Generated = sqerl_adhoc:select([<<"Field1">>, <<"Field2">>],
                                    <<"Table1">>,
                                    {<<"Field">>, in, 4, dollarn}),
    ?assertEqual(Expected, Generated).

select_in_star_test() ->
    Expected = <<"SELECT * FROM Table1 WHERE Field IN (?,?,?,?)">>,
    Generated = sqerl_adhoc:select([<<"*">>], 
                                    <<"Table1">>, 
                                    {<<"Field">>, in, 4, qmark}),
    ?assertEqual(Expected, Generated).

%% ensure_safe tests
%%
ensure_safe_string_test() ->
    ensure_safe("ABCdef123_*").

ensure_safe_binary_test() ->
    ensure_safe(<<"ABCdef123_*">>).

ensure_safe(Value) ->
    ?assertEqual(true, sqerl_adhoc:ensure_safe(Value)).

ensure_safe_bad_values_test() ->
    BadValues = "`-=[]\;',./~!@#$%^&()+{}|:\"<>?",
    %% we want to test individual values here, so iterate
    [ensure_safe_error(list_to_binary([BadValue])) || BadValue <- BadValues].

ensure_safe_error(BadValue) ->
    ?assertException(error, {badmatch, nomatch}, sqerl_adhoc:ensure_safe(BadValue)).

ensure_safe_list_test() ->
    ensure_safe(["toto", "mytable", "_Field_1", <<"binary">>]).

ensure_safe_bad_values_sets_test() ->
    BadValuesSets = [
                     ["good", "`bad`", "ok"],
                     ["ok", "'bad'", "bad;"]],
    [ensure_safe_error(BadValues) || BadValues <- BadValuesSets].


%% placeholders tests
%%
placeholders_qmark_test() ->
    ?assertEqual([<<"?">>, <<"?">>, <<"?">>],
                 sqerl_adhoc:placeholders(3, qmark)),
    ?assertEqual([<<"?">>, <<"?">>, <<"?">>, <<"?">>, <<"?">>],
                 sqerl_adhoc:placeholders(5, qmark)).

placeholders_dollarn_test() ->
    ?assertEqual([<<"$1">>, <<"$2">>, <<"$3">>],
                 sqerl_adhoc:placeholders(3, dollarn)),
    ?assertEqual([<<"$1">>, <<"$2">>, <<"$3">>, <<"$4">>, <<"$5">>, <<"$6">>],
                 sqerl_adhoc:placeholders(6, dollarn)).

placeholders_le0_test() ->
    ?assertException(error, function_clause, sqerl_adhoc:placeholders(0, qmark)),
    ?assertException(error, function_clause, sqerl_adhoc:placeholders(-2, qmark)).

placeholder_qmark_test() ->
    %% For qmark style, it should always be a qmark
    ?assertEqual(<<"?">>, sqerl_adhoc:placeholder(1, qmark)),
    ?assertEqual(<<"?">>, sqerl_adhoc:placeholder(2, qmark)),
    ?assertEqual(<<"?">>, sqerl_adhoc:placeholder(120, qmark)).

placeholder_dollarn_test() ->
    %% For dollarn style it should be "$n"
    ?assertEqual(<<"$1">>, sqerl_adhoc:placeholder(1, dollarn)),
    ?assertEqual(<<"$2">>, sqerl_adhoc:placeholder(2, dollarn)),
    ?assertEqual(<<"$5">>, sqerl_adhoc:placeholder(5, dollarn)),
    ?assertEqual(<<"$120">>, sqerl_adhoc:placeholder(120, dollarn)).

placeholder_bad_style_test() ->
    %% Anything else should cause an error
    ?assertException(error, function_clause, sqerl_adhoc:placeholder(1, unsupported)),
    ?assertException(error, function_clause, sqerl_adhoc:placeholder(4, toto)).


%% join test
%%
join_test() ->
    ?assertEqual([], sqerl_adhoc:join([], 0)),
    ?assertEqual([1], sqerl_adhoc:join([1], 0)),
    ?assertEqual([1, 0, 2, 0, 3], sqerl_adhoc:join([1, 2, 3], 0)),
    ?assertEqual([<<"1">>, <<",">>, <<"2">>, <<",">>, <<"3">>],
                 sqerl_adhoc:join([<<"1">>, <<"2">>, <<"3">>], <<",">>)).

