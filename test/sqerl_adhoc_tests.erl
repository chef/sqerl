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
select_undefined_test() ->
    Expected = {<<"SELECT * FROM users">>, []},
    Generated = sqerl_adhoc:select([<<"*">>], <<"users">>, [], dollarn),
    ?assertEqual(Expected, Generated).

select_all_test() ->
    Expected = {<<"SELECT * FROM users">>, []},
    Generated = sqerl_adhoc:select([<<"*">>], <<"users">>, [{where, all}], dollarn),
    ?assertEqual(Expected, Generated).

select_equals_test() ->
    select_op_test(equals, <<"=">>).

select_nequals_test() ->
    select_op_test(nequals, <<"!=">>).

select_gt_test() ->
    select_op_test(gt, <<">">>).

select_gte_test() ->
    select_op_test(gte, <<">=">>).

select_lt_test() ->
    select_op_test(lt, <<"<">>).

select_lte_test() ->
    select_op_test(lte, <<"<=">>).

select_op_test(OpAtom, OpString) ->
    Expected = {<<"SELECT * FROM users WHERE id ", OpString/binary, " $1">>, [1]},
    Generated = sqerl_adhoc:select([<<"*">>], <<"users">>, [{where, {<<"id">>, OpAtom, 1}}], dollarn),
    ?assertEqual(Expected, Generated).

select_in_dollarn_test() ->
    Values = [<<"1">>, <<"2">>, <<"3">>, <<"4">>],
    Expected = {<<"SELECT Field1,Field2 FROM Table1 WHERE Field IN ($1,$2,$3,$4)">>, Values},
    Generated = sqerl_adhoc:select([<<"Field1">>, <<"Field2">>],
                                    <<"Table1">>,
                                    [{where, {<<"Field">>, in, Values}}], dollarn),
    ?assertEqual(Expected, Generated).

select_in_star_test() ->
    Values = [<<"1">>, <<"2">>, <<"3">>, <<"4">>],
    Expected = {<<"SELECT * FROM Table1 WHERE Field IN ($1,$2,$3,$4)">>, Values},
    Generated = sqerl_adhoc:select([<<"*">>], 
                                    <<"Table1">>, 
                                    [{where, {<<"Field">>, in, Values}}], dollarn),
    ?assertEqual(Expected, Generated).

select_notin_test() ->
    Values = [<<"1">>, <<"2">>, <<"3">>, <<"4">>],
    Expected = {<<"SELECT * FROM Table1 WHERE Field NOT IN ($1,$2,$3,$4)">>, Values},
    Generated = sqerl_adhoc:select([<<"*">>], 
                                    <<"Table1">>, 
                                    [{where, {<<"Field">>, notin, Values}}], dollarn),
    ?assertEqual(Expected, Generated).

select_and_test() ->
    select_logic_test('and', <<"AND">>).

select_or_test() ->
    select_logic_test('or', <<"OR">>).

select_logic_test(OpAtom, OpString) ->
    Where1 = {<<"Field1">>, equals, 1},
    Where2 = {<<"Field2">>, equals, <<"2">>},
    Where3 = {<<"Field3">>, in, [3, 4, 5]},
    Where = {OpAtom, [Where1, Where2, Where3]},
    ExpectedValues = [1, <<"2">>, 3, 4, 5],
    ExpectedSQL = <<"SELECT Field4 FROM Table1 WHERE (Field1 = $1 ",
                    OpString/binary,
                    " Field2 = $2 ",
                    OpString/binary,
                    " Field3 IN ($3,$4,$5))">>,
    {SQL, Values} = sqerl_adhoc:select([<<"Field4">>], <<"Table1">>, [{where, Where}], dollarn),
    ?assertEqual(ExpectedSQL, SQL),
    ?assertEqual(ExpectedValues, Values).

select_not_test() ->
    Value1 = <<"V">>,
    Where1 = {<<"Field1">>, equals, Value1},
    Where = {'not', Where1},
    Expected = {<<"SELECT id FROM t WHERE NOT (Field1 = $1)">>, [Value1]},
    Actual = sqerl_adhoc:select([<<"id">>], <<"t">>, [{where, Where}], dollarn),
    ?assertEqual(Expected, Actual).

select_complex_test() ->
    Actual = sqerl_adhoc:select([<<"id">>, <<"name">>],
                                <<"t">>,
                                [{where, {'or',[
                                                {'and', [
                                                         {<<"id">>, in, [1, 2, 3]},
                                                         {<<"name">>, nequals, <<"Toto">>}
                                                        ]},
                                                {<<"name">>, equals, <<"Boss">>}
                                               ]}
                                 },
                                 {order_by, {[<<"id">>, <<"name">>], desc}},
                                 {limit, {20, offset, 40}}
                                ],
                                dollarn),
    ExpectedValues = [1, 2, 3, <<"Toto">>, <<"Boss">>],
    ExpectedSQL = <<"SELECT id,name FROM t ",
                    "WHERE ((id IN ($1,$2,$3) AND name != $4) OR name = $5) ",
                    "ORDER BY id, name DESC LIMIT 20 OFFSET 40">>,
    Expected = {ExpectedSQL, ExpectedValues},
    ?assertEqual(Expected, Actual).

select_string_test() ->
    %% tests using strings instead of binary
    Actual = sqerl_adhoc:select(["id", "name"],
                                "t",
                                [{where, {'or',[
                                                {'and', [
                                                         {"id", in, [1, 2, 3]},
                                                         {"name", nequals, "Toto"}
                                                        ]},
                                                {"name", equals, "Boss"}
                                               ]}
                                 },
                                 {order_by, {["id", "name"], desc}},
                                 {limit, {20, offset, 40}}
                                ],
                                dollarn),
    ExpectedValues = [1, 2, 3, "Toto", "Boss"],
    ExpectedSQL = <<"SELECT id,name FROM t ",
                    "WHERE ((id IN ($1,$2,$3) AND name != $4) OR name = $5) ",
                    "ORDER BY id, name DESC LIMIT 20 OFFSET 40">>,
    Expected = {ExpectedSQL, ExpectedValues},
    ?assertEqual(Expected, Actual).

select_atom_test() ->
    %% tests using atoms instead of binary
    Actual = sqerl_adhoc:select([id, name],
                                t,
                                [{where, {'or',[
                                                {'and', [
                                                         {id, in, [1, 2, 3]},
                                                         {name, nequals, 'Toto'}
                                                        ]},
                                                {name, equals, 'Boss'}
                                               ]}
                                 },
                                 {order_by, {[id, name], desc}},
                                 {limit, {20, offset, 40}}
                                ],
                                dollarn),
    ExpectedValues = [1, 2, 3, 'Toto', 'Boss'],
    ExpectedSQL = <<"SELECT id,name FROM t ",
                    "WHERE ((id IN ($1,$2,$3) AND name != $4) OR name = $5) ",
                    "ORDER BY id, name DESC LIMIT 20 OFFSET 40">>,
    Expected = {ExpectedSQL, ExpectedValues},
    ?assertEqual(Expected, Actual).

group_by_test() ->
    Fields = [<<"F1">>, <<"F2">>],
    Actual = sqerl_adhoc:group_by_sql(Fields),
    Expected = <<" GROUP BY F1, F2">>,
    ?assertEqual(Expected, Actual).

group_by_undefined_test() ->
    Actual = sqerl_adhoc:group_by_sql(undefined),
    Expected = <<"">>,
    ?assertEqual(Expected, Actual).

order_by_test() ->
    Fields = [<<"F1">>, <<"F2">>],
    Actual = sqerl_adhoc:order_by_sql(Fields),
    Expected = <<" ORDER BY F1, F2 ASC">>,
    ?assertEqual(Expected, Actual).

order_by_desc_test() ->
    Fields = [<<"F1">>, <<"F2">>],
    Direction = desc,
    Actual = sqerl_adhoc:order_by_sql({Fields, Direction}),
    Expected = <<" ORDER BY F1, F2 DESC">>,
    ?assertEqual(Expected, Actual).

order_by_undefined_test() ->
    Actual = sqerl_adhoc:order_by_sql(undefined),
    Expected = <<"">>,
    ?assertEqual(Expected, Actual).

order_by_string_test() ->
    Fields = ["F1", "F2"],
    Actual = sqerl_adhoc:order_by_sql(Fields),
    Expected = <<" ORDER BY F1, F2 ASC">>,
    ?assertEqual(Expected, Actual).

order_by_atom_test() ->
    Fields = [f1, f2],
    Actual = sqerl_adhoc:order_by_sql(Fields),
    Expected = <<" ORDER BY f1, f2 ASC">>,
    ?assertEqual(Expected, Actual).

limit_test() ->
    ?assertEqual(<<" LIMIT 100">>, sqerl_adhoc:limit_sql(100)).

limit_undefined_test() ->
    ?assertEqual(<<"">>, sqerl_adhoc:limit_sql(undefined)).

limit_offset_test() ->
    ?assertEqual(<<" LIMIT 100 OFFSET 200">>, sqerl_adhoc:limit_sql({100, offset, 200})).

%% delete tests
%%
delete_test() ->
    Values = [<<"A">>, <<"B">>, <<"C">>],
    Expected = {<<"DELETE FROM users WHERE name IN ($1,$2,$3)">>, Values},
    Actual = sqerl_adhoc:delete(<<"users">>, {<<"name">>, in, Values}, dollarn),
    ?assertEqual(Expected, Actual).

%% update tests
%%
update_test() ->
    Expected = {<<"UPDATE t SET f1 = $1, f2 = $2 WHERE id = $3">>, [1, 2, 3]},
    Actual = sqerl_adhoc:update(<<"t">>, [{<<"f1">>, 1}, {<<"f2">>, 2}], {<<"id">>, equals, 3}, dollarn),
    ?assertEqual(Expected, Actual).

%% insert tests
%%
insert_test() ->
    Actual = sqerl_adhoc:insert(<<"users">>, [<<"id">>, <<"name">>], 3, dollarn),
    Expected = <<"INSERT INTO users ( id,name ) VALUES ($1,$2),($3,$4),($5,$6)">>,
    ?assertEqual(Expected, Actual).

insert_single_row_test() ->
    Actual = sqerl_adhoc:insert(<<"users">>, [<<"id">>, <<"name">>], 1, dollarn),
    Expected = <<"INSERT INTO users ( id,name ) VALUES ($1,$2)">>,
    ?assertEqual(Expected, Actual).

insert_qmark_test() ->
    Actual = sqerl_adhoc:insert(<<"users">>, [<<"id">>, <<"name">>], 3, dollarn),
    Expected = <<"INSERT INTO users ( id,name ) VALUES ($1,$2),($3,$4),($5,$6)">>,
    ?assertEqual(Expected, Actual).


%% values_part(s) tests
%%
values_part_test() ->
    Actual = list_to_binary(sqerl_adhoc:values_part(3, 0, dollarn)),
    Expected = <<"($1,$2,$3)">>,
    ?assertEqual(Expected, Actual).

values_part_offset_test() ->
    Actual = list_to_binary(sqerl_adhoc:values_part(3, 3, dollarn)),
    Expected = <<"($4,$5,$6)">>,
    ?assertEqual(Expected, Actual).

values_parts_test() ->
    Actual = list_to_binary(sqerl_adhoc:values_parts(3, 3, dollarn)),
    Expected = <<"($1,$2,$3),($4,$5,$6),($7,$8,$9)">>,
    ?assertEqual(Expected, Actual).

values_parts_single_row_test() ->
    Actual = list_to_binary(sqerl_adhoc:values_parts(3, 1, dollarn)),
    Expected = <<"($1,$2,$3)">>,
    ?assertEqual(Expected, Actual).

%% ensure_safe tests
%%
ensure_safe_string_test() ->
    ?assertEqual(<<"ABCdef123_ ()*">>, ensure_safe("ABCdef123_ ()*")).

ensure_safe_binary_test() ->
    ?assertEqual(<<"ABCdef123_*">>, ensure_safe(<<"ABCdef123_*">>)).

ensure_safe_atom_test() ->
    ?assertEqual(<<"ABCdef123_*">>, ensure_safe('ABCdef123_*')).

ensure_safe(Value) ->
    sqerl_adhoc:ensure_safe(Value).

ensure_safe_bad_values_test() ->
    BadValues = "`-=[]\;',./~!@#$%^&+{}|:\"<>?",
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
placeholders_dollarn_test() ->
    ?assertEqual([<<"$1">>, <<"$2">>, <<"$3">>],
                 sqerl_adhoc:placeholders(3, 0, dollarn)),
    ?assertEqual([<<"$1">>, <<"$2">>, <<"$3">>, <<"$4">>, <<"$5">>, <<"$6">>],
                 sqerl_adhoc:placeholders(6, 0, dollarn)).

placeholders_le0_test() ->
    ?assertException(error, function_clause, sqerl_adhoc:placeholders(0, 0, dollarn)),
    ?assertException(error, function_clause, sqerl_adhoc:placeholders(-2, 0, dollarn)).

placeholders_offset_test() ->
    ?assertEqual([<<"$5">>, <<"$6">>, <<"$7">>], sqerl_adhoc:placeholders(3, 4, dollarn)).

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
    ?assertEqual([], sqerl_adhoc:join([], <<",">>)),
    ?assertEqual([<<"1">>], sqerl_adhoc:join([<<"1">>], <<",">>)),
    ?assertEqual([<<"1">>, <<",">>, <<"2">>, <<",">>, <<"3">>],
                 sqerl_adhoc:join([<<"1">>, <<"2">>, <<"3">>], <<",">>)).

