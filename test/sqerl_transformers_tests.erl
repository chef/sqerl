%% -*- erlang-indent-level: 4;indent-tabs-mode: nil; fill-column: 92-*-
%% ex: ts=4 sw=4 et
%% @author Christopher Maier <cm@opscode.com>
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

-module(sqerl_transformers_tests).

-record(user, {id,
               authz_id,
               username,
               pubkey_version,
               public_key}).

-include_lib("eunit/include/eunit.hrl").

rows_as_records_test() ->
    Rows = [[{<<"id">>, 123},
             {<<"authz_id">>, <<"authz_id">>},
             {<<"username">>, <<"clownco">>},
             {<<"pubkey_version">>, <<"XXX">>},
             {<<"public_key">>, <<"abcdef0123456789">>}],
            [{<<"id">>, 1234},
             {<<"authz_id">>, <<"authz_id2">>},
             {<<"username">>, <<"skynet">>},
             {<<"pubkey_version">>, <<"XXX">>},
             {<<"public_key">>, <<"9876543210fedcab">>}]],

    UserTransformer = sqerl_transformers:rows_as_records(user, record_info(fields, user)),
    ?assertEqual({ok, none}, UserTransformer(none)),
    ?assertEqual({ok, none}, UserTransformer([])),
    ?assertEqual({ok, [#user{id=123,
                             authz_id= <<"authz_id">>,
                             username= <<"clownco">>,
                             pubkey_version= <<"XXX">>,
                             public_key= <<"abcdef0123456789">>},
                       #user{id=1234,
                             authz_id= <<"authz_id2">>,
                             username= <<"skynet">>,
                             pubkey_version= <<"XXX">>,
                             public_key= <<"9876543210fedcab">>}]},
                 UserTransformer(Rows)).

first_test() ->
    First = sqerl_transformers:first(),

    Empty = [],
    ?assertEqual({ok, none}, First(Empty)),

    HasStuff = [foo, bar, baz],

    ?assertEqual({ok, foo}, First(HasStuff)).

first_as_scalar_test() ->
    FirstScore = sqerl_transformers:first_as_scalar(score),
    ?assertMatch({ok, none}, FirstScore([])),

    ?assertMatch({ok, 0}, FirstScore([[{<<"score">>, 0}],
                                       [{<<"score">>, 1}]])),

    ?assertMatch({ok, 42}, FirstScore([[{<<"score">>, 42}],
                                       [{<<"score">>, 100}]]))
.

first_as_record_test() ->
    ChefUserFirstAsRecord = sqerl_transformers:first_as_record(user, record_info(fields, user)),
    Empty = [],
    ?assertEqual({ok, none}, ChefUserFirstAsRecord(Empty)),

    Rows = [[{<<"id">>, 123},
             {<<"authz_id">>, <<"authz_id">>},
             {<<"username">>, <<"clownco">>},
             {<<"pubkey_version">>, <<"XXX">>},
             {<<"public_key">>, <<"abcdef0123456789">>}],
            [{<<"id">>, 1234},{<<"authz_id">>, <<"authz_id2">>},
             {<<"username">>, <<"skynet">>},
             {<<"pubkey_version">>, <<"XXX">>},
             {<<"public_key">>, <<"9876543210fedcab">>}]],

    ?assertEqual({ok, #user{id=123,
                            authz_id= <<"authz_id">>,
                            username= <<"clownco">>,
                            pubkey_version= <<"XXX">>,
                            public_key= <<"abcdef0123456789">>}},
                 ChefUserFirstAsRecord(Rows)).
rows_test() ->
    Rows = [[{<<"id">>, 123},
             {<<"authz_id">>, <<"authz_id">>},
             {<<"username">>, <<"clownco">>},
             {<<"pubkey_version">>, <<"XXX">>},
             {<<"public_key">>, <<"abcdef0123456789">>}],
            [{<<"id">>, 1234},
             {<<"authz_id">>, <<"authz_id2">>},
             {<<"username">>, <<"skynet">>},
             {<<"pubkey_version">>, <<"XXX">>},
             {<<"public_key">>, <<"9876543210fedcab">>}]],

    RowsTransformer = sqerl_transformers:rows(),

    ?assertEqual({ok, none}, RowsTransformer([])),
    Results = [[123, <<"authz_id">>, <<"clownco">>, <<"XXX">>, <<"abcdef0123456789">>],
               [1234, <<"authz_id2">>, <<"skynet">>, <<"XXX">>, <<"9876543210fedcab">>]],
    ?assertEqual({ok, Results}, RowsTransformer(Rows)).

count_test() ->
    CountTransformer = sqerl_transformers:count(),
    ?assertEqual({ok, 666}, CountTransformer(666)),
    ?assertEqual({ok, 0}, CountTransformer(0)).

rows_as_scalars_test() ->
    ScalarTransformer = sqerl_transformers:rows_as_scalars(username),

    Rows = [[{<<"id">>, 123},
             {<<"authz_id">>, <<"authz_id">>},
             {<<"username">>, <<"clownco">>},
             {<<"pubkey_version">>, <<"XXX">>},
             {<<"public_key">>, <<"abcdef0123456789">>}],
            [{<<"id">>, 1234},{<<"authz_id">>, <<"authz_id2">>},
             {<<"username">>, <<"skynet">>},
             {<<"pubkey_version">>, <<"XXX">>},
             {<<"public_key">>, <<"9876543210fedcab">>}]],


    ?assertEqual({ok, none}, ScalarTransformer([])),
    ?assertEqual({ok, [<<"clownco">>,<<"skynet">>]},
                 ScalarTransformer(Rows)).

single_column_with_no_transformers_test() ->
    Transforms = [{}],
    ValFoo = {<<"foo">>, <<"x">>},
    ?assertEqual(ValFoo, sqerl_transformers:single_column(ValFoo, Transforms)).

%% @doc check that single_column works when the transform is a {Mod, Fun} tuple
single_column_with_tuple_test() ->
    Transforms = [{<<"foo">>, {sqerl_transformers, convert_integer_to_boolean}}],
    ?assertEqual({<<"foo">>, true}, sqerl_transformers:single_column({<<"foo">>, 1},
                                                                    Transforms)),
    ?assertEqual({<<"foo">>, false}, sqerl_transformers:single_column({<<"foo">>, 0},
                                                                      Transforms)),
    ?assertEqual({<<"foo">>, false}, sqerl_transformers:single_column({<<"foo">>, -1},
                                                                      Transforms)).


%% @doc check that single_column works when the transform is a fun
single_column_with_fun_test() ->
    Transforms = [{<<"foo">>, fun sqerl_transformers:convert_integer_to_boolean/1} ],
    ValFoo = {<<"foo">>, 1},
    ?assertEqual({<<"foo">>, true}, sqerl_transformers:single_column(ValFoo, Transforms)),
    ?assertEqual({<<"foo">>, false}, sqerl_transformers:single_column({<<"foo">>, 0},
                                                                     Transforms)).

%% @doc we get the underlying function_clause error back badmatch if we pass an invalid
%% value into a transformer (in this case a binary() into something expecting a number)
single_column_badarg_test() ->
    Transforms = [{<<"foo">>, fun sqerl_transformers:convert_integer_to_boolean/1} ],
    ?assertError(function_clause, sqerl_transformers:single_column({<<"foo">>, <<"wontwork">>},
                                                                   Transforms)).

