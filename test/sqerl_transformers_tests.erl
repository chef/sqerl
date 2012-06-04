%% -*- erlang-indent-level: 4;indent-tabs-mode: nil; fill-column: 92-*-
%% ex: ts=4 sw=4 et
%% @author Christopher Maier <cm@opscode.com>
%% @copyright 2011 Opscode, Inc.
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

convert_integer_to_boolean_test() ->
    ?assertEqual(false, sqerl_transformers:convert_integer_to_boolean(0)),
    ?assertEqual(true, sqerl_transformers:convert_integer_to_boolean(1)),
    ?assertEqual(true, sqerl_transformers:convert_integer_to_boolean(2)),

    %% no function for neg_integer()
    ?assertException(error, function_clause, sqerl_transformers:convert_integer_to_boolean(-1)),
    %% no function for binary()
    ?assertException(error, function_clause, sqerl_transformers:convert_integer_to_boolean(<<"foo">>)).

