%% -*- erlang-indent-level: 4;indent-tabs-mode: nil; fill-column: 92-*-
%% ex: ts=4 sw=4 et
%% @author Christopher Maier <maier@lambda.local>
%% @copyright 2011 Opscode, Inc.
-module(sqerl_transformers_tests).

-record(user, {'id',
               'authz_id',
               'username',
               'pubkey_version',
               'public_key'}).

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
    RowsTransformer = sqerl_transformers:rows(),

    ?assertEqual({ok, none}, RowsTransformer([])),
    ?assertEqual({ok, [foo, bar, baz]}, RowsTransformer([foo, bar, baz])).

count_test() ->
    CountTransformer = sqerl_transformers:count(),
    ?assertEqual({ok, 666}, CountTransformer(666)).
