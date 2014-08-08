%% Copyright 2014 CHEF Software, Inc. All Rights Reserved.
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

-module(sqerl_rec_tests).

-include_lib("eunit/include/eunit.hrl").

make_name(Prefix) ->
    V = io_lib:format("~B.~B.~B", erlang:tuple_to_list(erlang:now())),
    erlang:iolist_to_binary([Prefix, V]).

statements_test_() ->
    [
     {"[kitchen, cook]",
      fun() ->
              Statements = sqerl_rec:statements([kitchen, cook]),
              [ begin
                    ?assert(is_atom(Name)),
                    ?assert(is_binary(SQL))
                end
                || {Name, SQL} <- Statements ],
              KitchenFetchAll = <<"SELECT id, name FROM kitchens ORDER BY name">>,
              ?assertEqual(KitchenFetchAll,
                           proplists:get_value(kitchen_fetch_all, Statements)),
              ?assertEqual(<<"SELECT name FROM kitchens ORDER BY name">>,
                           proplists:get_value(kitchen_fetch_names, Statements)),

              %% no dups
              ?assertEqual(length(Statements), length(lists:usort(Statements)))
      end},

     {"[eg1] (no defaults)",
      fun() ->
              Statements = sqerl_rec:statements([eg1]),
              Expect = [{eg1_test, <<"SELECT * FROM eg1">>}],
              ?assertEqual(Expect, Statements)
      end},
     {"[spoon] overriding defaults",
      fun() ->
              Statements = sqerl_rec:statements([spoon]),

              ?assertEqual(<<"SELECT 'testing' FROM spoons">>,
                           proplists:get_value(spoon_fetch_by_id, Statements)),

              ?assertEqual(<<"DELETE FROM spoons WHERE name = 'testing'">>,
                           proplists:get_value(spoon_delete_by_id, Statements)),

              ExpectKeys = lists:sort([spoon_delete_by_id,
                                       spoon_fetch_by_id,
                                       spoon_insert,
                                       spoon_update]),
              GotKeys = lists:sort([ K || {K, _} <- Statements ]),
              ?assertEqual(ExpectKeys, GotKeys)
      end}
    ].

kitchen_test_() ->
    {setup,
     fun() ->
             sqerl_test_helper:setup_db()
             %% , application:set_env(sqerl, log_statements, true)
             , error_logger:tty(false)
     end,
     fun(_) ->
             pooler:rm_pool(sqerl),
             Apps = [pooler, epgsql, sqerl, epgsql],
             [ application:stop(A) || A <- Apps ]
     end,
     [
      ?_assertEqual([], sqerl_rec:fetch_all(kitchen)),
      ?_assertEqual([], sqerl_rec:fetch(kitchen, name, <<"none">>)),
      ?_assertEqual([], sqerl_rec:fetch_page(kitchen, <<"a">>, 1000)),
      {"insert",
       fun() ->
               {K0, Name} = make_kitchen(<<"pingpong">>),
               [K1] = sqerl_rec:insert(K0),
               validate_kitchen(Name, K1)
       end},
      {"insert, fetch, update, fetch",
       fun() ->
               {K0, Name0} = make_kitchen(<<"pingpong">>),
               [K1] = sqerl_rec:insert(K0),
               [FK1] = sqerl_rec:fetch(kitchen, name, Name0),
               %% can fetch inserted
               ?assertEqual(K1, FK1),

               K2 = kitchen:setvals([{name, <<"tennis">>}], K1),
               ?assertEqual([K2], sqerl_rec:update(K2)),
               ?assertEqual(K2,
                            hd(sqerl_rec:fetch(kitchen, name, <<"tennis">>)))
       end},
      {"fetch_all, delete",
       fun() ->
               %% TODO: if you provide a bad atom here, you get a
               %% confusing crash. Try: 'kitchens'
               Kitchens = sqerl_rec:fetch_all(kitchen),
               ?assertEqual(2, length(Kitchens)),
               Res = [ sqerl_rec:delete(K, id) || K <- Kitchens ],
               ?assertEqual([{ok, 1}, {ok, 1}], Res),
               ?assertEqual([], sqerl_rec:fetch_all(kitchen))
       end},
      {"fetch_all, fetch_page",
       fun() ->
               %% setup
               Kitchens = [ begin
                                B = int_to_0bin(I),
                                {K, _Name} = make_kitchen(<<"A-", B/binary, "-">>),
                                K
                            end
                            || I <- lists:seq(1, 20) ],
               [ sqerl_rec:insert(K) || K <- Kitchens ],

               All = sqerl_rec:fetch_all(kitchen),
               ExpectNames = [ kitchen:getval(name, K) || K <- Kitchens ],
               FoundNames = [ kitchen:getval(name, K) || K <- All ],
               ?assertEqual(ExpectNames, FoundNames),

               K_1_10 = sqerl_rec:fetch_page(kitchen, sqerl_rec:first_page(), 10),
               Next = kitchen:getval(name, lists:last(K_1_10)),
               K_11_20 = sqerl_rec:fetch_page(kitchen, Next , 10),
               PageNames = [ kitchen:getval(name, K) || K <- (K_1_10 ++ K_11_20) ],
               ?assertEqual(ExpectNames, PageNames)
       end},
      {"bad query returns error",
       fun() ->
               Error = sqerl:select(kitchen_bad_query, []),
               Msg = <<"relation \"not_a_table\" does not exist">>,
               ?assertMatch({error, {syntax, {Msg, _}}}, Error)
       end},
      {"scalar_fetch",
       fun() ->
               Names = sqerl_rec:scalar_fetch(kitchen, fetch_names, []),
               Len = length(Names),
               ?assert(Len > 1),
               Matched = [ B || <<"A-00", _/binary>> = B <- Names ],
               ?assertEqual(Len, length(Matched))
       end},
      {"scalar_fetch bad query",
       fun() ->
               ?assertMatch({error,
                             {{bad_row, _},
                             {sqerl_rec, scalar_fetch,
                              "query did not return a single column",
                              [kitchen, fetch_all, []]}}},
                            sqerl_rec:scalar_fetch(kitchen, fetch_all, []))
       end}
     ]}.

kitchen_app_test_() ->
    {setup,
     fun() ->
             sqerl_test_helper:setup_db_app()
             %% , application:set_env(sqerl, log_statements, true)
             , error_logger:tty(false)
     end,
     fun(_) ->
             pooler:rm_pool(sqerl),
             Apps = [pooler, epgsql, sqerl, epgsql],
             [ application:stop(A) || A <- Apps ]
     end,
     [
      ?_assertEqual([], sqerl_rec:fetch_all(kitchen)),
      ?_assertEqual([], sqerl_rec:fetch(kitchen, name, <<"none">>)),
      ?_assertEqual([], sqerl_rec:fetch_page(kitchen, <<"a">>, 1000)),
      {"insert",
       fun() ->
               {K0, Name} = make_kitchen(<<"pingpong">>),
               [K1] = sqerl_rec:insert(K0),
               validate_kitchen(Name, K1)
       end},
      {"insert, fetch, update, fetch",
       fun() ->
               {K0, Name0} = make_kitchen(<<"pingpong">>),
               [K1] = sqerl_rec:insert(K0),
               [FK1] = sqerl_rec:fetch(kitchen, name, Name0),
               %% can fetch inserted
               ?assertEqual(K1, FK1),

               K2 = kitchen:setvals([{name, <<"tennis">>}], K1),
               ?assertEqual([K2], sqerl_rec:update(K2)),
               ?assertEqual(K2,
                            hd(sqerl_rec:fetch(kitchen, name, <<"tennis">>)))
       end},
      {"fetch_all, delete",
       fun() ->
               %% TODO: if you provide a bad atom here, you get a
               %% confusing crash. Try: 'kitchens'
               Kitchens = sqerl_rec:fetch_all(kitchen),
               ?assertEqual(2, length(Kitchens)),
               Res = [ sqerl_rec:delete(K, id) || K <- Kitchens ],
               ?assertEqual([{ok, 1}, {ok, 1}], Res),
               ?assertEqual([], sqerl_rec:fetch_all(kitchen))
       end},

      {"fetch_all, fetch_page",
       fun() ->
               %% setup
               Kitchens = [ begin
                                B = int_to_0bin(I),
                                {K, _Name} = make_kitchen(<<"A-", B/binary, "-">>),
                                K
                            end
                            || I <- lists:seq(1, 20) ],
               [ sqerl_rec:insert(K) || K <- Kitchens ],

               All = sqerl_rec:fetch_all(kitchen),
               ExpectNames = [ kitchen:getval(name, K) || K <- Kitchens ],
               FoundNames = [ kitchen:getval(name, K) || K <- All ],
               ?assertEqual(ExpectNames, FoundNames),

               K_1_10 = sqerl_rec:fetch_page(kitchen, sqerl_rec:first_page(), 10),
               Next = kitchen:getval(name, lists:last(K_1_10)),
               K_11_20 = sqerl_rec:fetch_page(kitchen, Next , 10),
               PageNames = [ kitchen:getval(name, K) || K <- (K_1_10 ++ K_11_20) ],
               ?assertEqual(ExpectNames, PageNames)
       end},
      {"bad query returns error",
       fun() ->
               Error = sqerl:select(kitchen_bad_query, []),
               Msg = <<"relation \"not_a_table\" does not exist">>,
               ?assertMatch({error, {syntax, {Msg, _}}}, Error)
       end},
      {"scalar_fetch",
       fun() ->
               Names = sqerl_rec:scalar_fetch(kitchen, fetch_names, []),
               Len = length(Names),
               ?assert(Len > 1),
               Matched = [ B || <<"A-00", _/binary>> = B <- Names ],
               ?assertEqual(Len, length(Matched))
       end},
      {"scalar_fetch bad query",
       fun() ->
               ?assertMatch({error,
                             {{bad_row, _},
                              {sqerl_rec, scalar_fetch,
                               "query did not return a single column",
                               [kitchen, fetch_all, []]}
                              }},
                            sqerl_rec:scalar_fetch(kitchen, fetch_all, []))
       end}
     ]}.

cook_test_() ->
    {setup,
     fun() ->
             sqerl_test_helper:setup_db()
             %% , application:set_env(sqerl, log_statements, true)
             , error_logger:tty(false)
     end,
     fun(_) ->
             pooler:rm_pool(sqerl),
             Apps = [pooler, epgsql, sqerl, epgsql],
             [ application:stop(A) || A <- Apps ]
     end,
     [
      {"insert, qfetch by multiple cooks",
       fun() ->
               {K0, _KName0} = make_kitchen(<<"basket">>),
               [K1] = sqerl_rec:insert(K0),
               KitchenId = kitchen:getval(id, K1),
               {C0, CName0} = make_cook(<<"grace">>, KitchenId,
                                        <<"grace">>, <<"hopper">>),
               [C1] = sqerl_rec:insert(C0),
               [C2] = sqerl_rec:qfetch(cook, fetch_by_name_kitchen_id,
                                       [CName0, KitchenId]),
               ?assertEqual(C1, C2)
       end},
      {"qfetch no result on insert",
       fun() ->
               ?assertMatch({error,
                             {{ok, 0},
                              {sqerl_rec, qfetch,
                               "query returned count only; expected rows",
                               [cook, insert2,
                                [<<"no such kitchen">>, <<"sam">>]]}}},
                            sqerl_rec:qfetch(cook, insert2,
                                             [<<"no such kitchen">>,
                                              <<"sam">>]))
       end},
      {"qfetch insert undefineds sanitized",
       fun() ->
               {K0, _KName} = make_kitchen(<<"no such kitchen">>),
               [K1] = sqerl_rec:insert(K0),
               KitchenId = kitchen:getval(id, K1),
               [C] = sqerl_rec:qfetch(cook, insert,
                                      [KitchenId,
                                       make_name(<<"asm">>),
                                       <<"asdsasd">>,
                                       <<"NONE">>,
                                       <<"">>,
                                       undefined,
                                       <<"">>
                                      ]),
               %% make sure we can insert via raw qfetch
               ?assertMatch(cook, element(1, C)),

               %% make sure that undefineds aren't coming back as
               %% literal binaries
               Name = cook:getval(name, C),
               [FetchedCook] = sqerl_rec:qfetch(cook, fetch_by_name_kitchen_id,
                                                [Name, KitchenId]),
               ?assertEqual(undefined, cook:getval(last_name, FetchedCook))
       end},
       {"undefined record properties get translated to NULL, and back",
        fun() ->
                {K0, _KName} = make_kitchen(<<"null-test">>),
                [K1] = sqerl_rec:insert(K0),
                KitchenId = kitchen:getval(id, K1),
                {Cook, _CName} = make_cook(<<"no-last-name">>, KitchenId),
                [SavedCook] = sqerl_rec:insert(Cook),
                [FetchedCook] = sqerl_rec:qfetch(cook, fetch_null_last_names,
                                                 [KitchenId]),
                ?assertEqual(SavedCook, FetchedCook),
                ?assertEqual(undefined, cook:getval(last_name, FetchedCook))
       end}
     ]}.

cook_app_test_() ->
    {setup,
     fun() ->
             sqerl_test_helper:setup_db_app()
             %% , application:set_env(sqerl, log_statements, true)
             , error_logger:tty(false)
     end,
     fun(_) ->
             pooler:rm_pool(sqerl),
             Apps = [pooler, epgsql, sqerl, epgsql],
             [ application:stop(A) || A <- Apps ]
     end,
     [
      {"insert, qfetch by multiple cooks",
       fun() ->
               {K0, _KName0} = make_kitchen(<<"basket">>),
               [K1] = sqerl_rec:insert(K0),
               KitchenId = kitchen:getval(id, K1),
               {C0, CName0} = make_cook(<<"grace">>, KitchenId,
                                        <<"grace">>, <<"hopper">>),
               [C1] = sqerl_rec:insert(C0),
               [C2] = sqerl_rec:qfetch(cook, fetch_by_name_kitchen_id,
                                       [CName0, KitchenId]),
               ?assertEqual(C1, C2)
       end}
     ]}.

int_to_0bin(I) ->
    erlang:iolist_to_binary(io_lib:format("~5.10.0B", [I])).

make_kitchen(Prefix) ->
    Name = make_name(Prefix),
    K = kitchen:fromlist([{name, Name}]),
    {K, Name}.

make_cook(Prefix, KitchenId) ->
    Name = make_name(Prefix),
    SSH = <<"NONE">>,
    AuthToken = base64:encode(crypto:rand_bytes(10)),
    C = cook:fromlist([ {name, Name},
                        {kitchen_id, KitchenId},
                        {auth_token, AuthToken},
                        {ssh_pub_key, SSH}
                      ]),
    {C, Name}.

make_cook(Prefix, KitchenId, First, Last) ->
    {Cook, Name} = make_cook(Prefix, KitchenId),
    Email = iolist_to_binary([First, "@", Last, ".com"]),
    NewCook = cook:setvals([{email, Email},
                            {first_name, First},
                            {last_name, Last}], Cook),
    {NewCook, Name}.

validate_kitchen(Name, K) ->
    ?assert(kitchen:'#is'(K)),
    ?assertEqual(Name, kitchen:getval(name, K)),
    ?assert(erlang:is_integer(kitchen:getval(id, K))).

gen_fetch_test_() ->
    Tests = [
             {{kitchen, name},
              ["SELECT ",
               "id, name",
               " FROM ", "kitchens",
               " WHERE ", "name = $1"]},

             {{kitchen, id},
              ["SELECT ",
               "id, name",
               " FROM ", "kitchens",
               " WHERE ", "id = $1"]}
            ],
    [ ?_assertEqual(E, sqerl_rec:gen_fetch(Rec, By))
      || {{Rec, By}, E} <- Tests ].

gen_fetch_multiple_test_() ->
    Tests = [
             {{cook, [name, kitchen_id]},
              ["SELECT ",
               "id, kitchen_id, name, auth_token, auth_token_bday, "
               "ssh_pub_key, first_name, last_name, email",
               " FROM ", "cookers",
               " WHERE ", "name = $1 AND kitchen_id = $2"]}
             ],
    [ ?_assertEqual(E, sqerl_rec:gen_fetch(Rec, By))
      || {{Rec, By}, E} <- Tests ].

gen_delete_test() ->
    Expect = ["DELETE FROM ", "kitchens",
              " WHERE ", "id", " = $1"],
    ?assertEqual(Expect, sqerl_rec:gen_delete(kitchen, id)).

gen_params_test_() ->
    Tests = [{1, "$1"},
             {2, "$1, $2"},
             {3, "$1, $2, $3"}],
    [ ?_assertEqual(E, sqerl_rec:gen_params(N))
      || {N, E} <- Tests ].

gen_update_test() ->
    Expect = ["UPDATE ", "cookers",
              " SET ",
              "name = $1, auth_token = $2, ssh_pub_key = $3, "
              "first_name = $4, last_name = $5, email = $6",
              " WHERE ", "id", " = ", "$7",
              " RETURNING ",
              "id, kitchen_id, name, auth_token, auth_token_bday, "
              "ssh_pub_key, "
              "first_name, last_name, email"],
    ?assertEqual(Expect, sqerl_rec:gen_update(cook, id)).

gen_insert_test() ->
    Expect = ["INSERT INTO ", "cookers", "(",
              "kitchen_id, name, auth_token, ssh_pub_key, "
              "first_name, last_name, email",
              ") VALUES (",
              "$1, $2, $3, $4, $5, $6, $7", ") RETURNING ",
              "id, kitchen_id, name, auth_token, auth_token_bday, "
              "ssh_pub_key, "
              "first_name, last_name, email"],
    ?assertEqual(Expect, sqerl_rec:gen_insert(cook)).

gen_fetch_all_test() ->
    Expect = ["SELECT ",
              "id, kitchen_id, name, auth_token, "
              "auth_token_bday, ssh_pub_key, first_name, "
              "last_name, email",
              " FROM ", "cookers",
              " ORDER BY ", "name"],
    ?assertEqual(Expect, sqerl_rec:gen_fetch_all(cook, name)).

gen_fetch_page_test() ->
    Expect = ["SELECT ",
              "id, kitchen_id, name, auth_token, "
              "auth_token_bday, ssh_pub_key, first_name, "
              "last_name, email",
              " FROM ", "cookers",
              " WHERE ", "name", " > $1 ORDER BY ",
              "name", " LIMIT $2"],
    ?assertEqual(Expect, sqerl_rec:gen_fetch_page(cook, name)).

pluralize_test_() ->
    [ ?_assertEqual(Expect, sqerl_rec:pluralize(In))
      || {In, Expect} <- [{"cook", "cooks"},
                          {"box", "boxes"},
                          {"batch", "batches"},
                          {"bash", "bashes"},
                          {"mess", "messes"},
                          {"entry", "entries"},
                          {"toy", "toys"},
                          {"bay", "bays"},
                          {"queue", "queues"},
                          {"node", "nodes"},
                          {"alias", "aliases"},
                          {"status", "statuses"}]
    ].
