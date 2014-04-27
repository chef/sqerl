-module(kitchen).
-behaviour(sqerl_rec).

-export([
         '#insert_fields'/0,
         '#update_fields'/0,
         '#statements'/0
        ]).

-compile({parse_transform, exprecs}).
-export_records([kitchen]).

-record(kitchen, {
          id,
          name
         }).

'#insert_fields'() ->
    [name].

'#update_fields'() ->
    [name].

'#statements'() ->
    [default,
     {test_query,
      ["SELECT name FROM ",
       "kitchens ",
       "ORDER BY name"]}].
