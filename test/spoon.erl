-module(spoon).
-behaviour(sqerl_rec).

-export([
         '#insert_fields'/0,
         '#update_fields'/0,
         '#statements'/0
        ]).

-compile({parse_transform, exprecs}).
-export_records([spoon]).

-record(spoon, {
          id,
          name
         }).

'#insert_fields'() ->
    [name].

'#update_fields'() ->
    [name].

'#statements'() ->
    [default,
     {fetch_by_id, "SELECT 'testing' FROM spoons"},
     {['delete_by_', 'id'], "DELETE FROM spoons WHERE name = 'testing'"}
    ].
