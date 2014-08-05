-module(kitchen).

-export([
         '#insert_fields'/0,
         '#update_fields'/0,
         '#statements'/0
        ]).

-compile({parse_transform, sqerl_gobot}).

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
     {fetch_by_name, sqerl_rec:gen_fetch(kitchen, name)},
     {fetch_all, sqerl_rec:gen_fetch_all(kitchen, name)},
     {fetch_page, sqerl_rec:gen_fetch_page(kitchen, name)},
     {fetch_names,
      ["SELECT name FROM ",
       "kitchens ",
       "ORDER BY name"]},
     {bad_query,
      ["SELECT name FROM not_a_table"]}
    ].
