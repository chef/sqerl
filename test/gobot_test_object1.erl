-module(gobot_test_object1).

-record(gobot_test_object1, {
    id :: integer(),
    name :: string(),
    rank :: string(),
    serial_no :: integer(),
    non_typed
    }).

-record(thing, {made_of, orange, rock}).

-compile({parse_transform, sqerl_gobot}).

-export([
    '#insert_fields'/0,
    '#update_fields'/0,
    '#statements'/0,
    '#table_name'/0
    ]).

'#insert_fields'() ->
    [name, rank, serial_no, non_typed].

'#update_fields'() ->
    [name, rank, serial_no, non_typed].

'#statements'() ->
    [default,
        {fetch_all, sqerl_rec:gen_fetch_all(gobot_test_object1, id)}
    ].

'#table_name'() ->
    orange_rock(), %% for no reason other to get around compiler warnings
    "object".

orange_rock() ->
    #thing{}.
