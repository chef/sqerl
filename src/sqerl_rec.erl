%% TODO:
%% - common handler for sqerl results
%% - intermediate form to chain queries and include in a transaction.
%% specs, docs, tests

-module(sqerl_rec).

-export([
         delete/2,
         fetch/3,
         fetch_all/1,
         fetch_page/3,
         first_page/0,
         insert/1,
         qfetch/3,
         update/1,
         statements/1,
         statements_for/1
        ]).

-ifdef(TEST).
-compile([export_all]).
-endif.

%% These are the callbacks used for generating prepared queries and
%% providing the basic access helpers.

%% db_rec is assumed to be a record. It must at least be a tuple with
%% first element containing the `db_rec''s name as an atom.will almost
%% always be a record, but doesn't have to be as long as the behavior
%% is implemented.
-type db_rec() :: tuple().

%% These callbacks are a bit odd, but align with the functions created
%% by the exprecs parse transform.
-callback '#get-'(atom(), db_rec()) ->
    any().

-callback '#new-'(atom()) ->
    db_rec().

-callback '#fromlist-'(atom(), db_rec()) ->
    db_rec().

-callback '#info-'(atom()) ->
    [atom()].

%% these are not part of the exprecs parse transform. Making these /0
%% forces implementing modules to make one module per record. If we
%% don't want that, or if we want symmetry with the exprecs generated
%% items, we'd do /1 and accept rec name as arg.
-callback '#insert_fields'() ->
    [atom()].

-callback '#update_fields'() ->
    [atom()].

-callback '#statements'() ->
    [default | {atom(), iolist()}].

%% @doc Fetch using prepared query `Query' returning a list of records
%% `[#RecName{}]'. The `Vals' list is the list of parameters for the
%% prepared query. If the prepared query does not take parameters, use
%% `[]'.
-spec qfetch(atom(), atom(), [any()]) -> [db_rec()] | {error, _}.
qfetch(RecName, Query, Vals) ->
    case sqerl:select(Query, Vals) of
        {ok, none} ->
            [];
        %% match on single row result
        {ok, Rows} ->
            rows_to_recs(Rows, RecName);
        Error ->
            ensure_error(Error)
    end.

-spec fetch(atom(), atom(), any()) -> [db_rec()] | {error, _}.
fetch(RecName, By, Val) ->
    Query = join_atoms([RecName, '_', fetch_by, '_', By]),
    qfetch(RecName, Query, [Val]).

-spec fetch_all(atom()) -> [db_rec()] | {error, _}.
fetch_all(RecName) ->
    Query = join_atoms([RecName, '_', fetch_all]),
    qfetch(RecName, Query, []).

-spec fetch_page(atom(), string(), integer()) -> [db_rec()] | {error, _}.
fetch_page(RecName, StartName, Limit) ->
    Query = join_atoms([RecName, '_', fetch_page]),
    qfetch(RecName, Query, [StartName, Limit]).

first_page() ->
    %% ascii value that sorts less or equal to any valid name.
    "\001".

-spec insert(db_rec()) -> [db_rec()] | {error, _}.
insert(Rec) ->
    RecName = rec_name(Rec),
    InsertFields = RecName:'#insert_fields'(),
    Query = join_atoms([RecName, '_', insert]),
    Values = rec_to_vlist(Rec, InsertFields),
    case sqerl:select(Query, Values) of
        {ok, 1, Rows} ->
            rows_to_recs(Rows, RecName);
        Error ->
            ensure_error(Error)
    end.

-spec update(db_rec()) -> ok | {error, _}.
update(Rec) ->
    RecName = rec_name(Rec),
    UpdateFields = RecName:'#update_fields'(),
    Query = join_atoms([RecName, '_', update]),
    Values = rec_to_vlist(Rec, UpdateFields),
    Id = RecName:'#get-'(id, Rec),
    case sqerl:select(Query, Values ++ [Id]) of
        {ok, 1} ->
            ok;
        Error ->
            ensure_error(Error)
    end.

-spec delete(db_rec(), atom()) -> ok | {error, _}.
delete(Rec, By) ->
    RecName = rec_name(Rec),
    Query = join_atoms([RecName, '_', delete_by, '_', By]),
    Id = RecName:'#get-'(By, Rec),
    case sqerl:select(Query, [Id]) of
        {ok, _} ->
            ok;
        Error ->
            ensure_error(Error)
    end.

rec_to_vlist(Rec, Fields) ->
    RecName = rec_name(Rec),
    [ RecName:'#get-'(F, Rec) || F <- Fields ].

rows_to_recs(Rows, RecName) when is_atom(RecName) ->
    rows_to_recs(Rows, RecName:'#new-'(RecName));
rows_to_recs(Rows, Rec) when is_tuple(Rec) ->
    [ row_to_rec(Row, Rec) || Row <- Rows ].

row_to_rec(Row, Rec) ->
    RecName = rec_name(Rec),
    RecName:'#fromlist-'(atomize_keys(Row), Rec).

atomize_keys(L) ->
    [ {bin_to_atom(B), V} || {B, V} <- L ].

bin_to_atom(B) ->
    erlang:binary_to_atom(B, utf8).

-spec statements([atom()]) -> [{atom(), binary()}].
statements(RecList) ->
    lists:flatten([ statements_for(RecName) || RecName <- RecList ]).

-spec statements_for(atom()) -> [{atom(), binary()}].
statements_for(RecName) ->
    RawStatements = RecName:'#statements'(),
    %% do we have default?
    Defaults = case lists:member(default, RawStatements) of
                   true ->
                       default_queries(RecName);
                   false ->
                       []
               end,
    Customs = [ Q || {_Name, _SQL} = Q <- RawStatements ],
    Prefix = join_atoms([RecName, '_']),
    [ {join_atoms([Prefix, Key]), as_bin(Query)}
      || {Key, Query} <- proplist_merge(Customs, Defaults) ].
    
proplist_merge(L1, L2) ->
    SL1 = lists:keysort(1, L1),
    SL2 = lists:keysort(1, L2),
    lists:keymerge(1, SL1, SL2).
    
default_queries(RecName) ->
    [  {fetch_by_id,   gen_fetch_by(RecName, id)}
     , {fetch_by_name, gen_fetch_by(RecName, name)}
     , {delete_by_id,  gen_delete(RecName, id)}
     , {insert,        gen_insert(RecName)}
     , {fetch_all,     gen_fetch_all(RecName, name)}
     , {fetch_page,    gen_fetch_page(RecName, name)}
     , {update,        gen_update(RecName, id)}
    ].
    
join_atoms(Atoms) ->
    Bins = [ erlang:atom_to_binary(A, utf8) || A <- Atoms ],
    erlang:binary_to_atom(iolist_to_binary(Bins), utf8).

as_bin(B) when is_binary(B) ->
    B;
as_bin(S) ->
    erlang:iolist_to_binary(S).


rec_name(Rec) ->
    erlang:element(1, Rec).

gen_params(N) ->
    Params = [ "$" ++ erlang:integer_to_list(I) || I <- lists:seq(1, N) ],
    string:join(Params, ", ").

gen_delete(RecName, By) ->
    ByStr = to_str(By),
    Table = table_name(RecName),
    ["DELETE FROM ", Table, " WHERE ", ByStr, " = $1"].

gen_update(RecName, By) ->
    UpdateFields = RecName:'#update_fields'(),
    ByStr = to_str(By),
    Table = table_name(RecName),
    UpdateCount = length(UpdateFields),
    LastParam = "$" ++ erlang:integer_to_list(1 + UpdateCount),
    AllFields = map_to_str(UpdateFields),
    IdxFields = lists:zip(map_to_str(lists:seq(1, UpdateCount)), AllFields),
    KeyVals = string:join([ Key ++ " = $" ++ I || {I, Key} <- IdxFields ], ", "),
    ["UPDATE ", Table, " SET ", KeyVals,
     " WHERE ", ByStr, " = ", LastParam].

gen_insert(RecName) ->
    InsertFields = map_to_str(RecName:'#insert_fields'()),
    InsertFieldsSQL = string:join(InsertFields, ", "),
    AllFieldsSQL = string:join(map_to_str(all_fields(RecName)), ", "),
    Params = gen_params(length(InsertFields)),
    Table = table_name(RecName),
    ["INSERT INTO ", Table, "(", InsertFieldsSQL,
     ") VALUES (", Params, ") RETURNING ", AllFieldsSQL].
    
gen_fetch_page(RecName, OrderBy) ->
    AllFields = map_to_str(all_fields(RecName)),
    FieldsSQL = string:join(AllFields, ", "),
    OrderByStr = to_str(OrderBy),
    Table = table_name(RecName),
    ["SELECT ", FieldsSQL, " FROM ", Table,
     " WHERE ", OrderByStr, " > $1 ORDER BY ", OrderByStr,
     " LIMIT $2"].
    
gen_fetch_all(RecName, OrderBy) ->
    AllFields = map_to_str(all_fields(RecName)),
    FieldsSQL = string:join(AllFields, ", "),
    OrderByStr = to_str(OrderBy),
    Table = table_name(RecName),
    ["SELECT ", FieldsSQL, " FROM ", Table,
     " ORDER BY ", OrderByStr].
    
gen_fetch_by(RecName, By) ->
    AllFields = map_to_str(all_fields(RecName)),
    FieldsSQL = string:join(AllFields, ", "),
    ByStr = to_str(By),
    Table = table_name(RecName),
    ["SELECT ", FieldsSQL, " FROM ", Table,
     " WHERE ", ByStr, " = $1"].

map_to_str(L) ->
    [ to_str(Elt) || Elt <- L ].

to_str(S) when is_list(S) ->
    S;
to_str(B) when is_binary(B) ->
    erlang:binary_to_list(B);
to_str(A) when is_atom(A) ->
    erlang:atom_to_list(A);
to_str(I) when is_integer(I) ->
    erlang:integer_to_list(I).

all_fields(RecName) ->
    RecName:'#info-'(RecName).

table_name(RecName) ->
    Exports = RecName:module_info(exports),
    case lists:member({'#table_name', 0}, Exports) of
        true ->
            RecName:'#table_name'();
        false ->
            pluralize(to_str(RecName))
    end.

%% Naive pluralization of lowercase strings. Rules are simplified from
%% a more robust library found here:
%% https://github.com/lukegalea/inflector

pluralize("alias") ->
    "aliases";
pluralize("status") ->
    "statuses";
pluralize(S) ->
    do_pluralize(lists:reverse(S)).

do_pluralize("x" ++ _ = R) ->
    lists:reverse("se" ++ R);
do_pluralize("hc" ++ _ = R) ->
    lists:reverse("se" ++ R);
do_pluralize("ss" ++ _ = R) ->
    lists:reverse("se" ++ R);
do_pluralize("hs" ++ _ = R) ->
    lists:reverse("se" ++ R);
do_pluralize("y" ++ [C|Rest]) when C == $a orelse
                                   C == $e orelse
                                   C == $i orelse
                                   C == $o orelse
                                   C == $u ->
    lists:reverse("sy" ++ [C|Rest]);
do_pluralize("y" ++ Rest) ->
    lists:reverse("sei" ++ Rest);
do_pluralize(S) ->
    lists:reverse("s" ++ S).

ensure_error({error, _} = E) ->
    E;
ensure_error(E) ->
    {error, E}.
