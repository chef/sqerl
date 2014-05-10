%% -*- erlang-indent-level: 4;indent-tabs-mode: nil; fill-column: 92 -*-
%% ex: ts=4 sw=4 et
%%
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
%% @author Seth Falcon <seth@getchef.com>
%% @copyright 2014 CHEF Software, Inc. All Rights Reserved.
%% @doc Record to DB mapping module and behaviour.
%%
%% This module helps you map records to and from the DB using prepared
%% queries. By creating a module, named the same as your record, and
%% implementing the `sqerl_rec' behaviour, you can take advantage of a
%% default set of generated prepared queries and helper functions
%% (defined in this module) that leverage those queries.
%%
%% Most of the callbacks can be generated for you if you use the
%% `exprecs' parse transform. If you use this parse transform, then
%% you will only need to implement the following three callbacks in
%% your record module:
%%
%% <ul>
%% <li>``'#insert_fields'/0'' A list of atoms describing the fields (which
%% should align with column names) used to insert a row into the
%% db. In many cases this is a proper subset of the record fields to
%% account for sequence ids and db generated timestamps.</li>
%% <li>``'#update_fields'/0'' A list of atoms giving the fields used for
%% updating a row.</li>
%% <li>``'#statements'/0'' A list of `[default | {atom(),
%% iolist()}]'. If the atom `'default'' is included, then a default
%% set of queries will be generated. Custom queries provided as
%% `{Name, SQL}' tuples will override any default queries of the same
%% name.</li>
%% </ul>
%%
%% If the table name associated with your record name does not follow
%% the naive pluralization rule implemented by `sqerl_rel', you can
%% export a ``'#table_name'/0'' function to provide the table name for
%% the mapping.
%%
%% @end
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
    RealQ = join_atoms([RecName, '_', Query]),
    case sqerl:select(RealQ, Vals) of
        {ok, none} ->
            [];
        {ok, Rows} ->
            rows_to_recs(Rows, RecName);
        Error ->
            ensure_error(Error)
    end.

%% @doc Return a list of `RecName' records using single parameter
%% prepared query `RecName_fetch_by_By' where `By' is a field and
%% column name and `Val' is the value of the column to match for in a
%% WHERE clause. A (possibly empty) list of record results is returned
%% even though a common use is to fetch a single row.
-spec fetch(atom(), atom(), any()) -> [db_rec()] | {error, _}.
fetch(RecName, By, Val) ->
    Query = join_atoms([fetch_by, '_', By]),
    qfetch(RecName, Query, [Val]).

%% @doc Return all rows from the table associated with record module
%% `RecName'. Results will, by default, be ordered by the name field
%% (which is assumed to exist).
-spec fetch_all(atom()) -> [db_rec()] | {error, _}.
fetch_all(RecName) ->
    qfetch(RecName, fetch_all, []).

%% @doc Fetch rows from the table associated with record module
%% `RecName' in a paginated fashion. The default generated query, like
%% that for `fetch_all', assumes a `name' field and column and orders
%% results by this field. The `StartName' argument determines the
%% start point and `Limit' the number of items to return. To fetch the
%% "first" page, use {@link first_page/0}. Use the last name received
%% as the value for `StartName' to fetch the "next" page.
-spec fetch_page(atom(), string(), integer()) -> [db_rec()] | {error, _}.
fetch_page(RecName, StartName, Limit) ->
    qfetch(RecName, fetch_page, [StartName, Limit]).

%% @doc Return an ascii value, as a string, that sorts less or equal
%% to any valid name.
first_page() ->
    "\001".

%% @doc Insert record `Rec' using prepared query `RecName_insert'. The
%% fields of `Rec' passed as parameters to the query are determined by
%% `RecName:'#insert_fields/0'. This function assumes the query uses
%% "INSERT ... RETURNING" and returns a record with db assigned fields
%% (such as sequence ids and timestamps filled out).
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

%% @doc Update record `Rec'. Uses the prepared query with name
%% `RecName_update'. Assumes an `id' field and corresponding column
%% which is used to find the row to update. The fields from `Rec'
%% passed as parameters to the query are determined by
%% `RecName:'#update_fields/0'.
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

%% @doc Delete the rows where the column identified by `By' matches
%% the value as found in `Rec'. Typically, one would use `id' to
%% delete a single row. The prepared query with name
%% `RecName_delete_by_By' will be used.
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

%% @doc Given a list of module (and record) names implementing the
%% `sqerl_rec' behaviour, return a proplist of prepared queries in the
%% form of `[{QueryName, SQLBinary}]'. If the atom `'default'' is
%% present in the list, then a default set of queries will be
%% generated. These include: `fetch_by_id', `fetch_by_name',
%% `delete_by_id', `insert', `fetch_all', `fetch_page', and
%% `update'. The returned query names will have `Recname_'
%% prepended. Custom queries override default queries of the same
%% name.
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
