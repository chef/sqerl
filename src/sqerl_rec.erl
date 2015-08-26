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
%% It's also worth noting that `undefined' properties of a record will
%% be saved in the DB as `NULL', and then translated back to `undefined'
%% when fetched from the DB.
%% @end
-module(sqerl_rec).

-export([
         delete/2,
         fetch/3,
         fetch_all/1,
         fetch_page/3,
         first_page/0,
         insert/1,
         cinsert/1,
         qfetch/3,
         cquery/3,
         update/1,
         scalar_fetch/3,
         statements/1,
         statements_for/1,
         gen_fetch/2,
         gen_delete/2,
         gen_fetch_page/2,
         gen_fetch_all/2
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

%% These callbacks are better, thanks to the sqerl_gobot parse transform.
-callback getval(atom(), db_rec()) ->
    any().

-callback '#new'() ->
    db_rec().

-callback fromlist([{atom(), _}]) ->
    db_rec().

-callback fields() ->
    [atom()].

%% these are not part of the exprecs parse transform. Making these /0
%% forces implementing modules to make one module per record. If we
%% don't want that, or if we want symmetry with the exprecs generated
%% items, we'd do /1 and accept rec name as arg.
-callback '#insert_fields'() ->
    [atom()].

-callback '#update_fields'() ->
    [atom()].

%% Like an iolist but only atoms
-type atom_list() :: atom() | [atom() | atom_list()].
-export_type([atom_list/0]).

-callback '#statements'() ->
    [default | {atom_list(), iolist()}].

%% @doc Fetch using prepared query `Query' returning a list of records
%% `[#RecName{}]'. The `Vals' list is the list of parameters for the
%% prepared query. If the prepared query does not take parameters, use
%% `[]'. Note that this can be used for INSERT and UPDATE queries if
%% they use an appropriate RETURNING clause.
-spec qfetch(atom(), atom_list(), [any()]) -> [db_rec()] | {error, _}.
qfetch(RecName, Query, Vals) ->
    RealQ = join_atoms([RecName, '_', Query]),
    CleanVals = [undef_to_null(V) || V <- Vals],
    case sqerl:select(RealQ, CleanVals) of
        {ok, none} ->
            [];
        {ok, N} when is_integer(N) ->
            Msg = "query returned count only; expected rows",
            {error,
             {{ok, N},
              {sqerl_rec, qfetch, Msg, [RecName, Query, Vals]}}};
        {ok, Rows} ->
            rows_to_recs(Rows, RecName);
        {ok, N, Rows} when is_integer(N) ->
            rows_to_recs(Rows, RecName);
        Error ->
            ensure_error(Error)
    end.

%% @doc Execute query `Query' that returns a row count. If the query
%% returns results, e.g. an UPDATE ... RETURNING query, the result is
%% ignored and only the count is returned. See also {@link qfetch/3}.
-spec cquery(atom(), atom_list(), [any()]) -> {ok, integer()} | {error, _}.
cquery(RecName, Query, Vals) ->
    RealQ = join_atoms([RecName, '_', Query]),
    CleanVals = [undef_to_null(V) || V <- Vals],
    case sqerl:select(RealQ, CleanVals) of
        {ok, N} when is_integer(N) ->
            {ok, N};
        {ok, N, _Rows} when is_integer(N) ->
            {ok, N};
        {ok, Rows} when is_list(Rows) ->
            Msg = "query returned rows and no count; expected count",
            {error,
             {{ok, Rows},
              {sqerl_rec, cquery, Msg, [RecName, Query, CleanVals]}}};
        Error ->
            ensure_error(Error)
    end.

%% @doc Execute a query that returns a list of scalar values. The
%% query must return a single column in result rows. This does
%% slightly less processing than using the rows_as_scalars transform
%% and prepends `RecName' to `Query' to match the sqerl_rec style.
-spec scalar_fetch(atom(), atom(), [any()]) -> [any()] | {error, _}.
scalar_fetch(RecName, Query, Params) ->
    RealQuery = join_atoms([RecName, '_', Query]),
    case sqerl:select(RealQuery, Params) of
        {ok, none} ->
            [];
        {ok, Results} ->
            try scalar_results(Results)
            catch throw:{bad_row, Bad} ->
                    Msg = "query did not return a single column",
                    {error,
                     {{bad_row, Bad},
                      {sqerl_rec, scalar_fetch, Msg,
                       [RecName, Query, Params]}}}
            end;
        {error, _} = Error ->
            Error
    end.

scalar_results(Results) ->
    lists:map(fun([{_ColName, Value}]) ->
                      Value;
                 (Bad) ->
                      throw({bad_row, Bad})
              end, Results).

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
    Values = rec_to_vlist(Rec, InsertFields),
    qfetch(RecName, insert, Values).

%% @doc Insert record `Rec' using prepared query `RecName_insert'. The
%% fields of `Rec' passed as parameters to the query are determined by
%% `RecName:'#insert_fields/0'. The result is ignored and only the
%% count is returned.
-spec cinsert(db_rec()) -> {ok, integer()} | {error, _}.
cinsert(Rec) ->
    RecName = rec_name(Rec),
    InsertFields = RecName:'#insert_fields'(),
    Values = rec_to_vlist(Rec, InsertFields),
    cquery(RecName, insert, Values).


%% @doc Update record `Rec'. Uses the prepared query with name
%% `RecName_update'. Assumes an `id' field and corresponding column
%% which is used to find the row to update. The fields from `Rec'
%% passed as parameters to the query are determined by
%% `RecName:'#update_fields/0'. This function assumes the UPDATE query
%% uses a RETURNING clause so that it can return a list of updated
%% records (similar to {@link insert/1}. This allows calling code to
%% receive db generated values such as timestamps and sequence ids
%% without making an additional round trip.
-spec update(db_rec()) -> [db_rec()] | {error, _}.
update(Rec) ->
    RecName = rec_name(Rec),
    UpdateFields = RecName:'#update_fields'(),
    Values = rec_to_vlist(Rec, UpdateFields),
    Id = RecName:getval(id, Rec),
    qfetch(RecName, update, Values ++ [Id]).

%% @doc Delete the rows where the column identified by `By' matches
%% the value as found in `Rec'. Typically, one would use `id' to
%% delete a single row. The prepared query with name
%% `RecName_delete_by_By' will be used.
-spec delete(db_rec(), atom()) -> {ok, integer()} | {error, _}.
delete(Rec, By) ->
    RecName = rec_name(Rec),
    Id = RecName:getval(By, Rec),
    cquery(RecName, ['delete_by_', By], [Id]).

rec_to_vlist(Rec, Fields) ->
    RecName = rec_name(Rec),
    [ RecName:getval(F, Rec) || F <- Fields ].

%% we translate `undefined' properties to `null' so that
%% they get saved as `NULL' in the DB, and not `"undefined"'
undef_to_null(undefined) -> null;
undef_to_null(Other) -> Other.

rows_to_recs(Rows, RecName) when is_atom(RecName) ->
    rows_to_recs(Rows, RecName:'#new'());
rows_to_recs(Rows, Rec) when is_tuple(Rec) ->
    [ row_to_rec(Row, Rec) || Row <- Rows ].

row_to_rec(Row, Rec) ->
    RecName = rec_name(Rec),
    RecName:fromlist(atomize_keys_and_null_to_undef(Row)).

atomize_keys_and_null_to_undef(L) ->
    [ {bin_to_atom(B), null_to_undef(V)} || {B, V} <- L ].

bin_to_atom(B) ->
    erlang:binary_to_atom(B, utf8).

%% same as for saving, we need to translate `null' back to `undefined'
null_to_undef(null) -> undefined;
null_to_undef(Other) -> Other.

%% @doc This function is intended to be used as the `{M, F, A}' for sqerl's
%% `prepared_statements' app config key and returns a proplist of prepared
%% queries in the form `[{QueryName, SQLBinary}]'. The `RecList' argument
%% should be a list of modules implementing the `sqerl_rec' behaviour or
%% elements of the form `{app, App}' in which case sqerl will auto-discover all
%% modules implementing the behavior. Ordering of modules and elements is
%% ignored. Any duplicate modules generated by specifying '{app, App}'
%% will also be ignored.
%%
%% Example inputs:
%%
%%   [mod1, mod2, mod3, {app, app1}]
%%   [mod1, mod2, mod3, mod4, mod5]
%%
%%
%% If the atom `default' is present in the list, then a default set of
%% queries will be generated using the first field returned by
%% ``RecName:'#info-'/1'' as a unique column for the WHERE clauses of
%% UPDATE, DELETE, and SELECT of single rows. The default queries are:
%% `fetch_by_FF', `delete_by_FF', `insert', and `update', where `FF'
%% is the name of the First Field. The returned query names will have
%% `RecName_' prepended. Custom queries override default queries of
%% the same name.
-spec statements([atom() | {app, term()}]) -> [{atom(), binary()}].
statements(RecList) ->
    RecList2 = lists:usort(lists:flatten([ expand_if_app(Term) || Term <- RecList])),
    lists:flatten([ statements_for(RecName) || RecName <- RecList2 ]).

ensure_application_spec_loaded(App) ->
    case application:load(App) of
        {error, {already_loaded, App}} -> ok;
        Other -> Other
    end.

expand_if_app({app, App}) ->
    %% Ensure that the application spec is loaded before trying to read its module list
    ok = ensure_application_spec_loaded(App),
    {ok, Mods} = application:get_key(App, modules),
    %% We use the built-in function `Mod:module_info/1` to lookup the attributes
    %% of the module, then check if the sqerl_rec behaviour is present.
    %% Currently, a module's attributes will have a separate entry for each
    %% behavior it implements `[..., {behaviour,[sqerl_rec]},
    %% {behaviour,[db_helper]}]`, instead of `[.., {behaviour,[sqerl_rec,
    %% db_helper]}`, which is why we check specifically for the
    %% `{behaviour, [sqerl_rec]}` tuple.
    [ Mod || Mod <- Mods, lists:member({behaviour, [sqerl_rec]}, Mod:module_info(attributes))];
expand_if_app(Mod) ->
    Mod.

-spec statements_for(atom()) -> [{atom(), binary()}].
statements_for(RecName) ->
    RawStatements = RecName:'#statements'(),
    %% We need to normalize the query names (keys) with join_atoms
    %% *before* merging custom and defaults. Otherwise a duplicate
    %% could sneak in since the same query can be expressed in more
    %% than one way (e.g. `foo_bar' and `[foo_, bar]').
    Prefix = [RecName, '_'],
    %% do we have default?
    Defaults = case lists:member(default, RawStatements) of
                   true ->
                       [ {join_atoms([Prefix, Key]), as_bin(SQL)}
                         || {Key, SQL} <- default_queries(RecName) ];
                   false ->
                       []
               end,
    Customs = [ {join_atoms([Prefix, Key]), as_bin(SQL)}
                || {Key, SQL} <- RawStatements ],
    proplist_merge(Customs, Defaults).

proplist_merge(L1, L2) ->
    SL1 = lists:ukeysort(1, L1),
    SL2 = lists:ukeysort(1, L2),
    lists:ukeymerge(1, SL1, SL2).

default_queries(RecName) ->
    FirstField = first_field(RecName),
    [
       {insert,                     gen_insert(RecName)}
     , {update,                     gen_update(RecName, FirstField)}
     , {['delete_by_', FirstField], gen_delete(RecName, FirstField)}
     , {['fetch_by_', FirstField],  gen_fetch(RecName, FirstField)}
    ].

join_atoms(Atoms) when is_list(Atoms) ->
    Bins = [ erlang:atom_to_binary(A, utf8) || A <- lists:flatten(Atoms) ],
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

%% @doc Return a SQL DELETE query appropriate for module `RecName'
%% implementing the `sqerl_rec' behaviour. Example:
%%
%% ```
%% SQL1 = gen_delete(user, id),
%% SQL1 = ["DELETE FROM ","cookers"," WHERE ","id = $1"]
%%
%% SQL2 = gen_delete(user, [id, name]),
%% SQL2 = ["DELETE FROM ","cookers",
%%         " WHERE ","id = $1 AND name = $2"]
%% '''
-spec gen_delete(atom(), atom() | [atom()]) -> [string()].
gen_delete(RecName, By) when is_atom(By) ->
    gen_delete(RecName, [By]);
gen_delete(RecName, ByList) when is_list(ByList) ->
    WhereItems = zip_params(ByList, " = "),
    WhereClause = string:join(WhereItems, " AND "),
    Table = table_name(RecName),
    ["DELETE FROM ", Table, " WHERE ", WhereClause].

%% @doc Generate an UPDATE query. Uses ``RecName:'#update_fields'/0''
%% to determine the fields to include for SET.
%%
%% Example:
%% ```
%% SQL1 = sqerl_rec:gen_update(cook, id),
%% SQL1 = ["UPDATE ","cookers"," SET ",
%%         "name = $1, auth_token = $2, ssh_pub_key = $3, "
%%         "first_name = $4, last_name = $5, email = $6",
%%         " WHERE ","id = $7"]
%%
%% SQL2 = sqerl_rec:gen_update(cook, [id, name]),
%% SQL2 = ["UPDATE ","cookers"," SET ",
%%         "name = $1, auth_token = $2, ssh_pub_key = $3, "
%%         "first_name = $4, last_name = $5, email = $6",
%%         " WHERE ","id = $7 AND name = $8"]
%% '''
-spec gen_update(atom(), atom() | [atom()]) -> [string()].
gen_update(RecName, By) when is_atom(By) ->
    gen_update(RecName, [By]);
gen_update(RecName, ByList) when is_list(ByList) ->
    UpdateFields = RecName:'#update_fields'(),
    Table = table_name(RecName),
    UpdateCount = length(UpdateFields),
    LastParam = 1 + UpdateCount,
    AllFields = map_to_str(UpdateFields),
    IdxFields = lists:zip(map_to_str(lists:seq(1, UpdateCount)), AllFields),
    KeyVals = string:join([ Key ++ " = $" ++ I || {I, Key} <- IdxFields ], ", "),
    AllFieldsSQL = string:join(map_to_str(RecName:fields()), ", "),
    WhereItems = zip_params(ByList, " = ", LastParam),
    WhereClause = string:join(WhereItems, " AND "),
    ["UPDATE ", Table, " SET ", KeyVals,
     " WHERE ", WhereClause,
    " RETURNING ", AllFieldsSQL].

%% @doc Generate an INSERT query for sqerl_rec behaviour
%% `RecName'. Uses ``RecName:'#insert_fields'/0'' to determine the
%% fields to insert. Generates an INSERT ... RETURNING query that
%% returns a complete record.
%%
%% Example:
%% ```
%% SQL = sqerl_rec:gen_insert(kitchen),
%% SQL = ["INSERT INTO ", "kitchens",
%%        "(", "name", ") VALUES (", "$1",
%%        ") RETURNING ", "id, name"]
%% '''
-spec gen_insert(atom()) -> [string()].
gen_insert(RecName) ->
    InsertFields = map_to_str(RecName:'#insert_fields'()),
    InsertFieldsSQL = string:join(InsertFields, ", "),
    AllFieldsSQL = string:join(map_to_str(RecName:fields()), ", "),
    Params = gen_params(length(InsertFields)),
    Table = table_name(RecName),
    ["INSERT INTO ", Table, "(", InsertFieldsSQL,
     ") VALUES (", Params, ") RETURNING ", AllFieldsSQL].

%% @doc Generate a paginated fetch query.
%%
%% Example:
%% ```
%% SQL = sqerl_rec:gen_fetch_page(kitchen, name).
%% SQL = ["SELECT ", "id, name", " FROM ", "kitchens",
%%        " WHERE ","name",
%%        " > $1 ORDER BY ","name"," LIMIT $2"]
%% '''
-spec gen_fetch_page(atom(), atom()) -> [string()].
gen_fetch_page(RecName, OrderBy) ->
    AllFields = map_to_str(RecName:fields()),
    FieldsSQL = string:join(AllFields, ", "),
    OrderByStr = to_str(OrderBy),
    Table = table_name(RecName),
    ["SELECT ", FieldsSQL, " FROM ", Table,
     " WHERE ", OrderByStr, " > $1 ORDER BY ", OrderByStr,
     " LIMIT $2"].

%% @doc Generate a query to return all rows
%%
%% Example:
%% ```
%% SQL = sqerl_rec:gen_fetch_all(kitchen, name),
%% SQL = ["SELECT ", "id, name", " FROM ", "kitchens",
%%        " ORDER BY ", "name"]
%% '''
-spec gen_fetch_all(atom(), atom()) -> [string()].
gen_fetch_all(RecName, OrderBy) ->
    AllFields = map_to_str(RecName:fields()),
    FieldsSQL = string:join(AllFields, ", "),
    OrderByStr = to_str(OrderBy),
    Table = table_name(RecName),
    ["SELECT ", FieldsSQL, " FROM ", Table,
     " ORDER BY ", OrderByStr].

%% @doc Generate a SELECT query for `RecName' rows.
%%
%% Example:
%% ```
%% SQL1 = sqerl_rec:gen_fetch(kitchen, name).
%% SQL1 = ["SELECT ", "id, name", " FROM ", "kitchens",
%%         " WHERE ", "name = $1"]
%%
%% SQL2 = sqerl_rec:gen_fetch(cook, [kitchen_id, name]),
%% SQL2 = ["SELECT ",
%%         "id, kitchen_id, name, auth_token, auth_token_bday, "
%%         "ssh_pub_key, first_name, last_name, email",
%%         " FROM ", "cookers", " WHERE ",
%%         "kitchen_id = $1 AND name = $2"]
%% '''
-spec gen_fetch(atom(), atom() | [atom()]) -> [string()].
gen_fetch(RecName, By) when is_atom(By) ->
    gen_fetch(RecName, [By]);
gen_fetch(RecName, ByList) when is_list(ByList) ->
    AllFields = map_to_str(RecName:fields()),
    FieldsSQL = string:join(AllFields, ", "),
    WhereItems = zip_params(ByList, " = "),
    WhereClause = string:join(WhereItems, " AND "),
    Table = table_name(RecName),
    ["SELECT ", FieldsSQL, " FROM ", Table,
     " WHERE ", WhereClause].

zip_params(Prefixes, Sep) ->
    zip_params(Prefixes, Sep, 1).

zip_params(Prefixes, Sep, StartIndex) ->
    Params = str_seq("$", StartIndex, length(Prefixes) + StartIndex - 1),
    [ to_str(Prefix) ++ Sep ++ Param
      || {Prefix, Param} <- lists:zip(Prefixes, Params) ].

str_seq(Prefix, Start, End) ->
    [ Prefix ++ erlang:integer_to_list(I)
      || I <- lists:seq(Start, End) ].

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

first_field(RecName) ->
    hd(RecName:fields()).

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
