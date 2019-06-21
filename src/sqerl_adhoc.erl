%% -*- erlang-indent-level: 4;indent-tabs-mode: nil; fill-column: 92-*-
%% ex: ts=4 sw=4 et
%% @author Jean-Philippe Langlois <jpl@opscode.com>
%% @doc SQL generation library.
%% Copyright 2011-2012 Opscode, Inc. All Rights Reserved.
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

-module(sqerl_adhoc).

-export([select/4,
         delete/3,
         insert/4,
         update/4]).

-include_lib("sqerl.hrl").

-ifdef(TEST).
-compile([export_all]).
-endif.

-include_lib("eunit/include/eunit.hrl").

-type sql_clause() :: atom() | tuple().
-type sql_word() :: sqerl_sql().

%% @doc Generates SELECT SQL.
%% Returns {SQL, ParameterValues} which can be passed on to be executed.
%% SQL generated uses parameter place holders so query can be
%% prepared.
%%
%% Note: Validates that parameters are safe.
%%
%% ```
%% Clauses = clause list
%% Clause = Where|Order By|Limit
%%
%%
%% Where Clause
%% -------------
%% Form: {where, Where}
%%
%% Where = all|undefined
%% Does not generate a WHERE clause. Matches all records in table.
%%
%% 1> select([<<"*">>], <<"users">>, [], qmark).
%% {<<"SELECT * FROM users">>, []}
%%
%% Where = {Field, equals|nequals|gt|gte|lt|lte, Value}
%% Generates SELECT ... WHERE Field =|!=|>|>=|<|<= ?
%%
%% 1> select([<<"name">>], <<"users">>, [{where, {<<"id">>, equals, 1}}], qmark).
%% {<<"SELECT name FROM users WHERE id = ?">>, [1]}
%%
%% Where = {Field, in|notin, Values}
%% Generates SELECT ... IN|NOT IN SQL with parameter strings (not values),
%% which can be prepared and executed.
%%
%% 1> select([<<"name">>], <<"users">>, [{where, {<<"id">>, in, [1,2,3]}}], qmark).
%% {<<"SELECT name FROM users WHERE id IN (?, ?, ?)">>, [1,2,3]}
%%
%% Where = {'and'|'or', WhereList}
%% Composes WhereList with AND or OR
%%
%% 1> select([<<"id">>], <<"t">>, [{where, {'or', [{<<"id">>, lt, 5}, {<<"id">>, gt, 10}]}}]).
%% {<<"SELECT id FROM t WHERE (id < ? OR id > ?)">>, [5,10]}
%%
%% Group By Clause
%% ---------------
%% Form: {group_by, Fields}
%%
%% Order By Clause
%% ---------------
%% Form: {order_by, Fields | {Fields, asc|desc}}
%%
%% Limit/Offset Clause
%% --------------------
%% Form: {limit, Limit} | {limit, {Limit, offset, Offset}}
%%
%% ParamStyle is qmark (?, ?, ... for e.g. mysql)
%% or dollarn ($1, $2, etc. for e.g. pgsql)
%% '''
-spec select([sql_word()], sql_word(), [] | [sql_clause()], atom()) -> {binary(), list()}.
select(Columns, Table, Clauses, ParamStyle) ->
    [SafeColumns, SafeTable] = ensure_safe([Columns, Table]),
    {WhereSQL, Values} = where_sql(proplists:get_value(where, Clauses), ParamStyle),
    GroupBySQL = group_by_sql(proplists:get_value(group_by, Clauses)),
    OrderBySQL = order_by_sql(proplists:get_value(order_by, Clauses)),
    LimitSQL = limit_sql(proplists:get_value(limit, Clauses)),
    Parts = [<<"SELECT ">>, column_parts(SafeColumns),
             <<" FROM ">>, SafeTable,
             WhereSQL,
             GroupBySQL,
             OrderBySQL,
             LimitSQL],
    SQL = list_to_binary(Parts),
    {SQL, Values}.

%% Returns {SQL, Values}
%% SQL is clause of SQL query, e.g. <<"WHERE F = ?">>.
%% Returns empty binary if no where clause is needed.
where_sql(undefined, _ParamStyle) ->
    {<<"">>, []};
where_sql(all, _ParamStyle) ->
    {<<"">>, []};
where_sql(Where, ParamStyle) ->
    {Parts, Values} = where_parts(Where, ParamStyle),
    SQL = list_to_binary(Parts),
    {<<" WHERE ", SQL/binary>>, Values}.

%% Returns "GROUP BY ..." SQL
group_by_sql(undefined) ->
    <<"">>;
group_by_sql(Fields) ->
    SafeFields = ensure_safe(Fields),
    FieldsSQL = list_to_binary(join(SafeFields, <<", ">>)),
    <<" GROUP BY ", FieldsSQL/binary>>.

%% Returns "ORDER BY ..." SQL
order_by_sql(undefined) ->
    <<"">>;
order_by_sql({Fields, Direction}) ->
    SafeFields = ensure_safe(Fields),
    FieldsSQL = list_to_binary(join(SafeFields, <<", ">>)),
    DirectionSQL = case Direction of
                       asc -> <<" ASC">>;
                       desc -> <<" DESC">>
                   end,
    <<" ORDER BY ", FieldsSQL/binary, DirectionSQL/binary>>;
order_by_sql(Fields) ->
    order_by_sql({Fields, asc}).

%% Returns LIMIT ... SQL
limit_sql(undefined) ->
    <<"">>;
limit_sql(Limit) when is_integer(Limit) ->
    LimitBin = list_to_binary(integer_to_list(Limit)),
    <<" LIMIT ", LimitBin/binary>>;
limit_sql({Limit, offset, Offset}) when is_integer(Limit), is_integer(Offset) ->
    LimitBin = list_to_binary(integer_to_list(Limit)),
    OffsetBin = list_to_binary(integer_to_list(Offset)),
    <<" LIMIT ", LimitBin/binary, " OFFSET ", OffsetBin/binary>>.

%% @doc Generate UPDATE statement.
%%
%% Update is given under the form of a Row (proplist).
%% Uses the same Where specifications as select/4 except for "all" which is
%% not supported.
%%
%% ```
%% 1> update(<<"users">>, [{<<"last_name">>, <<"Toto">>}], {<<"id">>, equals, 1}, qmark).
%% {<<"UPDATE users SET last_name = ? WHERE id = ?">>, [<<"Toto">>, 1]}
%% '''
%%%
-spec update(sql_word(), sqerl_row(), sql_clause(), atom()) -> {binary(), list()}.
update(Table, Row, Where, ParamStyle) ->
    SafeTable = ensure_safe(Table),
    {SetSQL, SetValues} = set_parts(Row, ParamStyle),
    {WhereSQL, WhereValues} = where_parts(Where, ParamStyle, length(SetValues)),
    Parts = [<<"UPDATE">>,
             SafeTable,
             <<"SET">>,
             SetSQL,
             <<"WHERE">>,
             WhereSQL],
    SQL = list_to_binary(join(Parts, <<" ">>)),
    {SQL, SetValues ++ WhereValues}.

%% @doc Generate set parts of update query.
set_parts(Row, ParamStyle) when length(Row) > 0 ->
    set_parts(Row, ParamStyle, 0).

set_parts(Row, ParamStyle, ParamPosOffset) ->
    set_parts(Row, ParamStyle, ParamPosOffset, [], []).

set_parts([], _ParamStyle, _ParamPosOffset, PartsAcc, ValuesAcc) ->
    {join(lists:reverse(PartsAcc), <<", ">>), lists:reverse(ValuesAcc)};

set_parts([{Field, Value}|T], ParamStyle, ParamPosOffset, PartsAcc, ValuesAcc) ->
    SafeField = ensure_safe(Field),
    Parts = [SafeField, <<" = ">>, placeholder(1 + ParamPosOffset, ParamStyle)],
    set_parts(T, ParamStyle, ParamPosOffset + 1, [Parts|PartsAcc], [Value|ValuesAcc]).


%% @doc Generate DELETE statement.
%%
%% Uses the same Where specifications as select/4.
%% See select/4 for details about the "Where" parameter.
%%
%% ```
%% 1> delete(<<"users">>, {<<"id">>, equals, 1}, qmark).
%% {<<"DELETE FROM users WHERE id = ?">>, [1]}
%% '''
%%
-spec delete(sql_word(), sql_clause(), atom()) -> {binary(), [any()]}.
delete(Table, Where, ParamStyle) ->
    SafeTable = ensure_safe(Table),
    {WhereSQL, Values} = where_sql(Where, ParamStyle),
    Parts = [<<"DELETE FROM ">>,
             SafeTable,
             WhereSQL],
    SQL = list_to_binary(Parts),
    {SQL, Values}.


%% @doc Generate INSERT statement for N rows.
%%
%% ```
%% 1> insert(<<"users">>, [<<"id">>, <<"name">>], 3, qmark).
%% <<"INSERT INTO users (name) VALUES (?,?),(?,?),(?,?)">>
%% '''
%%
-spec insert(sql_word(), [sql_word()], integer(), atom()) -> binary().
insert(Table, Columns, NumRows, ParamStyle) when
    NumRows > 0,
    length(Columns) > 0 ->
    [SafeTable, SafeColumns] = ensure_safe([Table, Columns]),
    Parts = [<<"INSERT INTO">>,
             SafeTable,
             <<"(">>,
             column_parts(SafeColumns),
             <<")">>,
             <<"VALUES">>,
             values_parts(length(Columns), NumRows, ParamStyle)],
    Query = join(Parts, <<" ">>),
    list_to_binary(lists:flatten(Query)).

%% @doc Generate values parts of insert query
-spec values_parts(non_neg_integer(), non_neg_integer(), atom()) -> [binary()].
values_parts(NumColumns, NumRows, ParamStyle) ->
    values_parts(NumColumns, NumRows, ParamStyle, 0).

%% @doc Generate values parts with parameter placeholders offset.
%%
%% Offset is useful for parameter placeholder styles that use numbers
%% e.g. dollarn for e.g. postgresql.
-spec values_parts(integer(), integer(), atom(), integer()) -> [binary()].
values_parts(NumColumns, NumRows, ParamStyle, Offset) ->
    ValuesPartF = fun(RowIndex) -> values_part(NumColumns, RowIndex * NumColumns + Offset, ParamStyle) end,
    Parts = [ValuesPartF(RowIndex) || RowIndex <- lists:seq(0, NumRows-1)],
    join(Parts, <<",">>).

%% @doc Generates values part for one row
-spec values_part(integer(), integer(), atom()) -> [binary()].
values_part(NumColumns, Offset, ParamStyle) ->
    [<<"(">>,
     join(placeholders(NumColumns, Offset, ParamStyle), <<",">>),
     <<")">>].

%% @doc Generate columns parts of query.
%%
%% (Just joins them with comma).
%% Assumes input has been validated for safety.
%%
column_parts(Columns) -> join(Columns, <<",">>).

%% @doc Generate "WHERE" parts of query.
%%
%% Returns {SQL, Values}
%%
%% SQL is a list of binaries.
%%
%% See select/4 for more details on supported forms.
%%
where_parts(Where, ParamStyle) ->
    where_parts(Where, ParamStyle, 0).

where_parts({'not', Where}, ParamStyle, ParamPosOffset) ->
    where_unary(<<"NOT">>, Where, ParamStyle, ParamPosOffset);
where_parts({Field, equals, Value}, ParamStyle, ParamPosOffset) ->
    where_binary(Field, <<"=">>, Value, ParamStyle, ParamPosOffset);
where_parts({Field, nequals, Value}, ParamStyle, ParamPosOffset) ->
    where_binary(Field, <<"!=">>, Value, ParamStyle, ParamPosOffset);
where_parts({Field, gt, Value}, ParamStyle, ParamPosOffset) ->
    where_binary(Field, <<">">>, Value, ParamStyle, ParamPosOffset);
where_parts({Field, gte, Value}, ParamStyle, ParamPosOffset) ->
    where_binary(Field, <<">=">>, Value, ParamStyle, ParamPosOffset);
where_parts({Field, lt, Value}, ParamStyle, ParamPosOffset) ->
    where_binary(Field, <<"<">>, Value, ParamStyle, ParamPosOffset);
where_parts({Field, lte, Value}, ParamStyle, ParamPosOffset) ->
    where_binary(Field, <<"<=">>, Value, ParamStyle, ParamPosOffset);
where_parts({Field, in, Values}, ParamStyle, ParamPosOffset) ->
  where_values(Field, <<"IN">>, Values, ParamStyle, ParamPosOffset);
where_parts({Field, notin, Values}, ParamStyle, ParamPosOffset) ->
  where_values(Field, <<"NOT IN">>, Values, ParamStyle, ParamPosOffset);
where_parts({'and', WhereList}, ParamStyle, ParamPosOffset) ->
    where_logic(<<"AND">>, WhereList, ParamStyle, ParamPosOffset);
where_parts({'or', WhereList}, ParamStyle, ParamPosOffset) ->
    where_logic(<<"OR">>, WhereList, ParamStyle, ParamPosOffset).

%% @doc Generate where part for a unary operator.
%% e.g. NOT (...)
where_unary(Op, Where, ParamStyle, ParamPosOffset) ->
    {WhereSub, Values} = where_parts(Where, ParamStyle, ParamPosOffset),
    {[Op, <<" (">>, WhereSub, <<")">>], Values}.

%% @doc Generate where part for a binary operator
%% e.g. Field = Value
where_binary(Field, Op, Value, ParamStyle, ParamPosOffset) ->
    SafeField = ensure_safe([Field]),
    {[SafeField, <<" ">>, Op, <<" ">>, placeholder(1 + ParamPosOffset, ParamStyle)], [Value]}.

%% @doc Generate where part for a logic operator
%% e.g. Where1 AND Where2 AND Where3
where_logic(Op, WhereList, ParamStyle, ParamPosOffset)
  when length(WhereList) > 1 ->
    {WhereSubs, Values} = where_subs(WhereList, ParamStyle, ParamPosOffset),
    Parts = [<<"(">>,
             join(WhereSubs, <<" ", Op/binary, " ">>),
             <<")">>],
    {Parts, Values}.

%% @doc Generate where part for a values operator
%% e.g. Field IN (Value1, Value2, ...)
where_values(Field, Op, Values, ParamStyle, ParamPosOffset)
  when length(Values) > 0 ->
    SafeField = ensure_safe(Field),
    PlaceHolders = join(placeholders(length(Values), ParamPosOffset, ParamStyle), <<",">>),
    {[SafeField, <<" ">>, Op, <<" (">>, PlaceHolders, <<")">>], Values}.

%% @doc Generate where parts for a where list.
%% Results can be 'joined' with the intended operator.
where_subs(WhereList, ParamStyle, ParamPosOffset) when length(WhereList) > 1 ->
    where_subs(WhereList, ParamStyle, ParamPosOffset, [], []).

where_subs([], _ParamStyle, _ParamOffset, WherePartsAcc, ValuesAcc) ->
    {lists:reverse(WherePartsAcc), lists:reverse(ValuesAcc)};

where_subs([Where|WhereList], ParamStyle, ParamPosOffset, WherePartsAcc, ValuesAcc) ->
    {WherePart, Values} = where_parts(Where, ParamStyle, ParamPosOffset),
    ParamPosOffset2 = ParamPosOffset + length(Values),
    where_subs(WhereList, ParamStyle, ParamPosOffset2,
               [WherePart|WherePartsAcc],
               lists:reverse(Values) ++ ValuesAcc).

%%
%% SQL Value Safety
%% Uses RE so can take string and binary as input
%%

-define(SAFE_VALUE_RE, <<"^[A-Za-z0-9_(). \*]*$">>).

%% @doc Checks that value(s) is(are) safe to use while generating SQL.
%% Walks io lists.
%% Returns safe binary value (converts atoms and strings to binary).
ensure_safe(Value) when is_binary(Value) ->
    {match, _} = re:run(Value, ?SAFE_VALUE_RE),
    Value;
ensure_safe([Char|_]=Str) when is_integer(Char) ->
    %% string
    ensure_safe(list_to_binary(Str));
ensure_safe(Value) when is_atom(Value) ->
    ensure_safe(list_to_binary(atom_to_list(Value)));
ensure_safe([H]) ->
    [ensure_safe(H)];
ensure_safe([H|T]) ->
    [ensure_safe(H)|ensure_safe(T)].


%%
%% Parameter placeholders
%%

%% @doc Generates a list of placeholders with the
%% given parameter style.
%%
%% ```
%% 1> placeholders(3, qmark).
%% [<<"?">>,<<"?">>,<<"?">>]
%%
%% 1> placeholders(3, dollarn).
%% [<<"$1">>,<<"$2">>,<<"$3">>]
%%
%% The 3-argument version takes an offset argument.
%%
%% 1> placeholders(3, 4, dollarn).
%% [<<"$5">>,<<"$6">>,<<"$7">>]
%% '''
%%
-spec placeholders(integer(), integer(), atom()) -> [binary()].
placeholders(NumValues, Offset, Style) when NumValues > 0 ->
    [placeholder(N + Offset, Style) || N <- lists:seq(1, NumValues)].

%% @doc Returns a parameter string for the given style
%% at position N in the statement.
-spec placeholder(integer(), qmark | dollarn) -> binary().
placeholder(_Pos, qmark) ->
    <<"?">>;
placeholder(Pos, dollarn) ->
    list_to_binary([<<"$">>, integer_to_list(Pos)]).


%%
%% Utilities
%%

%% @doc Join elements of list with Sep
%%
%% ```1> join([1,2,3], 0).
%% [1,0,2,0,3]'''
%%
join([], _Sep) -> [];
join(L, Sep) -> join(L, Sep, []).

join([H], _Sep, Acc)  -> lists:reverse([H|Acc]);
join([H|T], Sep, Acc) -> join(T, Sep, [Sep, H|Acc]).
