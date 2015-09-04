

# Module sqerl_adhoc #
* [Description](#description)
* [Data Types](#types)
* [Function Index](#index)
* [Function Details](#functions)


SQL generation library.
__Authors:__ Jean-Philippe Langlois ([`jpl@opscode.com`](mailto:jpl@opscode.com)).
<a name="description"></a>

## Description ##


Copyright 2011-2012 Opscode, Inc. All Rights Reserved.



This file is provided to you under the Apache License,
Version 2.0 (the "License"); you may not use this file
except in compliance with the License.  You may obtain
a copy of the License at



http://www.apache.org/licenses/LICENSE-2.0


Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.

<a name="types"></a>

## Data Types ##




### <a name="type-sql_clause">sql_clause()</a> ###



<pre><code>
sql_clause() = atom() | tuple()
</code></pre>





### <a name="type-sql_word">sql_word()</a> ###



<pre><code>
sql_word() = <a href="#type-sqerl_sql">sqerl_sql()</a>
</code></pre>


<a name="index"></a>

## Function Index ##


<table width="100%" border="1" cellspacing="0" cellpadding="2" summary="function index"><tr><td valign="top"><a href="#delete-3">delete/3</a></td><td>Generate DELETE statement.</td></tr><tr><td valign="top"><a href="#insert-4">insert/4</a></td><td>Generate INSERT statement for N rows.</td></tr><tr><td valign="top"><a href="#select-4">select/4</a></td><td>Generates SELECT SQL.</td></tr><tr><td valign="top"><a href="#update-4">update/4</a></td><td>Generate UPDATE statement.</td></tr></table>


<a name="functions"></a>

## Function Details ##

<a name="delete-3"></a>

### delete/3 ###


<pre><code>
delete(Table::<a href="#type-sql_word">sql_word()</a>, Where::<a href="#type-sql_clause">sql_clause()</a>, ParamStyle::atom()) -&gt; {binary(), [any()]}
</code></pre>

<br></br>



Generate DELETE statement.



Uses the same Where specifications as select/4.
See select/4 for details about the "Where" parameter.



```
  1> delete(<<"users">>, {<<"id">>, equals, 1}, qmark).
  {<<"DELETE FROM users WHERE id = ?">>, [1]}
```


<a name="insert-4"></a>

### insert/4 ###


<pre><code>
insert(Table::<a href="#type-sql_word">sql_word()</a>, Columns::[<a href="#type-sql_word">sql_word()</a>], NumRows::integer(), ParamStyle::atom()) -&gt; binary()
</code></pre>

<br></br>



Generate INSERT statement for N rows.



```
  1> insert(<<"users">>, [<<"id">>, <<"name">>], 3, qmark).
  <<"INSERT INTO users (name) VALUES (?,?),(?,?),(?,?)">>
```


<a name="select-4"></a>

### select/4 ###


<pre><code>
select(Columns::[<a href="#type-sql_word">sql_word()</a>], Table::<a href="#type-sql_word">sql_word()</a>, Clauses::[] | [<a href="#type-sql_clause">sql_clause()</a>], ParamStyle::atom()) -&gt; {binary(), list()}
</code></pre>

<br></br>



Generates SELECT SQL.
Returns {SQL, ParameterValues} which can be passed on to be executed.
SQL generated uses parameter place holders so query can be
prepared.



Note: Validates that parameters are safe.



```
  Clauses = clause list
  Clause = Where|Order By|Limit
  Where Clause
  -------------
  Form: {where, Where}
  Where = all|undefined
  Does not generate a WHERE clause. Matches all records in table.
  1> select([<<"*">>], <<"users">>, [], qmark).
  {<<"SELECT * FROM users">>, []}
  Where = {Field, equals|nequals|gt|gte|lt|lte, Value}
  Generates SELECT ... WHERE Field =|!=|>|>=|<|<= ?
  1> select([<<"name">>], <<"users">>, [{where, {<<"id">>, equals, 1}}], qmark).
  {<<"SELECT name FROM users WHERE id = ?">>, [1]}
  Where = {Field, in|notin, Values}
  Generates SELECT ... IN|NOT IN SQL with parameter strings (not values),
  which can be prepared and executed.
  1> select([<<"name">>], <<"users">>, [{where, {<<"id">>, in, [1,2,3]}}], qmark).
  {<<"SELECT name FROM users WHERE id IN (?, ?, ?)">>, [1,2,3]}
  Where = {'and'|'or', WhereList}
  Composes WhereList with AND or OR
  1> select([<<"id">>], <<"t">>, [{where, {'or', [{<<"id">>, lt, 5}, {<<"id">>, gt, 10}]}}]).
  {<<"SELECT id FROM t WHERE (id < ? OR id > ?)">>, [5,10]}
  Group By Clause
  ---------------
  Form: {group_by, Fields}
  Order By Clause
  ---------------
  Form: {order_by, Fields | {Fields, asc|desc}}
  Limit/Offset Clause
  --------------------
  Form: {limit, Limit} | {limit, {Limit, offset, Offset}}
  ParamStyle is qmark (?, ?, ... for e.g. mysql)
  or dollarn ($1, $2, etc. for e.g. pgsql)
```

<a name="update-4"></a>

### update/4 ###


<pre><code>
update(Table::<a href="#type-sql_word">sql_word()</a>, Row::<a href="#type-sqerl_row">sqerl_row()</a>, Where::<a href="#type-sql_clause">sql_clause()</a>, ParamStyle::atom()) -&gt; {binary(), list()}
</code></pre>

<br></br>



Generate UPDATE statement.



Update is given under the form of a Row (proplist).
Uses the same Where specifications as select/4 except for "all" which is
not supported.



```
  1> update(<<"users">>, [{<<"last_name">>, <<"Toto">>}], {<<"id">>, equals, 1}, qmark).
  {<<"UPDATE users SET last_name = ? WHERE id = ?">>, [<<"Toto">>, 1]}
```
