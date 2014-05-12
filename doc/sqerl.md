

# Module sqerl #
* [Function Index](#index)
* [Function Details](#functions)

__Authors:__ Seth Falcon ([`seth@opscode.com`](mailto:seth@opscode.com)), Mark Anderson ([`mark@opscode.com`](mailto:mark@opscode.com)).
<a name="index"></a>

## Function Index ##


<table width="100%" border="1" cellspacing="0" cellpadding="2" summary="function index"><tr><td valign="top"><a href="#adhoc_delete-2">adhoc_delete/2</a></td><td>Adhoc delete.</td></tr><tr><td valign="top"><a href="#adhoc_insert-2">adhoc_insert/2</a></td><td>Insert Rows into Table with default batch size.</td></tr><tr><td valign="top"><a href="#adhoc_insert-3">adhoc_insert/3</a></td><td>Insert Rows into Table with given batch size.</td></tr><tr><td valign="top"><a href="#adhoc_insert-4">adhoc_insert/4</a></td><td>Insert records defined by {Columns, RowsValues}
into Table using given BatchSize.</td></tr><tr><td valign="top"><a href="#adhoc_select-3">adhoc_select/3</a></td><td>Execute an adhoc select query.</td></tr><tr><td valign="top"><a href="#adhoc_select-4">adhoc_select/4</a></td><td>Execute an adhoc select query with additional clauses.</td></tr><tr><td valign="top"><a href="#checkin-1">checkin/1</a></td><td></td></tr><tr><td valign="top"><a href="#checkout-0">checkout/0</a></td><td></td></tr><tr><td valign="top"><a href="#execute-1">execute/1</a></td><td>Execute query or statement with no parameters.</td></tr><tr><td valign="top"><a href="#execute-2">execute/2</a></td><td>Execute query or statement with parameters.</td></tr><tr><td valign="top"><a href="#extract_insert_data-1">extract_insert_data/1</a></td><td>Extract insert data from Rows.</td></tr><tr><td valign="top"><a href="#select-2">select/2</a></td><td></td></tr><tr><td valign="top"><a href="#select-3">select/3</a></td><td></td></tr><tr><td valign="top"><a href="#select-4">select/4</a></td><td></td></tr><tr><td valign="top"><a href="#statement-2">statement/2</a></td><td></td></tr><tr><td valign="top"><a href="#statement-3">statement/3</a></td><td></td></tr><tr><td valign="top"><a href="#statement-4">statement/4</a></td><td></td></tr><tr><td valign="top"><a href="#with_db-1">with_db/1</a></td><td></td></tr></table>


<a name="functions"></a>

## Function Details ##

<a name="adhoc_delete-2"></a>

### adhoc_delete/2 ###


<pre><code>
adhoc_delete(Table::binary(), Where::term()) -&gt; {ok, integer()} | {error, any()}
</code></pre>

<br></br>


Adhoc delete.
Uses the same Where specifications as adhoc_select/3.
Returns {ok, Count} or {error, ErrorInfo}.

<a name="adhoc_insert-2"></a>

### adhoc_insert/2 ###

`adhoc_insert(Table, Rows) -> any()`

Insert Rows into Table with default batch size.

__See also:__ [adhoc_insert/3](#adhoc_insert-3).
<a name="adhoc_insert-3"></a>

### adhoc_insert/3 ###

`adhoc_insert(Table, Rows, BatchSize) -> any()`


Insert Rows into Table with given batch size.


Reformats input data to {Columns, RowsValues} and
calls adhoc_insert/4.

```
  - Rows: list of proplists (such as returned by a select) e.g.
  [
      [{<<"id">>, 1},{<<"first_name">>, <<"Kevin">>}],
      [{<<"id">>, 2},{<<"first_name">>, <<"Mark">>}]
    ]
```

Returns {ok, InsertCount}


__See also:__ [adhoc_insert/4](#adhoc_insert-4).
<a name="adhoc_insert-4"></a>

### adhoc_insert/4 ###

`adhoc_insert(Table, Columns, RowsValues, BatchSize) -> any()`

Insert records defined by {Columns, RowsValues}
into Table using given BatchSize.

```
  - Columns, RowsValues e.g.
    {[<<"first_name">>, <<"last_name">>],
       [
         [<<"Joe">>, <<"Blow">>],
         [<<"John">>, <<"Doe">>]
       ]}
  Returns {ok, InsertedCount}.
  1> adhoc_insert(<<"users">>,
         {[<<"first_name">>, <<"last_name">>],
          [[<<"Joe">>, <<"Blow">>],
           [<<"John">>, <<"Doe">>]]}).
  {ok, 2}
```


<a name="adhoc_select-3"></a>

### adhoc_select/3 ###


<pre><code>
adhoc_select(Columns::[binary() | string()], Table::binary() | string(), Where::atom() | tuple()) -&gt; <a href="#type-sqerl_results">sqerl_results()</a>
</code></pre>

<br></br>


Execute an adhoc select query.

```
  Returns:
  - {ok, Rows}
  - {error, ErrorInfo}
  See execute/2 for more details on return data.
  Where Clause
  -------------
  Form: {where, Where}
  Where = all|undefined -- Does not generate a WHERE clause.
                           Matches all records in table.
  Where = {Field, equals|nequals|gt|gte|lt|lte, Value}
  Where = {Field, in|notin, Values}
  Where = {'and'|'or', WhereList} -- Composes WhereList with AND or OR
  adhoc_select/4 takes an additional Clauses argument which
  is a list of additional clauses for the query.
```

<a name="adhoc_select-4"></a>

### adhoc_select/4 ###


<pre><code>
adhoc_select(Columns::[binary() | string()], Table::binary() | string(), Where::atom() | tuple(), Clauses::[] | [atom() | tuple()]) -&gt; <a href="#type-sqerl_results">sqerl_results()</a>
</code></pre>

<br></br>


Execute an adhoc select query with additional clauses.

```
  Group By Clause
  ---------------
  Form: {groupby, Fields}
  Order By Clause
  ---------------
  Form: {orderby, Fields | {Fields, asc|desc}}
  Limit/Offset Clause
  --------------------
  Form: {limit, Limit} | {limit, {Limit, offset, Offset}}
```

See itest:adhoc_select_complex/0 for an example of a complex query
that uses several clauses.
<a name="checkin-1"></a>

### checkin/1 ###

`checkin(Connection) -> any()`


<a name="checkout-0"></a>

### checkout/0 ###

`checkout() -> any()`


<a name="execute-1"></a>

### execute/1 ###


<pre><code>
execute(QueryOrStatement::<a href="#type-sqerl_query">sqerl_query()</a>) -&gt; <a href="#type-sqerl_results">sqerl_results()</a>
</code></pre>

<br></br>


Execute query or statement with no parameters.
See execute/2 for return info.
<a name="execute-2"></a>

### execute/2 ###


<pre><code>
execute(QueryOrStatement::<a href="#type-sqerl_query">sqerl_query()</a>, Parameters::[] | [term()]) -&gt; <a href="#type-sqerl_results">sqerl_results()</a>
</code></pre>

<br></br>


Execute query or statement with parameters.

```
  Returns:
  - {ok, Result}
  - {error, ErrorInfo}
  Result depends on the query being executed, and can be
  - Rows
  - Count
  Row is a proplist-like array, e.g. [{<<"id">>, 1}, {<<"name">>, <<"John">>}]
  Note that both a simple query and a prepared statement can take
  parameters.
```


<a name="extract_insert_data-1"></a>

### extract_insert_data/1 ###


<pre><code>
extract_insert_data(Rows::<a href="#type-sqerl_rows">sqerl_rows()</a>) -&gt; {[binary()], [[term()]]}
</code></pre>

<br></br>



Extract insert data from Rows.



Assumes all rows have the same format.
Returns {Columns, RowsValues}.



```
  1> extract_insert_data([
                          [{<<"id">>, 1}, {<<"name">>, <<"Joe">>}],
                          [{<<"id">>, 2}, {<<"name">>, <<"Jeff">>}],
                         ]).
  {[<<"id">>,<<"name">>],[[1,<<"Joe">>],[2,<<"Jeff">>]]}
```


<a name="select-2"></a>

### select/2 ###

`select(StmtName, StmtArgs) -> any()`


<a name="select-3"></a>

### select/3 ###

`select(StmtName, StmtArgs, XformName) -> any()`


<a name="select-4"></a>

### select/4 ###

`select(StmtName, StmtArgs, XformName, XformArgs) -> any()`


<a name="statement-2"></a>

### statement/2 ###

`statement(StmtName, StmtArgs) -> any()`


<a name="statement-3"></a>

### statement/3 ###

`statement(StmtName, StmtArgs, XformName) -> any()`


<a name="statement-4"></a>

### statement/4 ###

`statement(StmtName, StmtArgs, XformName, XformArgs) -> any()`


<a name="with_db-1"></a>

### with_db/1 ###

`with_db(Call) -> any()`


