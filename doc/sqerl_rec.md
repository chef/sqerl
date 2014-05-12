

# Module sqerl_rec #
* [Description](#description)
* [Data Types](#types)
* [Function Index](#index)
* [Function Details](#functions)


Record to DB mapping module and behaviour.
Copyright (c) 2014 CHEF Software, Inc. All Rights Reserved.

__This module defines the `sqerl_rec` behaviour.__
<br></br>
 Required callback functions: `#get-/2`, `#new-/1`, `#fromlist-/2`, `#info-/1`, `#insert_fields/0`, `#update_fields/0`, `#statements/0`.

__Authors:__ Seth Falcon ([`seth@getchef.com`](mailto:seth@getchef.com)).
<a name="description"></a>

## Description ##



This module helps you map records to and from the DB using prepared
queries. By creating a module, named the same as your record, and
implementing the `sqerl_rec` behaviour, you can take advantage of a
default set of generated prepared queries and helper functions
(defined in this module) that leverage those queries.



Most of the callbacks can be generated for you if you use the
`exprecs` parse transform. If you use this parse transform, then
you will only need to implement the following three callbacks in
your record module:



* `'#insert_fields'/0` A list of atoms describing the fields (which
should align with column names) used to insert a row into the
db. In many cases this is a proper subset of the record fields to
account for sequence ids and db generated timestamps.

* `'#update_fields'/0` A list of atoms giving the fields used for
updating a row.

* `'#statements'/0` A list of `[default | {atom(),
iolist()}]`. If the atom `default'' is included, then a default
set of queries will be generated. Custom queries provided as
`{Name, SQL}` tuples will override any default queries of the same
name.



If the table name associated with your record name does not follow
the naive pluralization rule implemented by `sqerl_rel`, you can
export a `'#table_name'/0` function to provide the table name for
the mapping.

<a name="types"></a>

## Data Types ##




### <a name="type-db_rec">db_rec()</a> ###



<pre><code>
db_rec() = tuple()
</code></pre>


<a name="index"></a>

## Function Index ##


<table width="100%" border="1" cellspacing="0" cellpadding="2" summary="function index"><tr><td valign="top"><a href="#delete-2">delete/2</a></td><td>Delete the rows where the column identified by <code>By</code> matches
the value as found in <code>Rec</code>.</td></tr><tr><td valign="top"><a href="#fetch-3">fetch/3</a></td><td>Return a list of <code>RecName</code> records using single parameter
prepared query <code>RecName_fetch_by_By</code> where <code>By</code> is a field and
column name and <code>Val</code> is the value of the column to match for in a
WHERE clause.</td></tr><tr><td valign="top"><a href="#fetch_all-1">fetch_all/1</a></td><td>Return all rows from the table associated with record module
<code>RecName</code>.</td></tr><tr><td valign="top"><a href="#fetch_page-3">fetch_page/3</a></td><td>Fetch rows from the table associated with record module
<code>RecName</code> in a paginated fashion.</td></tr><tr><td valign="top"><a href="#first_page-0">first_page/0</a></td><td>Return an ascii value, as a string, that sorts less or equal
to any valid name.</td></tr><tr><td valign="top"><a href="#insert-1">insert/1</a></td><td>Insert record <code>Rec</code> using prepared query <code>RecName_insert</code>.</td></tr><tr><td valign="top"><a href="#qfetch-3">qfetch/3</a></td><td>Fetch using prepared query <code>Query</code> returning a list of records
<code>[#RecName{}]</code>.</td></tr><tr><td valign="top"><a href="#statements-1">statements/1</a></td><td>Given a list of module (and record) names implementing the
<code>sqerl_rec</code> behaviour, return a proplist of prepared queries in the
form of <code>[{QueryName, SQLBinary}]</code>.</td></tr><tr><td valign="top"><a href="#statements_for-1">statements_for/1</a></td><td></td></tr><tr><td valign="top"><a href="#update-1">update/1</a></td><td>Update record <code>Rec</code>.</td></tr></table>


<a name="functions"></a>

## Function Details ##

<a name="delete-2"></a>

### delete/2 ###


<pre><code>
delete(Rec::<a href="#type-db_rec">db_rec()</a>, By::atom()) -&gt; ok | {error, term()}
</code></pre>

<br></br>


Delete the rows where the column identified by `By` matches
the value as found in `Rec`. Typically, one would use `id` to
delete a single row. The prepared query with name
`RecName_delete_by_By` will be used.
<a name="fetch-3"></a>

### fetch/3 ###


<pre><code>
fetch(RecName::atom(), By::atom(), Val::any()) -&gt; [<a href="#type-db_rec">db_rec()</a>] | {error, term()}
</code></pre>

<br></br>


Return a list of `RecName` records using single parameter
prepared query `RecName_fetch_by_By` where `By` is a field and
column name and `Val` is the value of the column to match for in a
WHERE clause. A (possibly empty) list of record results is returned
even though a common use is to fetch a single row.
<a name="fetch_all-1"></a>

### fetch_all/1 ###


<pre><code>
fetch_all(RecName::atom()) -&gt; [<a href="#type-db_rec">db_rec()</a>] | {error, term()}
</code></pre>

<br></br>


Return all rows from the table associated with record module
`RecName`. Results will, by default, be ordered by the name field
(which is assumed to exist).
<a name="fetch_page-3"></a>

### fetch_page/3 ###


<pre><code>
fetch_page(RecName::atom(), StartName::string(), Limit::integer()) -&gt; [<a href="#type-db_rec">db_rec()</a>] | {error, term()}
</code></pre>

<br></br>


Fetch rows from the table associated with record module
`RecName` in a paginated fashion. The default generated query, like
that for `fetch_all`, assumes a `name` field and column and orders
results by this field. The `StartName` argument determines the
start point and `Limit` the number of items to return. To fetch the
"first" page, use [`first_page/0`](#first_page-0). Use the last name received
as the value for `StartName` to fetch the "next" page.
<a name="first_page-0"></a>

### first_page/0 ###

`first_page() -> any()`

Return an ascii value, as a string, that sorts less or equal
to any valid name.
<a name="insert-1"></a>

### insert/1 ###


<pre><code>
insert(Rec::<a href="#type-db_rec">db_rec()</a>) -&gt; [<a href="#type-db_rec">db_rec()</a>] | {error, term()}
</code></pre>

<br></br>


Insert record `Rec` using prepared query `RecName_insert`. The
fields of `Rec` passed as parameters to the query are determined by
`RecName:`#insert_fields/0'. This function assumes the query uses
"INSERT ... RETURNING" and returns a record with db assigned fields
(such as sequence ids and timestamps filled out).
<a name="qfetch-3"></a>

### qfetch/3 ###


<pre><code>
qfetch(RecName::atom(), Query::atom(), Vals::[any()]) -&gt; [<a href="#type-db_rec">db_rec()</a>] | {error, term()}
</code></pre>

<br></br>


Fetch using prepared query `Query` returning a list of records
`[#RecName{}]`. The `Vals` list is the list of parameters for the
prepared query. If the prepared query does not take parameters, use
`[]`.
<a name="statements-1"></a>

### statements/1 ###


<pre><code>
statements(RecList::[atom()]) -&gt; [{atom(), binary()}]
</code></pre>

<br></br>


Given a list of module (and record) names implementing the
`sqerl_rec` behaviour, return a proplist of prepared queries in the
form of `[{QueryName, SQLBinary}]`. If the atom `default'' is
present in the list, then a default set of queries will be
generated. These include: `fetch_by_id`, `fetch_by_name`,
`delete_by_id`, `insert`, `fetch_all`, `fetch_page`, and
`update`. The returned query names will have `Recname_`
prepended. Custom queries override default queries of the same
name.
<a name="statements_for-1"></a>

### statements_for/1 ###


<pre><code>
statements_for(RecName::atom()) -&gt; [{atom(), binary()}]
</code></pre>

<br></br>



<a name="update-1"></a>

### update/1 ###


<pre><code>
update(Rec::<a href="#type-db_rec">db_rec()</a>) -&gt; ok | {error, term()}
</code></pre>

<br></br>


Update record `Rec`. Uses the prepared query with name
`RecName_update`. Assumes an `id` field and corresponding column
which is used to find the row to update. The fields from `Rec`
passed as parameters to the query are determined by
`RecName:`#update_fields/0'.
