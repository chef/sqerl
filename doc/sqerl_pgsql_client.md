

# Module sqerl_pgsql_client #
* [Description](#description)
* [Function Index](#index)
* [Function Details](#functions)


Abstraction around interacting with pgsql databases
Copyright 2011-2012 Opscode, Inc.
__Behaviours:__ [`sqerl_client`](sqerl_client.md).

__Authors:__ Mark Anderson ([`mark@opscode.com`](mailto:mark@opscode.com)).
<a name="description"></a>

## Description ##

All Rights Reserved.



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
<a name="index"></a>

## Function Index ##


<table width="100%" border="1" cellspacing="0" cellpadding="2" summary="function index"><tr><td valign="top"><a href="#execute-3">execute/3</a></td><td>Execute query or prepared statement.</td></tr><tr><td valign="top"><a href="#init-1">init/1</a></td><td></td></tr><tr><td valign="top"><a href="#is_connected-1">is_connected/1</a></td><td></td></tr><tr><td valign="top"><a href="#prepare-3">prepare/3</a></td><td>Prepare a new statement.</td></tr><tr><td valign="top"><a href="#sql_parameter_style-0">sql_parameter_style/0</a></td><td></td></tr><tr><td valign="top"><a href="#unprepare-3">unprepare/3</a></td><td>Unprepare a previously prepared statement
Protocol between sqerl_client and db-specific modules
uses 3 parameters (QueryOrName, Args, State) for all
calls for simplicity.</td></tr></table>


<a name="functions"></a>

## Function Details ##

<a name="execute-3"></a>

### execute/3 ###


<pre><code>
execute(StatementOrQuery::<a href="#type-sqerl_query">sqerl_query()</a>, Parameters::[any()], State::#state{}) -&gt; {<a href="#type-sqerl_results">sqerl_results()</a>, #state{}}
</code></pre>

<br></br>



Execute query or prepared statement.
If a binary is provided, it is interpreted as an SQL query.
If an atom is provided, it is interpreted as a prepared statement name.



Returns:{Result, State}



Result:
- {ok, Rows}
- {ok, Count}
- {ok, {Count, Rows}}
- {error, ErrorInfo}


Row:  proplist e.g. `[{<<"id">>, 1}, {<<"name">>, <<"Toto">>}]`

<a name="init-1"></a>

### init/1 ###

`init(Config) -> any()`


<a name="is_connected-1"></a>

### is_connected/1 ###

`is_connected(State) -> any()`


<a name="prepare-3"></a>

### prepare/3 ###


<pre><code>
prepare(Name::atom(), SQL::binary(), State::#state{}) -&gt; {ok, #state{}}
</code></pre>

<br></br>


Prepare a new statement.
<a name="sql_parameter_style-0"></a>

### sql_parameter_style/0 ###


<pre><code>
sql_parameter_style() -&gt; dollarn
</code></pre>

<br></br>


__See also:__ [sqerl_adhoc:select/4](sqerl_adhoc.md#select-4).
<a name="unprepare-3"></a>

### unprepare/3 ###


<pre><code>
unprepare(Name::atom(), X2::[], State::#state{}) -&gt; {ok, #state{}}
</code></pre>

<br></br>


Unprepare a previously prepared statement
Protocol between sqerl_client and db-specific modules
uses 3 parameters (QueryOrName, Args, State) for all
calls for simplicity. For an unprepare call, there are
no arguments, so the second parameter of the function
is unused.
