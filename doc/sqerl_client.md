

# Module sqerl_client #
* [Description](#description)
* [Function Index](#index)
* [Function Details](#functions)


Abstraction around interacting with SQL databases
Copyright 2011-2012 Opscode, Inc.
__Behaviours:__ [`gen_server`](gen_server.md).

__This module defines the `sqerl_client` behaviour.__
<br></br>
 Required callback functions: `init/1`, `execute/3`, `is_connected/1`, `sql_parameter_style/0`, `prepare/3`, `unprepare/3`.

__Authors:__ Kevin Smith ([`kevin@opscode.com`](mailto:kevin@opscode.com)).
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


<table width="100%" border="1" cellspacing="0" cellpadding="2" summary="function index"><tr><td valign="top"><a href="#close-1">close/1</a></td><td></td></tr><tr><td valign="top"><a href="#code_change-3">code_change/3</a></td><td></td></tr><tr><td valign="top"><a href="#execute-2">execute/2</a></td><td>Execute SQL or prepared statement with no parameters.</td></tr><tr><td valign="top"><a href="#execute-3">execute/3</a></td><td>Execute SQL or prepared statement with parameters.</td></tr><tr><td valign="top"><a href="#handle_call-3">handle_call/3</a></td><td></td></tr><tr><td valign="top"><a href="#handle_cast-2">handle_cast/2</a></td><td></td></tr><tr><td valign="top"><a href="#handle_info-2">handle_info/2</a></td><td></td></tr><tr><td valign="top"><a href="#init-1">init/1</a></td><td></td></tr><tr><td valign="top"><a href="#prepare-3">prepare/3</a></td><td>Prepare a statement.</td></tr><tr><td valign="top"><a href="#sql_parameter_style-0">sql_parameter_style/0</a></td><td>Returns SQL parameter style atom, e.g.</td></tr><tr><td valign="top"><a href="#start_link-0">start_link/0</a></td><td></td></tr><tr><td valign="top"><a href="#start_link-1">start_link/1</a></td><td></td></tr><tr><td valign="top"><a href="#terminate-2">terminate/2</a></td><td></td></tr><tr><td valign="top"><a href="#unprepare-2">unprepare/2</a></td><td>Unprepare a previously prepared statement.</td></tr></table>


<a name="functions"></a>

## Function Details ##

<a name="close-1"></a>

### close/1 ###


<pre><code>
close(Cn::pid()) -&gt; ok
</code></pre>

<br></br>



<a name="code_change-3"></a>

### code_change/3 ###

`code_change(OldVsn, State, Extra) -> any()`


<a name="execute-2"></a>

### execute/2 ###


<pre><code>
execute(Cn::pid(), QueryOrStatement::<a href="#type-sqerl_query">sqerl_query()</a>) -&gt; <a href="#type-sqerl_results">sqerl_results()</a>
</code></pre>

<br></br>


Execute SQL or prepared statement with no parameters.
See execute/3 for return values.
<a name="execute-3"></a>

### execute/3 ###


<pre><code>
execute(Cn::pid(), QueryOrStatement::<a href="#type-sqerl_query">sqerl_query()</a>, Parameters::[any()]) -&gt; <a href="#type-sqerl_results">sqerl_results()</a>
</code></pre>

<br></br>


Execute SQL or prepared statement with parameters.
<a name="handle_call-3"></a>

### handle_call/3 ###

`handle_call(Request, From, State) -> any()`


<a name="handle_cast-2"></a>

### handle_cast/2 ###

`handle_cast(Msg, State) -> any()`


<a name="handle_info-2"></a>

### handle_info/2 ###

`handle_info(Info, State) -> any()`


<a name="init-1"></a>

### init/1 ###

`init(CallbackMod) -> any()`


<a name="prepare-3"></a>

### prepare/3 ###


<pre><code>
prepare(Cn::pid(), Name::atom(), SQL::binary()) -&gt; ok | {error, any()}
</code></pre>

<br></br>


Prepare a statement

<a name="sql_parameter_style-0"></a>

### sql_parameter_style/0 ###

`sql_parameter_style() -> any()`

Returns SQL parameter style atom, e.g. qmark, dollarn.
Note on approach: here we rely on sqerl config in
application environment to retrieve db type and from there
call the appropriate driver module.
It would be better to not be tied to how sqerl is
configured and instead retrieve that from state somewhere.
However, retrieving that from state implies making a call
to a process somewhere which comes with its set of
implications, contention being a potential issue.
-spec sql_parameter_style() -> atom().
<a name="start_link-0"></a>

### start_link/0 ###

`start_link() -> any()`


<a name="start_link-1"></a>

### start_link/1 ###

`start_link(DbType) -> any()`


<a name="terminate-2"></a>

### terminate/2 ###

`terminate(Reason, State) -> any()`


<a name="unprepare-2"></a>

### unprepare/2 ###


<pre><code>
unprepare(Cn::pid(), Name::atom()) -&gt; ok | {error, any()}
</code></pre>

<br></br>


Unprepare a previously prepared statement
