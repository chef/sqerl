

# Module sqerl_pgsql_errors #
* [Description](#description)
* [Function Index](#index)
* [Function Details](#functions)


Translates Postgres error codes into human-friendly error tuples.

<a name="index"></a>

## Function Index ##


<table width="100%" border="1" cellspacing="0" cellpadding="2" summary="function index"><tr><td valign="top"><a href="#translate-1">translate/1</a></td><td></td></tr><tr><td valign="top"><a href="#translate_code-1">translate_code/1</a></td><td></td></tr></table>


<a name="functions"></a>

## Function Details ##

<a name="translate-1"></a>

### translate/1 ###


<pre><code>
translate(Error::{error, binary()} | term()) -&gt; {error, atom()} | term()
</code></pre>

<br></br>



<a name="translate_code-1"></a>

### translate_code/1 ###

`translate_code(Error) -> any()`


