

# Module sqerl_transformers #
* [Description](#description)
* [Function Index](#index)
* [Function Details](#functions)


Abstraction around interacting with database results
Copyright 2011-2012 Opscode, Inc.
__Authors:__ Kevin Smith ([`kevin@opscode.com`](mailto:kevin@opscode.com)), Christopher Maier ([`cm@opscode.com`](mailto:cm@opscode.com)).
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


<table width="100%" border="1" cellspacing="0" cellpadding="2" summary="function index"><tr><td valign="top"><a href="#by_column_name-2">by_column_name/2</a></td><td></td></tr><tr><td valign="top"><a href="#convert_YMDHMS_tuple_to_datetime-1">convert_YMDHMS_tuple_to_datetime/1</a></td><td></td></tr><tr><td valign="top"><a href="#convert_integer_to_boolean-1">convert_integer_to_boolean/1</a></td><td>helper column transformer for mysql where booleans are represented as
tinyint(1) and can take on the value of 0 or 1.</td></tr><tr><td valign="top"><a href="#count-0">count/0</a></td><td></td></tr><tr><td valign="top"><a href="#first-0">first/0</a></td><td></td></tr><tr><td valign="top"><a href="#first_as_record-2">first_as_record/2</a></td><td></td></tr><tr><td valign="top"><a href="#first_as_scalar-1">first_as_scalar/1</a></td><td></td></tr><tr><td valign="top"><a href="#identity-0">identity/0</a></td><td></td></tr><tr><td valign="top"><a href="#parse_timestamp_to_datetime-1">parse_timestamp_to_datetime/1</a></td><td></td></tr><tr><td valign="top"><a href="#rows-0">rows/0</a></td><td></td></tr><tr><td valign="top"><a href="#rows_as_records-2">rows_as_records/2</a></td><td></td></tr><tr><td valign="top"><a href="#rows_as_scalars-1">rows_as_scalars/1</a></td><td></td></tr></table>


<a name="functions"></a>

## Function Details ##

<a name="by_column_name-2"></a>

### by_column_name/2 ###

`by_column_name(Rows, Transforms) -> any()`


<a name="convert_YMDHMS_tuple_to_datetime-1"></a>

### convert_YMDHMS_tuple_to_datetime/1 ###

`convert_YMDHMS_tuple_to_datetime(X1) -> any()`


<a name="convert_integer_to_boolean-1"></a>

### convert_integer_to_boolean/1 ###


<pre><code>
convert_integer_to_boolean(N::non_neg_integer()) -&gt; boolean()
</code></pre>

<br></br>


helper column transformer for mysql where booleans are represented as
tinyint(1) and can take on the value of 0 or 1.
<a name="count-0"></a>

### count/0 ###

`count() -> any()`


<a name="first-0"></a>

### first/0 ###

`first() -> any()`


<a name="first_as_record-2"></a>

### first_as_record/2 ###

`first_as_record(RecName, RecInfo) -> any()`


<a name="first_as_scalar-1"></a>

### first_as_scalar/1 ###

`first_as_scalar(Field) -> any()`


<a name="identity-0"></a>

### identity/0 ###

`identity() -> any()`


<a name="parse_timestamp_to_datetime-1"></a>

### parse_timestamp_to_datetime/1 ###

`parse_timestamp_to_datetime(TS) -> any()`


<a name="rows-0"></a>

### rows/0 ###

`rows() -> any()`


<a name="rows_as_records-2"></a>

### rows_as_records/2 ###

`rows_as_records(RecName, RecInfo) -> any()`


<a name="rows_as_scalars-1"></a>

### rows_as_scalars/1 ###

`rows_as_scalars(Field) -> any()`


