%% -*- erlang-indent-level: 4;indent-tabs-mode: nil; fill-column: 92 -*-
%% ex: ts=4 sw=4 et
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

%% @doc Translates Postgres error codes into human-friendly error tuples
-module(sqerl_pgsql_errors).

-include_lib("epgsql/include/pgsql.hrl").

-export([translate/1,
         translate_code/1]).

%% Error codes taken from http://www.postgresql.org/docs/9.1/static/errcodes-appendix.html

-spec translate(#error{} | {error, binary()} | term()) -> {error, atom()} | term().
translate(#error{code=Code}=_Error) ->
    translate({error, Code});
translate({error, Code}) when is_binary(Code) ->
    {error, {Code, translate_code(Code)}};
translate(Error) ->
    Error.

%% Private functions
translate_code(<<"08000">>) -> connection_exception;
translate_code(<<"08003">>) -> connection_does_not_exist;
translate_code(<<"08006">>) -> connection_failure;
translate_code(<<"08001">>) -> sqlclient_unable_to_establish_sqlconnection;
translate_code(<<"08004">>) -> sqlserver_rejected_establishment_of_sqlconnection;
translate_code(<<"08007">>) -> transaction_resolution_unknown;
translate_code(<<"08P01">>) -> protocol_violation;
translate_code(<<"09000">>) -> triggered_action_exception;
translate_code(<<"0A000">>) -> feature_not_supported;
translate_code(<<"0B000">>) -> invalid_transaction_initiation;
translate_code(<<"0F000">>) -> locator_exception;
translate_code(<<"0F001">>) -> invalid_locator_specification;
translate_code(<<"0L000">>) -> invalid_grantor;
translate_code(<<"0LP01">>) -> invalid_grant_operation;
translate_code(<<"0P000">>) -> invalid_role_specification;
translate_code(<<"20000">>) -> case_not_found;
translate_code(<<"21000">>) -> cardinality_violation;
translate_code(<<"22000">>) -> data_exception;
translate_code(<<"2202E">>) -> array_subscript_error;
translate_code(<<"22021">>) -> character_not_in_repertoire;
translate_code(<<"22008">>) -> datetime_field_overflow;
translate_code(<<"22012">>) -> division_by_zero;
translate_code(<<"22005">>) -> error_in_assignment;
translate_code(<<"2200B">>) -> escape_character_conflict;
translate_code(<<"22022">>) -> indicator_overflow;
translate_code(<<"22015">>) -> interval_field_overflow;
translate_code(<<"2201E">>) -> invalid_argument_for_logarithm;
translate_code(<<"22014">>) -> invalid_argument_for_ntile_function;
translate_code(<<"22016">>) -> invalid_argument_for_nth_value_function;
translate_code(<<"2201F">>) -> invalid_argument_for_power_function;
translate_code(<<"2201G">>) -> invalid_argument_for_width_bucket_function;
translate_code(<<"22018">>) -> invalid_character_value_for_cast;
translate_code(<<"22007">>) -> invalid_datetime_format;
translate_code(<<"22019">>) -> invalid_escape_character;
translate_code(<<"2200D">>) -> invalid_escape_octet;
translate_code(<<"22025">>) -> invalid_escape_sequence;
translate_code(<<"22P06">>) -> nonstandard_use_of_escape_character;
translate_code(<<"22010">>) -> invalid_indicator_parameter_value;
translate_code(<<"22023">>) -> invalid_parameter_value;
translate_code(<<"2201B">>) -> invalid_regular_expression;
translate_code(<<"2201W">>) -> invalid_row_count_in_limit_clause;
translate_code(<<"2201X">>) -> invalid_row_count_in_result_offset_clause;
translate_code(<<"22009">>) -> invalid_time_zone_displacement_value;
translate_code(<<"2200C">>) -> invalid_use_of_escape_character;
translate_code(<<"2200G">>) -> most_specific_type_mismatch;
translate_code(<<"22004">>) -> null_value_not_allowed;
translate_code(<<"22002">>) -> null_value_no_indicator_parameter;
translate_code(<<"22003">>) -> numeric_value_out_of_range;
translate_code(<<"22026">>) -> string_data_length_mismatch;
translate_code(<<"22001">>) -> string_data_right_truncation;
translate_code(<<"22011">>) -> substring_error;
translate_code(<<"22027">>) -> trim_error;
translate_code(<<"22024">>) -> unterminated_c_string;
translate_code(<<"2200F">>) -> zero_length_character_string;
translate_code(<<"22P01">>) -> floating_point_exception;
translate_code(<<"22P02">>) -> invalid_text_representation;
translate_code(<<"22P03">>) -> invalid_binary_representation;
translate_code(<<"22P04">>) -> bad_copy_file_format;
translate_code(<<"22P05">>) -> untranslatable_character;
translate_code(<<"2200L">>) -> not_an_xml_document;
translate_code(<<"2200M">>) -> invalid_xml_document;
translate_code(<<"2200N">>) -> invalid_xml_content;
translate_code(<<"2200S">>) -> invalid_xml_comment;
translate_code(<<"2200T">>) -> invalid_xml_processing_instruction;
translate_code(<<"23000">>) -> integrity_constraint_violation;
translate_code(<<"23001">>) -> restrict_violation;
translate_code(<<"23502">>) -> not_null_violation;
translate_code(<<"23503">>) -> foreign_key_violation;
translate_code(<<"23505">>) -> unique_violation;
translate_code(<<"23514">>) -> check_violation;
translate_code(<<"23P01">>) -> exclusion_violation;
translate_code(<<"24000">>) -> invalid_cursor_state;
translate_code(<<"25000">>) -> invalid_transaction_state;
translate_code(<<"25001">>) -> active_sql_transaction;
translate_code(<<"25002">>) -> branch_transaction_already_active;
translate_code(<<"25008">>) -> held_cursor_requires_same_isolation_level;
translate_code(<<"25003">>) -> inappropriate_access_mode_for_branch_transaction;
translate_code(<<"25004">>) -> inappropriate_isolation_level_for_branch_transaction;
translate_code(<<"25005">>) -> no_active_sql_transaction_for_branch_transaction;
translate_code(<<"25006">>) -> read_only_sql_transaction;
translate_code(<<"25007">>) -> schema_and_data_statement_mixing_not_supported;
translate_code(<<"25P01">>) -> no_active_sql_transaction;
translate_code(<<"25P02">>) -> in_failed_sql_transaction;
translate_code(<<"26000">>) -> invalid_sql_statement_name;
translate_code(<<"27000">>) -> triggered_data_change_violation;
translate_code(<<"28000">>) -> invalid_authorization_specification;
translate_code(<<"28P01">>) -> invalid_password;
translate_code(<<"2B000">>) -> dependent_privilege_descriptors_still_exist;
translate_code(<<"2BP01">>) -> dependent_objects_still_exist;
translate_code(<<"2D000">>) -> invalid_transaction_termination;
translate_code(<<"2F000">>) -> sql_routine_exception;
translate_code(<<"2F005">>) -> function_executed_no_return_statement;
translate_code(<<"2F002">>) -> modifying_sql_data_not_permitted;
translate_code(<<"2F003">>) -> prohibited_sql_statement_attempted;
translate_code(<<"2F004">>) -> reading_sql_data_not_permitted;
translate_code(<<"34000">>) -> invalid_cursor_name;
translate_code(<<"38000">>) -> external_routine_exception;
translate_code(<<"38001">>) -> containing_sql_not_permitted;
translate_code(<<"38002">>) -> modifying_sql_data_not_permitted;
translate_code(<<"38003">>) -> prohibited_sql_statement_attempted;
translate_code(<<"38004">>) -> reading_sql_data_not_permitted;
translate_code(<<"39000">>) -> external_routine_invocation_exception;
translate_code(<<"39001">>) -> invalid_sqlstate_returned;
translate_code(<<"39004">>) -> null_value_not_allowed;
translate_code(<<"39P01">>) -> trigger_protocol_violated;
translate_code(<<"39P02">>) -> srf_protocol_violated;
translate_code(<<"3B000">>) -> savepoint_exception;
translate_code(<<"3B001">>) -> invalid_savepoint_specification;
translate_code(<<"3D000">>) -> invalid_catalog_name;
translate_code(<<"3F000">>) -> invalid_schema_name;
translate_code(<<"40000">>) -> transaction_rollback;
translate_code(<<"40002">>) -> transaction_integrity_constraint_violation;
translate_code(<<"40001">>) -> serialization_failure;
translate_code(<<"40003">>) -> statement_completion_unknown;
translate_code(<<"40P01">>) -> deadlock_detected;
translate_code(<<"42000">>) -> syntax_error_or_access_rule_violation;
translate_code(<<"42601">>) -> syntax_error;
translate_code(<<"42501">>) -> insufficient_privilege;
translate_code(<<"42846">>) -> cannot_coerce;
translate_code(<<"42803">>) -> grouping_error;
translate_code(<<"42P20">>) -> windowing_error;
translate_code(<<"42P19">>) -> invalid_recursion;
translate_code(<<"42830">>) -> invalid_foreign_key;
translate_code(<<"42602">>) -> invalid_name;
translate_code(<<"42622">>) -> name_too_long;
translate_code(<<"42939">>) -> reserved_name;
translate_code(<<"42804">>) -> datatype_mismatch;
translate_code(<<"42P18">>) -> indeterminate_datatype;
translate_code(<<"42P21">>) -> collation_mismatch;
translate_code(<<"42P22">>) -> indeterminate_collation;
translate_code(<<"42809">>) -> wrong_object_type;
translate_code(<<"42703">>) -> undefined_column;
translate_code(<<"42883">>) -> undefined_function;
translate_code(<<"42P01">>) -> undefined_table;
translate_code(<<"42P02">>) -> undefined_parameter;
translate_code(<<"42704">>) -> undefined_object;
translate_code(<<"42701">>) -> duplicate_column;
translate_code(<<"42P03">>) -> duplicate_cursor;
translate_code(<<"42P04">>) -> duplicate_database;
translate_code(<<"42723">>) -> duplicate_function;
translate_code(<<"42P05">>) -> duplicate_prepared_statement;
translate_code(<<"42P06">>) -> duplicate_schema;
translate_code(<<"42P07">>) -> duplicate_table;
translate_code(<<"42712">>) -> duplicate_alias;
translate_code(<<"42710">>) -> duplicate_object;
translate_code(<<"42702">>) -> ambiguous_column;
translate_code(<<"42725">>) -> ambiguous_function;
translate_code(<<"42P08">>) -> ambiguous_parameter;
translate_code(<<"42P09">>) -> ambiguous_alias;
translate_code(<<"42P10">>) -> invalid_column_reference;
translate_code(<<"42611">>) -> invalid_column_definition;
translate_code(<<"42P11">>) -> invalid_cursor_definition;
translate_code(<<"42P12">>) -> invalid_database_definition;
translate_code(<<"42P13">>) -> invalid_function_definition;
translate_code(<<"42P14">>) -> invalid_prepared_statement_definition;
translate_code(<<"42P15">>) -> invalid_schema_definition;
translate_code(<<"42P16">>) -> invalid_table_definition;
translate_code(<<"42P17">>) -> invalid_object_definition;
translate_code(<<"44000">>) -> with_check_option_violation;
translate_code(<<"53000">>) -> insufficient_resources;
translate_code(<<"53100">>) -> disk_full;
translate_code(<<"53200">>) -> out_of_memory;
translate_code(<<"53300">>) -> too_many_connections;
translate_code(<<"54000">>) -> program_limit_exceeded;
translate_code(<<"54001">>) -> statement_too_complex;
translate_code(<<"54011">>) -> too_many_columns;
translate_code(<<"54023">>) -> too_many_arguments;
translate_code(<<"55000">>) -> object_not_in_prerequisite_state;
translate_code(<<"55006">>) -> object_in_use;
translate_code(<<"55P02">>) -> cant_change_runtime_param;
translate_code(<<"55P03">>) -> lock_not_available;
translate_code(<<"57000">>) -> operator_intervention;
translate_code(<<"57014">>) -> query_canceled;
translate_code(<<"57P01">>) -> admin_shutdown;
translate_code(<<"57P02">>) -> crash_shutdown;
translate_code(<<"57P03">>) -> cannot_connect_now;
translate_code(<<"57P04">>) -> database_dropped;
translate_code(<<"58030">>) -> io_error;
translate_code(<<"58P01">>) -> undefined_file;
translate_code(<<"58P02">>) -> duplicate_file;
translate_code(<<"F0000">>) -> config_file_error;
translate_code(<<"F0001">>) -> lock_file_exists;
translate_code(<<"HV000">>) -> fdw_error;
translate_code(<<"HV005">>) -> fdw_column_name_not_found;
translate_code(<<"HV002">>) -> fdw_dynamic_parameter_value_needed;
translate_code(<<"HV010">>) -> fdw_function_sequence_error;
translate_code(<<"HV021">>) -> fdw_inconsistent_descriptor_information;
translate_code(<<"HV024">>) -> fdw_invalid_attribute_value;
translate_code(<<"HV007">>) -> fdw_invalid_column_name;
translate_code(<<"HV008">>) -> fdw_invalid_column_number;
translate_code(<<"HV004">>) -> fdw_invalid_data_type;
translate_code(<<"HV006">>) -> fdw_invalid_data_type_descriptors;
translate_code(<<"HV091">>) -> fdw_invalid_descriptor_field_identifier;
translate_code(<<"HV00B">>) -> fdw_invalid_handle;
translate_code(<<"HV00C">>) -> fdw_invalid_option_index;
translate_code(<<"HV00D">>) -> fdw_invalid_option_name;
translate_code(<<"HV090">>) -> fdw_invalid_string_length_or_buffer_length;
translate_code(<<"HV00A">>) -> fdw_invalid_string_format;
translate_code(<<"HV009">>) -> fdw_invalid_use_of_null_pointer;
translate_code(<<"HV014">>) -> fdw_too_many_handles;
translate_code(<<"HV001">>) -> fdw_out_of_memory;
translate_code(<<"HV00P">>) -> fdw_no_schemas;
translate_code(<<"HV00J">>) -> fdw_option_name_not_found;
translate_code(<<"HV00K">>) -> fdw_reply_handle;
translate_code(<<"HV00Q">>) -> fdw_schema_not_found;
translate_code(<<"HV00R">>) -> fdw_table_not_found;
translate_code(<<"HV00L">>) -> fdw_unable_to_create_execution;
translate_code(<<"HV00M">>) -> fdw_unable_to_create_reply;
translate_code(<<"HV00N">>) -> fdw_unable_to_establish_connection;
translate_code(<<"P0000">>) -> plpgsql_error;
translate_code(<<"P0001">>) -> raise_exception;
translate_code(<<"P0002">>) -> no_data_found;
translate_code(<<"P0003">>) -> too_many_rows;
translate_code(Error) -> Error.
