%% -*- erlang-indent-level: 4;indent-tabs-mode: nil; fill-column: 92 -*-
%% ex: ts=4 sw=4 et
%% @author Kevin Smith <kevin@opscode.com>
%% @doc Abstraction around interacting with SQL databases
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

-module(sqerl_pgsql_errors).

-export([translate/1]).

%% Error codes taken from http://www.postgresql.org/docs/9.1/static/errcodes-appendix.html

-spec translate(binary() | term()) -> {error, atom()} | term().
translate({error, <<"08000">>}) -> {error, connection_exception};
translate({error, <<"08003">>}) -> {error, connection_does_not_exist};
translate({error, <<"08006">>}) -> {error, connection_failure};
translate({error, <<"08001">>}) -> {error, sqlclient_unable_to_establish_sqlconnection};
translate({error, <<"08004">>}) -> {error, sqlserver_rejected_establishment_of_sqlconnection};
translate({error, <<"08007">>}) -> {error, transaction_resolution_unknown};
translate({error, <<"08P01">>}) -> {error, protocol_violation};
translate({error, <<"09000">>}) -> {error, triggered_action_exception};
translate({error, <<"0A000">>}) -> {error, feature_not_supported};
translate({error, <<"0B000">>}) -> {error, invalid_transaction_initiation};
translate({error, <<"0F000">>}) -> {error, locator_exception};
translate({error, <<"0F001">>}) -> {error, invalid_locator_specification};
translate({error, <<"0L000">>}) -> {error, invalid_grantor};
translate({error, <<"0LP01">>}) -> {error, invalid_grant_operation};
translate({error, <<"0P000">>}) -> {error, invalid_role_specification};
translate({error, <<"20000">>}) -> {error, case_not_found};
translate({error, <<"21000">>}) -> {error, cardinality_violation};
translate({error, <<"22000">>}) -> {error, data_exception};
translate({error, <<"2202E">>}) -> {error, array_subscript_error};
translate({error, <<"22021">>}) -> {error, character_not_in_repertoire};
translate({error, <<"22008">>}) -> {error, datetime_field_overflow};
translate({error, <<"22012">>}) -> {error, division_by_zero};
translate({error, <<"22005">>}) -> {error, error_in_assignment};
translate({error, <<"2200B">>}) -> {error, escape_character_conflict};
translate({error, <<"22022">>}) -> {error, indicator_overflow};
translate({error, <<"22015">>}) -> {error, interval_field_overflow};
translate({error, <<"2201E">>}) -> {error, invalid_argument_for_logarithm};
translate({error, <<"22014">>}) -> {error, invalid_argument_for_ntile_function};
translate({error, <<"22016">>}) -> {error, invalid_argument_for_nth_value_function};
translate({error, <<"2201F">>}) -> {error, invalid_argument_for_power_function};
translate({error, <<"2201G">>}) -> {error, invalid_argument_for_width_bucket_function};
translate({error, <<"22018">>}) -> {error, invalid_character_value_for_cast};
translate({error, <<"22007">>}) -> {error, invalid_datetime_format};
translate({error, <<"22019">>}) -> {error, invalid_escape_character};
translate({error, <<"2200D">>}) -> {error, invalid_escape_octet};
translate({error, <<"22025">>}) -> {error, invalid_escape_sequence};
translate({error, <<"22P06">>}) -> {error, nonstandard_use_of_escape_character};
translate({error, <<"22010">>}) -> {error, invalid_indicator_parameter_value};
translate({error, <<"22023">>}) -> {error, invalid_parameter_value};
translate({error, <<"2201B">>}) -> {error, invalid_regular_expression};
translate({error, <<"2201W">>}) -> {error, invalid_row_count_in_limit_clause};
translate({error, <<"2201X">>}) -> {error, invalid_row_count_in_result_offset_clause};
translate({error, <<"22009">>}) -> {error, invalid_time_zone_displacement_value};
translate({error, <<"2200C">>}) -> {error, invalid_use_of_escape_character};
translate({error, <<"2200G">>}) -> {error, most_specific_type_mismatch};
translate({error, <<"22004">>}) -> {error, null_value_not_allowed};
translate({error, <<"22002">>}) -> {error, null_value_no_indicator_parameter};
translate({error, <<"22003">>}) -> {error, numeric_value_out_of_range};
translate({error, <<"22026">>}) -> {error, string_data_length_mismatch};
translate({error, <<"22001">>}) -> {error, string_data_right_truncation};
translate({error, <<"22011">>}) -> {error, substring_error};
translate({error, <<"22027">>}) -> {error, trim_error};
translate({error, <<"22024">>}) -> {error, unterminated_c_string};
translate({error, <<"2200F">>}) -> {error, zero_length_character_string};
translate({error, <<"22P01">>}) -> {error, floating_point_exception};
translate({error, <<"22P02">>}) -> {error, invalid_text_representation};
translate({error, <<"22P03">>}) -> {error, invalid_binary_representation};
translate({error, <<"22P04">>}) -> {error, bad_copy_file_format};
translate({error, <<"22P05">>}) -> {error, untranslatable_character};
translate({error, <<"2200L">>}) -> {error, not_an_xml_document};
translate({error, <<"2200M">>}) -> {error, invalid_xml_document};
translate({error, <<"2200N">>}) -> {error, invalid_xml_content};
translate({error, <<"2200S">>}) -> {error, invalid_xml_comment};
translate({error, <<"2200T">>}) -> {error, invalid_xml_processing_instruction};
translate({error, <<"23000">>}) -> {error, integrity_constraint_violation};
translate({error, <<"23001">>}) -> {error, restrict_violation};
translate({error, <<"23502">>}) -> {error, not_null_violation};
translate({error, <<"23503">>}) -> {error, foreign_key_violation};
translate({error, <<"23505">>}) -> {error, unique_violation};
translate({error, <<"23514">>}) -> {error, check_violation};
translate({error, <<"23P01">>}) -> {error, exclusion_violation};
translate({error, <<"24000">>}) -> {error, invalid_cursor_state};
translate({error, <<"25000">>}) -> {error, invalid_transaction_state};
translate({error, <<"25001">>}) -> {error, active_sql_transaction};
translate({error, <<"25002">>}) -> {error, branch_transaction_already_active};
translate({error, <<"25008">>}) -> {error, held_cursor_requires_same_isolation_level};
translate({error, <<"25003">>}) -> {error, inappropriate_access_mode_for_branch_transaction};
translate({error, <<"25004">>}) -> {error, inappropriate_isolation_level_for_branch_transaction};
translate({error, <<"25005">>}) -> {error, no_active_sql_transaction_for_branch_transaction};
translate({error, <<"25006">>}) -> {error, read_only_sql_transaction};
translate({error, <<"25007">>}) -> {error, schema_and_data_statement_mixing_not_supported};
translate({error, <<"25P01">>}) -> {error, no_active_sql_transaction};
translate({error, <<"25P02">>}) -> {error, in_failed_sql_transaction};
translate({error, <<"26000">>}) -> {error, invalid_sql_statement_name};
translate({error, <<"27000">>}) -> {error, triggered_data_change_violation};
translate({error, <<"28000">>}) -> {error, invalid_authorization_specification};
translate({error, <<"28P01">>}) -> {error, invalid_password};
translate({error, <<"2B000">>}) -> {error, dependent_privilege_descriptors_still_exist};
translate({error, <<"2BP01">>}) -> {error, dependent_objects_still_exist};
translate({error, <<"2D000">>}) -> {error, invalid_transaction_termination};
translate({error, <<"2F000">>}) -> {error, sql_routine_exception};
translate({error, <<"2F005">>}) -> {error, function_executed_no_return_statement};
translate({error, <<"2F002">>}) -> {error, modifying_sql_data_not_permitted};
translate({error, <<"2F003">>}) -> {error, prohibited_sql_statement_attempted};
translate({error, <<"2F004">>}) -> {error, reading_sql_data_not_permitted};
translate({error, <<"34000">>}) -> {error, invalid_cursor_name};
translate({error, <<"38000">>}) -> {error, external_routine_exception};
translate({error, <<"38001">>}) -> {error, containing_sql_not_permitted};
translate({error, <<"38002">>}) -> {error, modifying_sql_data_not_permitted};
translate({error, <<"38003">>}) -> {error, prohibited_sql_statement_attempted};
translate({error, <<"38004">>}) -> {error, reading_sql_data_not_permitted};
translate({error, <<"39000">>}) -> {error, external_routine_invocation_exception};
translate({error, <<"39001">>}) -> {error, invalid_sqlstate_returned};
translate({error, <<"39004">>}) -> {error, null_value_not_allowed};
translate({error, <<"39P01">>}) -> {error, trigger_protocol_violated};
translate({error, <<"39P02">>}) -> {error, srf_protocol_violated};
translate({error, <<"3B000">>}) -> {error, savepoint_exception};
translate({error, <<"3B001">>}) -> {error, invalid_savepoint_specification};
translate({error, <<"3D000">>}) -> {error, invalid_catalog_name};
translate({error, <<"3F000">>}) -> {error, invalid_schema_name};
translate({error, <<"40000">>}) -> {error, transaction_rollback};
translate({error, <<"40002">>}) -> {error, transaction_integrity_constraint_violation};
translate({error, <<"40001">>}) -> {error, serialization_failure};
translate({error, <<"40003">>}) -> {error, statement_completion_unknown};
translate({error, <<"40P01">>}) -> {error, deadlock_detected};
translate({error, <<"42000">>}) -> {error, syntax_error_or_access_rule_violation};
translate({error, <<"42601">>}) -> {error, syntax_error};
translate({error, <<"42501">>}) -> {error, insufficient_privilege};
translate({error, <<"42846">>}) -> {error, cannot_coerce};
translate({error, <<"42803">>}) -> {error, grouping_error};
translate({error, <<"42P20">>}) -> {error, windowing_error};
translate({error, <<"42P19">>}) -> {error, invalid_recursion};
translate({error, <<"42830">>}) -> {error, invalid_foreign_key};
translate({error, <<"42602">>}) -> {error, invalid_name};
translate({error, <<"42622">>}) -> {error, name_too_long};
translate({error, <<"42939">>}) -> {error, reserved_name};
translate({error, <<"42804">>}) -> {error, datatype_mismatch};
translate({error, <<"42P18">>}) -> {error, indeterminate_datatype};
translate({error, <<"42P21">>}) -> {error, collation_mismatch};
translate({error, <<"42P22">>}) -> {error, indeterminate_collation};
translate({error, <<"42809">>}) -> {error, wrong_object_type};
translate({error, <<"42703">>}) -> {error, undefined_column};
translate({error, <<"42883">>}) -> {error, undefined_function};
translate({error, <<"42P01">>}) -> {error, undefined_table};
translate({error, <<"42P02">>}) -> {error, undefined_parameter};
translate({error, <<"42704">>}) -> {error, undefined_object};
translate({error, <<"42701">>}) -> {error, duplicate_column};
translate({error, <<"42P03">>}) -> {error, duplicate_cursor};
translate({error, <<"42P04">>}) -> {error, duplicate_database};
translate({error, <<"42723">>}) -> {error, duplicate_function};
translate({error, <<"42P05">>}) -> {error, duplicate_prepared_statement};
translate({error, <<"42P06">>}) -> {error, duplicate_schema};
translate({error, <<"42P07">>}) -> {error, duplicate_table};
translate({error, <<"42712">>}) -> {error, duplicate_alias};
translate({error, <<"42710">>}) -> {error, duplicate_object};
translate({error, <<"42702">>}) -> {error, ambiguous_column};
translate({error, <<"42725">>}) -> {error, ambiguous_function};
translate({error, <<"42P08">>}) -> {error, ambiguous_parameter};
translate({error, <<"42P09">>}) -> {error, ambiguous_alias};
translate({error, <<"42P10">>}) -> {error, invalid_column_reference};
translate({error, <<"42611">>}) -> {error, invalid_column_definition};
translate({error, <<"42P11">>}) -> {error, invalid_cursor_definition};
translate({error, <<"42P12">>}) -> {error, invalid_database_definition};
translate({error, <<"42P13">>}) -> {error, invalid_function_definition};
translate({error, <<"42P14">>}) -> {error, invalid_prepared_statement_definition};
translate({error, <<"42P15">>}) -> {error, invalid_schema_definition};
translate({error, <<"42P16">>}) -> {error, invalid_table_definition};
translate({error, <<"42P17">>}) -> {error, invalid_object_definition};
translate({error, <<"44000">>}) -> {error, with_check_option_violation};
translate({error, <<"53000">>}) -> {error, insufficient_resources};
translate({error, <<"53100">>}) -> {error, disk_full};
translate({error, <<"53200">>}) -> {error, out_of_memory};
translate({error, <<"53300">>}) -> {error, too_many_connections};
translate({error, <<"54000">>}) -> {error, program_limit_exceeded};
translate({error, <<"54001">>}) -> {error, statement_too_complex};
translate({error, <<"54011">>}) -> {error, too_many_columns};
translate({error, <<"54023">>}) -> {error, too_many_arguments};
translate({error, <<"55000">>}) -> {error, object_not_in_prerequisite_state};
translate({error, <<"55006">>}) -> {error, object_in_use};
translate({error, <<"55P02">>}) -> {error, cant_change_runtime_param};
translate({error, <<"55P03">>}) -> {error, lock_not_available};
translate({error, <<"57000">>}) -> {error, operator_intervention};
translate({error, <<"57014">>}) -> {error, query_canceled};
translate({error, <<"57P01">>}) -> {error, admin_shutdown};
translate({error, <<"57P02">>}) -> {error, crash_shutdown};
translate({error, <<"57P03">>}) -> {error, cannot_connect_now};
translate({error, <<"57P04">>}) -> {error, database_dropped};
translate({error, <<"58030">>}) -> {error, io_error};
translate({error, <<"58P01">>}) -> {error, undefined_file};
translate({error, <<"58P02">>}) -> {error, duplicate_file};
translate({error, <<"F0000">>}) -> {error, config_file_error};
translate({error, <<"F0001">>}) -> {error, lock_file_exists};
translate({error, <<"HV000">>}) -> {error, fdw_error};
translate({error, <<"HV005">>}) -> {error, fdw_column_name_not_found};
translate({error, <<"HV002">>}) -> {error, fdw_dynamic_parameter_value_needed};
translate({error, <<"HV010">>}) -> {error, fdw_function_sequence_error};
translate({error, <<"HV021">>}) -> {error, fdw_inconsistent_descriptor_information};
translate({error, <<"HV024">>}) -> {error, fdw_invalid_attribute_value};
translate({error, <<"HV007">>}) -> {error, fdw_invalid_column_name};
translate({error, <<"HV008">>}) -> {error, fdw_invalid_column_number};
translate({error, <<"HV004">>}) -> {error, fdw_invalid_data_type};
translate({error, <<"HV006">>}) -> {error, fdw_invalid_data_type_descriptors};
translate({error, <<"HV091">>}) -> {error, fdw_invalid_descriptor_field_identifier};
translate({error, <<"HV00B">>}) -> {error, fdw_invalid_handle};
translate({error, <<"HV00C">>}) -> {error, fdw_invalid_option_index};
translate({error, <<"HV00D">>}) -> {error, fdw_invalid_option_name};
translate({error, <<"HV090">>}) -> {error, fdw_invalid_string_length_or_buffer_length};
translate({error, <<"HV00A">>}) -> {error, fdw_invalid_string_format};
translate({error, <<"HV009">>}) -> {error, fdw_invalid_use_of_null_pointer};
translate({error, <<"HV014">>}) -> {error, fdw_too_many_handles};
translate({error, <<"HV001">>}) -> {error, fdw_out_of_memory};
translate({error, <<"HV00P">>}) -> {error, fdw_no_schemas};
translate({error, <<"HV00J">>}) -> {error, fdw_option_name_not_found};
translate({error, <<"HV00K">>}) -> {error, fdw_reply_handle};
translate({error, <<"HV00Q">>}) -> {error, fdw_schema_not_found};
translate({error, <<"HV00R">>}) -> {error, fdw_table_not_found};
translate({error, <<"HV00L">>}) -> {error, fdw_unable_to_create_execution};
translate({error, <<"HV00M">>}) -> {error, fdw_unable_to_create_reply};
translate({error, <<"HV00N">>}) -> {error, fdw_unable_to_establish_connection};
translate({error, <<"P0000">>}) -> {error, plpgsql_error};
translate({error, <<"P0001">>}) -> {error, raise_exception};
translate({error, <<"P0002">>}) -> {error, no_data_found};
translate({error, <<"P0003">>}) -> {error, too_many_rows};
translate(Error) -> Error.
