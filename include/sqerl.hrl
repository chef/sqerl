-define(ALL(Record), {rows_as_records, [Record, record_info(fields, Record)]}).

-define(FIRST(Record), {first_as_record, [Record, record_info(fields, Record)]}).

%% This is intentionally vague since sqerl error codes are now user configurable
-type sqerl_error() :: {atom(), term()}.
