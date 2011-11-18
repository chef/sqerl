-define(ALL(Record), {rows_as_records, [Record, record_info(fields, Record)]}).

-define(FIRST(Record), {first_as_record, [Record, record_info(fields, Record)]}).
