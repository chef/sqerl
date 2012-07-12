-define(ALL(Record), {rows_as_records, [Record, record_info(fields, Record)]}).

-define(FIRST(Record), {first_as_record, [Record, record_info(fields, Record)]}).

-type sqerl_error() :: {error, term()} |
                        {conflict, term()} |
                        {foreign_key, term()}.
