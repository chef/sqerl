Sqerl
=====
RDBMS abstraction layer


Configuration
-------------
Sqerl database config files contain prepared statements and optionally a list of
database error codes. The path to the configuration file should be set within the
application config using the key "db_config".

Configuration options are set as tuples containing a key and a list of values.

    {KEY, [
      VALUES...
    ]}.


### Error Codes Configuration
Sqerl can generate error tuples based on error codes return from the database If
no error codes are contained within the configuration file, or if an error is
caught that does not match a code in the configuration file, sqerl will return
errors in the format {error, MESSAGE}.

Error codes should be a tuple in the form of {CODE, ATOM}. CODE being the error
code returned from the database and ATOM being the atom to match on in your code.
Codes should be of the form returned by the database adapter. Currently mysql
returns a number and postgres return a binary string.

A complete error code block for mysql should look like so:

    {error_codes, [
        {1062, conflict},
        {1451, foreign_key},
        {1452, foreign_key}
    ]}.

Once error codes have been configured, matching errors will return with the an
atom matching the proper error code followed by an error message. For example,
using the prior configuration, if the database were to return an error code of
1451, you would get an error in the form of:

    {foreign_key, <<"Cannot delete or update a parent row: a foreign key
    constraint fails (%s)">>}.


### Prepared Statements Configuration
Sqerl queries the database via a set of prepared statements. The prepared statements
block is identified by the atom "prepared_statements". Each statement is of the
format {KEY, <<QUERY>>}.

A prepared statements block may look like the following:

    {prepared_statements, [
        {new_user,
         <<"INSERT INTO users (first_name, last_name, high_score,
           created, active) VALUES (?, ?, ?, ?, ?)">>},

        {find_user_by_lname,
         <<"SELECT id, first_name, last_name, high_score, active
           from users where last_name = ?">>}
    ]}.
