-module(cook).

-export([
         '#insert_fields'/0,
         '#update_fields'/0,
         '#statements'/0,
         '#table_name'/0
        ]).

-compile({parse_transform, sqerl_gobot}).

-record(cook, {
          id,
          kitchen_id,
          name,
          auth_token,
          auth_token_bday,
          ssh_pub_key,
          first_name,
          last_name,
          email
         }).

-type custom_type() :: atom().

-export_type([{custom_type,0}]).

'#insert_fields'() ->
    [kitchen_id,
     name,
     auth_token,
     ssh_pub_key,
     first_name,
     last_name,
     email].

'#update_fields'() ->
    [name,
     auth_token,
     ssh_pub_key,
     first_name,
     last_name,
     email].

'#statements'() ->
    [default,
     {insert2,
      ["INSERT INTO cookers (kitchen_id, name) "
       "SELECT k.id, $2 FROM kitchens AS k "
       "WHERE k.name = $1 RETURNING "
       "id, kitchen_id, name, auth_token, "
       "auth_token_bday, ssh_pub_key, "
       "first_name, last_name, email"]},
     {fetch_by_name_kitchen_id,
      sqerl_rec:gen_fetch(cook, [name, kitchen_id])},
     {fetch_null_last_names,
      ["SELECT id, kitchen_id, name, auth_token, auth_token_bday, ",
       "ssh_pub_key, first_name, last_name, email FROM cookers "
       "WHERE last_name IS NULL AND kitchen_id = $1"]}].

'#table_name'() ->
    "cookers".
