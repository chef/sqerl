-module(eg1).

-export([
         '#statements'/0
        ]).

'#statements'() ->
    [{test, <<"SELECT * FROM eg1">>}].
