-module(gobot_tests).

-include_lib("eunit/include/eunit.hrl").

exports_test() ->
    Exports = gobot_test_object1:module_info(exports),

    ExpectedExports = [
        {'#new', 0},
        {getval, 2},
        {fromlist, 1},
        {'fields', 0},
        {'#insert_fields', 0},
        {'#update_fields', 0},
        {'#statements', 0},
        {'#table_name', 0},
        {module_info, 0},
        {module_info, 1},
        {'#is', 1},
        {setvals, 2}
    ],

    [assert_export(X, Exports) || X <- ExpectedExports],
    ?assertEqual({export_length, length(ExpectedExports)}, {export_length, length(Exports)}),

    ok.

assert_export({FuncName, Arity}, Exports) ->
    Any = lists:member({FuncName, Arity}, Exports),
    ?assertEqual({true, {FuncName, Arity}}, {Any, {FuncName, Arity}}).