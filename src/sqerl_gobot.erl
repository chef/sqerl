-module(sqerl_gobot).
%% Inspired by
%% https://raw.githubusercontent.com/uwiger/parse_trans/master/src/exprecs.erl

%% For more documentation on Abstract Forms, see
%% http://www.erlang.org/doc/apps/erts/absform.html

%% @doc Parse transform for generating record access functions and
%% sqerl_rec wrapper functions

-ifdef(typed_records).
-define(TYPED_RECORDS, true).
-else.
-define(TYPED_RECORDS, false).
-endif.

-record(state, {
    module :: atom(), %% What module did we find?
    record :: atom(), %% What record did we find?
    fields :: [{record_field, integer(), {atom, integer(), atom()}}], %% What fields are in the record
    types = [] :: [{atom(), {type, integer(), atom(), list()}}], %% What types are they?
    exports = [ %% Functions to export
        {fname(new), 0},
        {fname(get), 2},
        {fname(from_list), 1},
        {fname(info), 0},
        {fname(is), 1},
        {fname(set), 2}
    ] :: [{atom(), integer()}],
    eof :: integer(), %% What line is EOF?
    record_line :: integer(), %% What line did we find the record we care about?
    typed_record = false :: boolean(), %% Is the record typed?
    append_ast = [] :: list(), %% Append AST
    debug_output = false :: boolean(), % Debug output
    all_records_typed = ?TYPED_RECORDS :: boolean()
}).

%% Debug Macros, set below to 'true' to see output
-define(DBG(Debug, Fmt, Arg),
        case Debug of
            true -> io:format(user, Fmt ++ "~n", Arg);
            _ -> meh
        end
        ).

-export([parse_transform/2]).

parse_transform(AST, Options) ->
    Debug = proplists:is_defined(verbose, Options),
    ?DBG(Debug, "~p parse transform", [?MODULE]),

    ?DBG(Debug, "OriginalAST: ~p", [AST]),

    %% inspect_ast triggers a read only pass over the syntax tree to
    %% get some information about the module we're about to edit
    InspectedState = inspect_ast(AST, #state{debug_output=Debug}),

    PopulatedState = who_types_the_untyped(InspectedState),
    ?DBG(Debug, "TheState: ~p", [PopulatedState]),

    ?DBG(Debug, "Types: ~p", [PopulatedState#state.types]),

    %% Check if the module name equals the record name
    case PopulatedState#state.module =/= PopulatedState#state.record of
        true ->
            %% This is legit output.
            io:format(user,
                      "No record named '~p' found, ~p failed~n",
                      [PopulatedState#state.module, ?MODULE]),
            error;
        _ ->
            %% This is also legit output
            io:format(user,
                      "Record Identified: ~p~n",
                      [PopulatedState#state.record]),

            NewAST = walk_ast([], AST, PopulatedState),
            ?DBG(Debug, "NewAST: ~p", [NewAST]),
            NewAST

    end.

%% Checks if there was any record types for the record we're using. If
%% not, type them all to any()
-spec who_types_the_untyped(#state{}) -> #state{}.
who_types_the_untyped(#state{types=[]}=TheState) ->
    Types = [ {F, t_any(TheState#state.record_line)}
              || F <- massage_fields(TheState#state.fields)],
    TheState#state{types=Types};
who_types_the_untyped(TheState) ->
    TheState.

inspect_ast([], TheState) -> TheState;
inspect_ast([{eof, L}], TheState) -> TheState#state{eof=L};
inspect_ast([{attribute, _Line, export, Exports}|T], #state{exports=Es}=TheState) ->
    inspect_ast(T, TheState#state{exports = Es ++ Exports});
%% Module Directive
inspect_ast([{attribute, _L, module, Module}|T], TheState) ->
    inspect_ast(T, TheState#state{module=Module});
%% Found a record, is it THE record?
%% see how we're pattern matching for RecName and module?
inspect_ast([{attribute, Line, record, {RecName, Fields}}|T],
                          #state{module=RecName, all_records_typed=false} = TheState) ->
    inspect_ast(T, TheState#state{record = RecName, fields = Fields, record_line=Line});
inspect_ast([{attribute, Line, record, {RecName, Fields}}|Tail],
                          #state{module=RecName,
                                 all_records_typed=true,
                                 types=TypeAcc} = TheState) ->
    Type = lists:map(
             fun({typed_record_field, {record_field,_,{atom,_,A}}, T}) ->
                     {A, T};
                ({typed_record_field, {record_field,_,{atom,_,A},_}, T}) ->
                     {A, T};
                ({record_field, _, {atom,L,A}, _}) ->
                     {A, t_any(L)};
            ({record_field, _, {atom,L,A}}) ->
                     {A, t_any(L)}
             end, Fields),

    inspect_ast(Tail, TheState#state{record = RecName, fields = Fields, record_line=Line, types=lists:flatten([Type|TypeAcc]), typed_record=true});
%% This line is here to see if the record is typed
inspect_ast([{attribute, _Line, type, {{record, R}, RType,_}}|Tail],
                   #state{types=TypeAcc, record=R}=TheState) ->
    Type = lists:map(
         fun({typed_record_field, {record_field,_,{atom,_,A}}, T}) ->
                {A, T};
            ({typed_record_field, {record_field,_,{atom,_,A},_}, T}) ->
                {A, T};
            ({record_field, _, {atom,L,A}, _}) ->
                {A, t_any(L)};
            ({record_field, _, {atom,L,A}}) ->
                {A, t_any(L)}
         end, RType),
    inspect_ast(Tail, TheState#state{typed_record = true, types=lists:flatten([Type|TypeAcc])});
%% Not the droids you're looking for, move along
inspect_ast([_|T], TheState) ->
    inspect_ast(T, TheState).

%% @private
%%-spec walk_ast(Acc :: [syntaxTree()], [syntaxTree()], #state{}) -> {[syntaxTree()], #state{}}.
walk_ast(Acc, [], _TheState) ->
    lists:reverse(Acc);
%% EOF? Append the Sqerl_Rec abstractions
walk_ast(Acc, [{eof, Line}=H], TheState) ->
    ASTAddOns = generate_our_functions(Line, TheState),
    walk_ast([H|ASTAddOns] ++ Acc, [], TheState);
%% Right after the module directive add our exports and behavior directive
walk_ast(Acc, [{attribute, L, module, Module}=H|T], TheState) ->
    Exports = {attribute, L, export, TheState#state.exports},
    Behaviour = {attribute, L, behaviour, sqerl_rec},
    walk_ast([Exports|[Behaviour|[H|Acc]]], T, TheState#state{module=Module});
%% Here's the record, as good a place as any to add our stuff, but only for untyped records
walk_ast(Acc,
         [{attribute, Line, record, {Name, _Fields}}=H|T],
         #state{typed_record=TR,
                all_records_typed=ORT,
                record=Name}=TheState) when TR == false
                                            orelse ORT == true ->
    ASTAddOns = generate_types(Line, TheState),
    walk_ast(ASTAddOns ++ [H|Acc], T, TheState);
%% Do the same thing here for typed records
walk_ast(Acc, [{attribute, Line, type, {{record, Name}, _RType,_}}=H|T],
                                   #state{record=Name} = TheState) ->
    ASTAddOns = generate_types(Line, TheState),
    walk_ast(ASTAddOns ++ [H|Acc], T, TheState);
%% Swallow the original exports, we've got it covered above.
walk_ast(Acc, [{attribute, _L, export, _}|T], TheState) ->
    walk_ast(Acc, T, TheState);
walk_ast(Acc, [H|T], TheState) ->
    walk_ast([H|Acc], T, TheState).

%% Function name wrappers, in case we want to change the conventions
fname(new) -> '#new';
fname(get) -> getval;
fname(from_list) -> fromlist;
fname(set) -> setvals;
fname(info) -> fields;
fname(Atom) when is_atom(Atom) ->
  list_to_atom(
    "#" ++ atom_to_list(Atom)
  ).

generate_our_functions(L, TheState) ->
    lists:reverse(
        lists:flatten([
            f_new_(L, TheState),
            f_get_(L, TheState),
            f_fromlist_(L, TheState),
            f_info_(L, TheState),
            f_is_(L, TheState),
            f_set_(L, TheState)
        ])
    ).

%% generates the new/0 function
f_new_(L, #state{module=Module}) ->
    Fname = fname(new),
    [
    funspec(L, Fname, [],  {type, L, record, [{atom, L, Module}]}),
    {function, L, Fname, 0,
        [{clause, L, [], [],
            [{record, L, Module, []}]}]}
    ].

%% Generates the get/2 function
f_get_(L, #state{record=RecName, fields=Fields, types=Types}) ->
    Fname = fname(get),
    [
    funspec(L, Fname,
                [{[t_atom(L, A), t_record(L, RecName)], T}
                || {A, T} <- Types]
            ),
    {function, L, Fname, 2,
        [{clause, L, [{atom, L, Attr}, {var, L, 'R'}], [],
            [{record_field, L, {var, L, 'R'}, RecName, {atom, L, Attr}}]}
        || Attr <- massage_fields(Fields)]
        ++
        [{clause, L, [{var, L, 'Attr'}, {var, L, 'R'}], [],
            [bad_record_op(L, Fname, 'Attr', 'R')]}]
    }].

%% f_fromlist is an almost direct copy from exprecs.
%% This generates the 'fromlist/2' function
f_fromlist_(L, #state{record=RecName, fields=Fields}) ->
    Fname = fname(from_list),
    FldList = field_list(massage_fields(Fields)),
    TRec = t_record(L, RecName),
    [
    funspec(L, Fname, [t_list(L, [t_prop(L)])], TRec),
    {function, L, Fname, 1,
        [{clause, L, [{var, L, 'Vals'}], [],
            [{match, L, {var, L, 'AttrNames'}, FldList},
             {match, L, {var, L, 'F'},
                {'fun', L,
                    {clauses,
                        [{clause, L, [{nil, L},
                                      {var, L,'R'},
                                      {var, L,'_F1'}],
                                     [],
                                     [{var, L, 'R'}]},
                         {clause, L, [{cons, L,
                                        {tuple, L, [{var, L, 'H'},
                                                    {var, L, 'Pos'}]},
                                        {var, L, 'T'}},
                                      {var, L, 'R'},
                                      {var, L, 'F1'}],
                                     [[{call, L, {atom, L, is_list}, [{var, L, 'T'}]}]],
                                     [{'case', L, {call, L, {remote, L,
                                                                {atom,L,lists},{atom,L,keyfind}},
                                                            [{var,L,'H'},{integer,L,1},{var,L,'Vals'}]},
                                                  [{clause, L, [{atom,L,false}], [],
                                                        [{call, L, {var, L, 'F1'}, [{var, L, 'T'},
                                                                                    {var, L, 'R'},
                                                                                    {var, L, 'F1'}]}]},
                                                   {clause, L, [{tuple, L, [{var,L,'_'},{var,L,'Val'}]}],
                                                               [],
                                                               [{call, L, {var, L, 'F1'},
                                                                    [{var, L, 'T'},
                                                                     {call, L, {atom, L, 'setelement'},
                                                                        [{var, L, 'Pos'},
                                                                         {var, L, 'R'},
                                                                         {var, L, 'Val'}]},
                                                                     {var, L, 'F1'}]}]}
                                                  ]}
                                     ]}
                        ]
                    }
                }
            },
            {call, L, {var, L, 'F'}, [{var, L, 'AttrNames'},
                                      {record, L, RecName, []},
                                      {var, L, 'F'}]}
            ]}
        ]}
    ].

%% Generates the info/0 function
f_info_(L, #state{record=RecName, fields=Fields}) ->
    Fname = fname(info),
    FldList = massage_fields(Fields),
    [
    %% Create the spec
    funspec(L, Fname, [], t_list(L, [t_union(L, [t_atom(L,F) || F <- FldList])])),
    {function, L, Fname, 0,
        [{clause, L, [], [],
            [{call, L, {atom, L, record_info},
                [{atom, L, fields}, {atom, L, RecName}]}]
         }]
    }
    ].

%% Create the is/1 function
f_is_(L, #state{record=RecName}) ->
    Fname = fname(is),
    [
    funspec(L, Fname, [t_any(L)], t_boolean(L)),
    {function, L, Fname, 1, [
        {clause, L, [{var, L, 'Rec'}], [], [
            {op, L, '==', {atom, L, RecName},
                          {call,L,{atom,L,element},[{integer,L,1},
                                                    {var,L,'Rec'}]}}
            ]}
        ]
    }
    ].

%% Generates set/2
f_set_(L, #state{record=RecName, fields=Fields}) ->
    Fname = fname(set),
    FldList = massage_fields(Fields),
    TRec = t_record(L, RecName),
    [
    funspec(L, Fname, [t_list(L, [t_prop(L)]), TRec], TRec),
    {function, L, Fname, 2,
        [{clause, L, [{var, L, 'Vals'}, {var, L, 'Rec'}], [],
            [{match, L, {var, L, 'F'},
                {'fun', L,
                    {clauses,
                        [{clause, L, [{nil,L},
                                      {var,L,'R'},
                                      {var,L,'_F1'}],
                                     [],
                                     [{var, L, 'R'}]} |
                        [{clause, L,
                            [{cons, L, {tuple, L, [{atom, L, Attr},
                                                   {var,  L, 'V'}]},
                                       {var, L, 'T'}},
                             {var, L, 'R'},
                             {var, L, 'F1'}],
                            [[{call, L, {atom, L, is_list}, [{var, L, 'T'}]}]],
                            [{call, L, {var, L, 'F1'},
                                [{var,L,'T'},
                                 {record, L, {var,L,'R'}, RecName,
                                    [{record_field, L,
                                        {atom, L, Attr},
                                        {var, L, 'V'}}]},
                                 {var, L, 'F1'}]}]} || Attr <- FldList]
                         ++ [{clause, L, [{var, L, 'Vs'}, {var,L,'R'},{var,L,'_'}],
                                [],
                                [bad_record_op(L, Fname, 'Vs', 'R')]}]
                        ]
                    }
                }
            },
            {call, L, {var, L, 'F'}, [{var, L, 'Vals'},
                                      {var, L, 'Rec'},
                                      {var, L, 'F'}]}]}]}
    ].

generate_types(L, #state{record=RecName, fields=Fields, types=Types}) ->
    lists:reverse([
    %% Type one, the record!
    {attribute, L, type, {RecName, {type, L, record, [{atom, L, RecName}] }, []}},
    %% Possible attributes
    {attribute, L, type, {attr, {type, L, union, [
        {atom, L, F}
    || F <- massage_fields(Fields)]}, []}}
    , %% Properties
    {attribute, L, type, {prop, {type, L, union, [
        {type, L, tuple, [{atom,L,A},T]}
    || {A,T} <- Types]}, []}}
    ,
    {attribute, L, export_type, [{attr,0}, {RecName, 0}, {prop, 0}]}
    ]).

bad_record_op(L, Fname, Val, R) ->
    {call, L, {remote, L, {atom,L,erlang}, {atom,L,error}},
     [{atom,L,bad_record_op}, {cons, L, {atom, L, Fname},
                   {cons, L, {var, L, Val},
                {cons, L, {var, L, R},
                 {nil, L}}}}]}.

massage_fields(Fields) ->
    lists:map(
        fun({record_field,_, {atom,_,N}}) -> N;
           ({record_field,_, {atom,_,N}, _}) -> N;
           ({typed_record_field, {record_field,_,{atom,_,N}}, _}) ->
                N;
           ({typed_record_field, {record_field,_,{atom,_,N},_}, _}) ->
                N
        end, Fields).


field_list(Flds) ->
    erl_parse:abstract(
      lists:zip(Flds, lists:seq(2, length(Flds)+1))).

%% Writes function specs for us, Thanks Ulf!
funspec(L, Fname, [{H,_} | _] = Alts) ->
    Arity = length(H),
    {attribute, L, spec,
     {{Fname, Arity},
      [{type, L, 'fun', [{type, L, product, Head}, Ret]} ||
      {Head, Ret} <- Alts,
      no_empty_union(Head)]}}.

no_empty_union({type,_,union,[]}) ->
    false;
no_empty_union(T) when is_tuple(T) ->
    no_empty_union(tuple_to_list(T));
no_empty_union([H|T]) ->
    no_empty_union(H) andalso no_empty_union(T);
no_empty_union(_) ->
    true.

funspec(L, Fname, Head, Returns) ->
    Arity = length(Head),
    {attribute, L, spec,
        {{Fname, Arity},
            [{type, L, 'fun',
                [{type, L, product, Head}, Returns]}]}}.

t_prop(L) -> {type, L, prop, []}.
%t_attr(L) -> {type, L, attr, []}.
t_union(L, Alt)   -> {type, L, union, lists:usort(Alt)}.
t_any(L)          -> {type, L, any, []}.
%t_atom(L)         -> {type, L, atom, []}.
t_atom(L, A)      -> {atom, L, A}.
%t_integer(L)      -> {type, L, integer, []}.
%t_integer(L, I)   -> {integer, L, I}.
t_list(L, Es)     -> {type, L, list, Es}.
%t_fun(L, As, Res) -> {type, L, 'fun', [{type, L, product, As}, Res]}.
%t_tuple(L, Es)    -> {type, L, tuple, Es}.
t_boolean(L)     -> {type, L, boolean, []}.
t_record(L, A)   -> {type, L, record, [{atom, L, A}]}.
