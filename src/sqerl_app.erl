%% -*- erlang-indent-level: 4;indent-tabs-mode: nil; fill-column: 92-*-
%% ex: ts=4 sw=4 et
%% @author Kevin Smith <kevin@opscode.com>
%% @author Christopher Maier <cm@opscode.com>
%% @copyright 2012 Opscode, Inc.

-module(sqerl_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%% Short for "environment value", just provides some sugar for
%% grabbing config values
-define(EV(Key), application:get_env(sqerl, Key)).
-define(EVD(Key, Default), case application:get_env(sqerl, Key) of
                               undefined -> {ok, Default};
                               V -> V
                           end).

start(_StartType, _StartArgs) ->

    {ok, DbType} = ?EV(db_type),

    {ok, Host} = ?EV(host),
    {ok, Port} = ?EV(port),
    {ok, User} = ?EV(user),
    {ok, Password} = ?EV(pass),
    {ok, Database} = ?EV(db),
    {ok, PreparedStatements} = ?EV(prepared_statement_source),
    {ok, ColumnTransforms} = ?EV(column_transforms),

    {ok, MaxPoolSize} = ?EV(max_count),
    {ok, InitPoolSize} = ?EV(init_count),
    {ok, IdleCheck} = ?EVD(idle_check, 10000),

    PoolerConfig = [[{name, "sqerl"},
                     {max_count, MaxPoolSize},
                     {init_count, InitPoolSize},
                     {start_mfa, {sqerl_client, start_link, [DbType,
                                                             [{host, Host},
                                                              {port, Port},
                                                              {user, User},
                                                              {pass, Password},
                                                              {db, Database},
                                                              {idle_check, IdleCheck},
                                                              {prepared_statement_source, PreparedStatements},
                                                              {column_transforms, ColumnTransforms}]]}}]],
    sqerl_sup:start_link(PoolerConfig).

stop(_State) ->
    ok.
