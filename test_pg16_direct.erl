#!/usr/bin/env escript
%%! -name sqerl_pg16_test@127.0.0.1

main(_) ->
    io:format("~nTesting sqerl with PostgreSQL 16...~n~n"),
    
    % Add all dependency paths
    add_paths("_build/default/lib"),
    
    % Define pool name constant
    PoolName = sqerl,
    
    % Make sure our pg16_statements module is compiled and loaded
    compile:file(pg16_statements),
    code:load_file(pg16_statements),
    
    % Configure sqerl with required parameters for PostgreSQL 16
    ok = application:set_env(sqerl, db_driver_mod, sqerl_pgsql_client),
    ok = application:set_env(sqerl, db_host, "localhost"),
    ok = application:set_env(sqerl, db_port, 5435),
    ok = application:set_env(sqerl, db_user, "itest"),
    ok = application:set_env(sqerl, db_pass, ""),
    ok = application:set_env(sqerl, db_name, "itest"),
    ok = application:set_env(sqerl, idle_check, 10000),
    ok = application:set_env(sqerl, db_timeout, 5000),
    ok = application:set_env(sqerl, pooler_timeout, 5000),
    
    % Set prepared statements using the pg16_statements module
    ok = application:set_env(sqerl, prepared_statements, {pg16_statements, statements, []}),
    
    % Configure column transforms
    ok = application:set_env(sqerl, column_transforms, []),
    
    % Configure pooler explicitly with a named pool
    PoolConfig = [
        {name, PoolName},
        {max_count, 5},
        {init_count, 2},
        {start_mfa, {sqerl_client, start_link, []}}
    ],
    ok = application:set_env(pooler, pools, [PoolConfig]),
    
    % Start required applications in the correct order
    io:format("Starting applications in the correct order...~n"),
    
    % Use application:ensure_all_started for better dependency handling
    io:format("Starting crypto...~n"),
    case application:ensure_all_started(crypto) of
        {ok, StartedApps1} ->
            io:format("  ✓ Started crypto and dependencies: ~p~n", [StartedApps1]);
        {error, Reason1} ->
            io:format("  ✗ Failed to start crypto: ~p~n", [Reason1]),
            halt(1)
    end,
    
    io:format("Starting public_key...~n"),
    case application:ensure_all_started(public_key) of
        {ok, StartedApps2} ->
            io:format("  ✓ Started public_key and dependencies: ~p~n", [StartedApps2]);
        {error, Reason2} ->
            io:format("  ✗ Failed to start public_key: ~p~n", [Reason2]),
            halt(1)
    end,
    
    io:format("Starting ssl...~n"),
    case application:ensure_all_started(ssl) of
        {ok, StartedApps3} ->
            io:format("  ✓ Started ssl and dependencies: ~p~n", [StartedApps3]);
        {error, Reason3} ->
            io:format("  ✗ Failed to start ssl: ~p~n", [Reason3]),
            halt(1)
    end,
    
    io:format("Starting inets...~n"),
    case application:ensure_all_started(inets) of
        {ok, StartedApps4} ->
            io:format("  ✓ Started inets and dependencies: ~p~n", [StartedApps4]);
        {error, Reason4} ->
            io:format("  ✗ Failed to start inets: ~p~n", [Reason4]),
            halt(1)
    end,
    
    io:format("Starting pooler...~n"),
    case application:ensure_all_started(pooler) of
        {ok, StartedApps5} ->
            io:format("  ✓ Started pooler and dependencies: ~p~n", [StartedApps5]);
        {error, Reason5} ->
            io:format("  ✗ Failed to start pooler: ~p~n", [Reason5]),
            halt(1)
    end,
    
    io:format("Starting epgsql...~n"),
    case application:ensure_all_started(epgsql) of
        {ok, StartedApps6} ->
            io:format("  ✓ Started epgsql and dependencies: ~p~n", [StartedApps6]);
        {error, Reason6} ->
            io:format("  ✗ Failed to start epgsql: ~p~n", [Reason6]),
            halt(1)
    end,
    
    io:format("Starting sqerl...~n"),
    case application:ensure_all_started(sqerl) of
        {ok, StartedApps7} ->
            io:format("  ✓ Started sqerl and dependencies: ~p~n", [StartedApps7]);
        {error, Reason7} ->
            io:format("  ✗ Failed to start sqerl: ~p~n", [Reason7]),
            halt(1)
    end,
    
    % Pools are automatically started by pooler application
    % Give pooler a moment to initialize the connections
    io:format("~nWaiting for pool initialization...~n"),
    timer:sleep(1000),
    
    % Verify connection to PostgreSQL
    io:format("~nTesting connection to PostgreSQL 16...~n"),
    
    % Get a connection directly from pooler with timeout
    % Use pattern match with case to handle all possible responses
    case pooler:take_member(PoolName, 5000) of
        error_no_members ->
            io:format("  ✗ Failed to get connection from pool: no members available~n"),
            halt(1);
        {error, PoolTakeError} ->
            io:format("  ✗ Failed to get connection from pool: ~p~n", [PoolTakeError]),
            halt(1);
        Connection ->
            % Test query
            QueryResult = sqerl_client:with_db(Connection, 
                fun(C) -> 
                    epgsql:squery(C, "SELECT version()")
                end),
            pooler:return_member(PoolName, Connection, ok),
            
            % Check version result
            case QueryResult of
                {ok, _, [{Version}]} ->
                    io:format("  ✓ Connection successful~n"),
                    io:format("  PostgreSQL version: ~s~n", [Version]),
                    case binary:match(Version, <<"PostgreSQL 16">>) of
                        {_, _} -> 
                            io:format("  ✓ Using PostgreSQL 16~n~n");
                        nomatch ->
                            io:format("  ✗ Not using PostgreSQL 16~n~n"),
                            halt(1)
                    end;
                Error ->
                    io:format("  ✗ Connection failed: ~p~n~n", [Error]),
                    halt(1)
            end
    end,
    
    % Test creating user
    io:format("Testing insert...~n"),
    case pooler:take_member(PoolName, 5000) of
        error_no_members ->
            io:format("  ✗ Failed to get connection from pool: no members available~n"),
            halt(1);
        {error, PoolTakeError2} ->
            io:format("  ✗ Failed to get connection from pool: ~p~n", [PoolTakeError2]),
            halt(1);
        Connection2 ->
            InsertResult = sqerl_client:with_db(Connection2, 
                fun(C) -> 
                    epgsql:equery(C, "INSERT INTO users(first_name, last_name, high_score) VALUES ($1, $2, $3) RETURNING id", 
                                ["Test", "User", 100])
                end),
            pooler:return_member(PoolName, Connection2, ok),
            
            case InsertResult of
                {ok, 1, _, [{UserId}]} ->
                    io:format("  ✓ Insert successful, user ID: ~p~n~n", [UserId]);
                InsertError ->
                    io:format("  ✗ Insert failed: ~p~n~n", [InsertError]),
                    halt(1)
            end
    end,
    
    % Test selecting users
    io:format("Testing select...~n"),
    case pooler:take_member(PoolName, 5000) of
        error_no_members ->
            io:format("  ✗ Failed to get connection from pool: no members available~n"),
            halt(1);
        {error, PoolTakeError3} ->
            io:format("  ✗ Failed to get connection from pool: ~p~n", [PoolTakeError3]),
            halt(1);
        Connection3 ->
            SelectResult = sqerl_client:with_db(Connection3, 
                fun(C) -> 
                    epgsql:squery(C, "SELECT * FROM users")
                end),
            pooler:return_member(PoolName, Connection3, ok),
            
            case SelectResult of
                {ok, _, Users} when is_list(Users), length(Users) > 0 ->
                    io:format("  ✓ Select successful, found ~p users~n~n", [length(Users)]);
                {ok, _, []} ->
                    io:format("  ✗ No users found~n~n"),
                    halt(1);
                SelectError ->
                    io:format("  ✗ Select failed: ~p~n~n", [SelectError]),
                    halt(1)
            end
    end,
    
    io:format("~nAll tests completed successfully!~n"),
    halt(0).

% Add all dependency paths
add_paths(LibDir) ->
    case file:list_dir(LibDir) of
        {ok, Dirs} ->
            lists:foreach(fun(Dir) -> 
                Path = filename:join([LibDir, Dir, "ebin"]),
                code:add_path(Path),
                io:format("Added path: ~s~n", [Path])
            end, Dirs);
        _ -> ok
    end.
