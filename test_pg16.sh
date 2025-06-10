#!/bin/bash
# Test script for sqerl with PostgreSQL 16
set -e

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}Testing sqerl with PostgreSQL 16${NC}"

# Start PostgreSQL 16 container if not already running
CONTAINER_NAME="sqerl_pg16_test"
PG_PORT=5435
PG_USER="postgres"
PG_PASSWORD="postgres"
PG_DATABASE="postgres"

# Check if container exists, regardless of its state
if [ "$(docker ps -a -q -f name=$CONTAINER_NAME)" ]; then
    echo -e "${BLUE}Stopping and removing existing container...${NC}"
    docker stop $CONTAINER_NAME 2>/dev/null || true
    docker rm $CONTAINER_NAME 2>/dev/null || true
fi

echo -e "${BLUE}Starting PostgreSQL 16 container...${NC}"
docker run --name $CONTAINER_NAME -e POSTGRES_PASSWORD=$PG_PASSWORD \
    -p $PG_PORT:5432 -d postgres:16.1

# Wait for PostgreSQL to start
echo -e "${BLUE}Waiting for PostgreSQL to start...${NC}"
sleep 10

# Create test database and tables
echo -e "${BLUE}Setting up test database...${NC}"
docker cp ./ct/pgsql_create.sql $CONTAINER_NAME:/pgsql_create.sql
docker exec $CONTAINER_NAME psql -U $PG_USER -f /pgsql_create.sql

# Compile sqerl application and its dependencies if needed
echo -e "${BLUE}Compiling sqerl and dependencies...${NC}"
rebar3 compile || {
    echo -e "${RED}Failed to compile sqerl. Check rebar3 installation.${NC}"
    exit 1
}

# Create a simplified test using direct Erlang commands in a script
echo -e "${BLUE}Creating direct test script...${NC}"

cat > test_pg16_direct.erl << 'EOF'
#!/usr/bin/env escript
%%! -name sqerl_pg16_test@127.0.0.1

main(_) ->
    io:format("~nTesting sqerl with PostgreSQL 16...~n~n"),
    
    % Add all dependency paths
    add_paths("_build/default/lib"),
    
    % Configure sqerl
    Config = [
        {db_host, "localhost"},
        {db_port, 5435},
        {db_user, "itest"},
        {db_pass, ""},
        {db_name, "itest"},
        {idle_check, 10000},
        {db_timeout, 5000},
        {pool_max_size, 5},
        {pool_min_size, 2}
    ],
    application:set_env(sqerl, db_type, pgsql),
    application:set_env(sqerl, config, Config),
    
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
    
    % Verify connection to PostgreSQL
    io:format("~nTesting connection to PostgreSQL 16...~n"),
    
    % Get a connection directly from pooler
    {ok, Connection} = pooler:take_member(sqerl),
    
    % Test query
    QueryResult = sqerl_client:with_db(Connection, 
        fun(C) -> 
            epgsql:squery(C, "SELECT version()")
        end),
    pooler:return_member(sqerl, Connection, ok),
    
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
    end,
    
    % Test creating user
    io:format("Testing insert...~n"),
    {ok, Connection2} = pooler:take_member(sqerl),
    InsertResult = sqerl_client:with_db(Connection2, 
        fun(C) -> 
            epgsql:equery(C, "INSERT INTO users(first_name, last_name, high_score) VALUES ($1, $2, $3) RETURNING id", 
                         ["Test", "User", 100])
        end),
    pooler:return_member(sqerl, Connection2, ok),
    
    case InsertResult of
        {ok, 1, _, [{UserId}]} ->
            io:format("  ✓ Insert successful, user ID: ~p~n~n", [UserId]);
        InsertError ->
            io:format("  ✗ Insert failed: ~p~n~n", [InsertError]),
            halt(1)
    end,
    
    % Test selecting users
    io:format("Testing select...~n"),
    {ok, Connection3} = pooler:take_member(sqerl),
    SelectResult = sqerl_client:with_db(Connection3, 
        fun(C) -> 
            epgsql:squery(C, "SELECT * FROM users")
        end),
    pooler:return_member(sqerl, Connection3, ok),
    
    case SelectResult of
        {ok, _, Users} when is_list(Users), length(Users) > 0 ->
            io:format("  ✓ Select successful, found ~p users~n~n", [length(Users)]);
        {ok, _, []} ->
            io:format("  ✗ No users found~n~n"),
            halt(1);
        SelectError ->
            io:format("  ✗ Select failed: ~p~n~n", [SelectError]),
            halt(1)
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
EOF

# Make the script executable
chmod +x test_pg16_direct.erl

echo -e "${BLUE}Running sqerl tests against PostgreSQL 16...${NC}"
./test_pg16_direct.erl

echo -e "${GREEN}Testing completed!${NC}"