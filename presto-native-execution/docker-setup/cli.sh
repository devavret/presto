#!/bin/bash

# Helper script to access Presto CLI in different ways

set -e

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

print_usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  -h, --help     Show this help message"
    echo "  -i, --interactive  Start interactive CLI session (default)"
    echo "  -e, --execute  Execute a single query and exit"
    echo "  -c, --catalog  Specify catalog (default: tpch)"
    echo "  -s, --schema   Specify schema (default: tiny)"
    echo ""
    echo "Examples:"
    echo "  $0                                    # Interactive CLI"
    echo "  $0 -e \"SHOW TABLES\"                  # Execute single query"
    echo "  $0 -c hive -s default                # Use different catalog/schema"
    echo "  $0 -e \"SELECT * FROM tpch.tiny.nation LIMIT 5\""
}

# Default values
CATALOG="tpch"
SCHEMA="tiny"
INTERACTIVE=true
QUERY=""

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--help)
            print_usage
            exit 0
            ;;
        -i|--interactive)
            INTERACTIVE=true
            shift
            ;;
        -e|--execute)
            INTERACTIVE=false
            QUERY="$2"
            shift 2
            ;;
        -c|--catalog)
            CATALOG="$2"
            shift 2
            ;;
        -s|--schema)
            SCHEMA="$2"
            shift 2
            ;;
        *)
            echo "Unknown option: $1"
            print_usage
            exit 1
            ;;
    esac
done

# Check if containers are running
if ! docker compose ps | grep -q "presto-coordinator.*Up"; then
    echo -e "${YELLOW}Warning: Presto coordinator doesn't seem to be running.${NC}"
    echo "Start containers with: docker compose up -d"
    exit 1
fi

# Build CLI command
CLI_CMD="presto-cli --server presto-coordinator:8080 --catalog $CATALOG --schema $SCHEMA"

if [ "$INTERACTIVE" = true ]; then
    echo -e "${GREEN}Starting interactive Presto CLI...${NC}"
    echo "Catalog: $CATALOG, Schema: $SCHEMA"
    echo "Type 'quit;' or Ctrl+D to exit"
    echo ""
    docker compose exec presto-cli $CLI_CMD
else
    echo -e "${GREEN}Executing query: $QUERY${NC}"
    docker compose exec presto-cli $CLI_CMD --execute "$QUERY"
fi 