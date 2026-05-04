#!/bin/bash
# run_tests.sh — Run Go integration tests against a real PostgreSQL instance
#                with pg_dgraph pre-installed.
#
# Steps:
#   1. Build the pg_dgraph Docker image (multi-stage: Zig + pg_dgraph).
#   2. Start PostgreSQL via docker-compose.test.yml.
#   3. Wait for the database to be healthy.
#   4. Create extensions and verify installation.
#   5. Run Go tests with FAIL-on-no-DB mode.
#   6. Tear down the container (cleanup via trap).

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# ---------------------------------------------------------------------------
# Cleanup: save logs then tear down
# ---------------------------------------------------------------------------
cleanup() {
    echo -e "\n${YELLOW}Saving PostgreSQL logs...${NC}"
    TIMESTAMP=$(date +"%Y%m%d_%H%M%S")

    if [ -n "$PG_DGRAPH_LOGS_DIR" ]; then
        LOGS_DIR="$PG_DGRAPH_LOGS_DIR"
    else
        LOG_BASE="$(cd "$SCRIPT_DIR/../.." && pwd)"
        LOGS_DIR="${LOG_BASE}/logs"
    fi
    mkdir -p "$LOGS_DIR"
    LOG_FILE="${LOGS_DIR}/postgres_log_golang_${TIMESTAMP}.log"

    if docker ps -a --format '{{.Names}}' | grep -q "^pg_dgraph_examples_test$"; then
        echo -e "${YELLOW}Capturing logs from pg_dgraph_examples_test...${NC}"
        docker logs pg_dgraph_examples_test > "$LOG_FILE" 2>&1 || true
        echo -e "${GREEN}PostgreSQL logs saved to: ${LOG_FILE}${NC}"
    fi

    echo -e "\n${YELLOW}Cleaning up...${NC}"
    docker-compose -f docker-compose.test.yml down -v --remove-orphans 2>/dev/null || true
}

trap cleanup EXIT

# ---------------------------------------------------------------------------
# Step 1: Build and start the database
# ---------------------------------------------------------------------------
echo -e "${YELLOW}======================================================${NC}"
echo -e "${YELLOW}  pg_dgraph Go Integration Test Runner${NC}"
echo -e "${YELLOW}======================================================${NC}"

echo -e "\n${YELLOW}Step 1: Building and starting PostgreSQL with pg_dgraph...${NC}"
echo -e "${YELLOW}  (This compiles Zig + pg_roaringbitmap + pg_dgraph — may take a few minutes on first run)${NC}"
docker-compose -f docker-compose.test.yml build --no-cache
docker-compose -f docker-compose.test.yml up -d

# ---------------------------------------------------------------------------
# Step 2: Wait for health
# ---------------------------------------------------------------------------
echo -e "\n${YELLOW}Step 2: Waiting for PostgreSQL to be ready...${NC}"
MAX_RETRIES=60
RETRY_COUNT=0

while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
    if docker-compose -f docker-compose.test.yml exec -T pg_dgraph_examples_test \
            pg_isready -U postgres > /dev/null 2>&1; then
        echo -e "${GREEN}PostgreSQL is ready!${NC}"
        break
    fi
    RETRY_COUNT=$((RETRY_COUNT + 1))
    echo "Waiting for PostgreSQL... ($RETRY_COUNT/$MAX_RETRIES)"
    sleep 2
done

if [ $RETRY_COUNT -eq $MAX_RETRIES ]; then
    echo -e "${RED}ERROR: PostgreSQL did not become ready in time${NC}"
    docker-compose -f docker-compose.test.yml logs pg_dgraph_examples_test
    exit 1
fi

# ---------------------------------------------------------------------------
# Step 3: Create extensions
# ---------------------------------------------------------------------------
echo -e "\n${YELLOW}Step 3: Installing pg_dgraph extensions...${NC}"

docker-compose -f docker-compose.test.yml exec -T pg_dgraph_examples_test \
    psql -U postgres -c "CREATE EXTENSION IF NOT EXISTS roaringbitmap;"

docker-compose -f docker-compose.test.yml exec -T pg_dgraph_examples_test \
    psql -U postgres -c "CREATE EXTENSION IF NOT EXISTS pg_dgraph;"

docker-compose -f docker-compose.test.yml exec -T pg_dgraph_examples_test \
    psql -U postgres -c "SELECT extname, extversion FROM pg_extension WHERE extname IN ('roaringbitmap', 'pg_dgraph');"

# ---------------------------------------------------------------------------
# Step 4: Run Go tests
# ---------------------------------------------------------------------------
echo -e "\n${YELLOW}Step 4: Running Go tests...${NC}"

export TEST_DATABASE_URL="postgres://postgres:postgres@localhost:5436/postgres?sslmode=disable"
export PG_DGRAPH_TEST_FAIL_ON_NO_DB=true

if go test -v -race -timeout 10m ./...; then
    echo -e "\n${GREEN}======================================================${NC}"
    echo -e "${GREEN}  ALL TESTS PASSED!${NC}"
    echo -e "${GREEN}======================================================${NC}"
    EXIT_CODE=0
else
    echo -e "\n${RED}======================================================${NC}"
    echo -e "${RED}  TESTS FAILED!${NC}"
    echo -e "${RED}======================================================${NC}"
    EXIT_CODE=1
fi

# cleanup via trap
exit $EXIT_CODE
