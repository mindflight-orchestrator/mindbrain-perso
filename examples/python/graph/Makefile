# Makefile for pg_dgraph Python integration tests
#
# Usage:
#   make test       - Build Docker image, start DB, run tests, clean up (recommended)
#   make test-fast  - Run tests against an already-running container
#   make start-db   - Start the PostgreSQL container only
#   make stop-db    - Stop the PostgreSQL container
#   make logs       - Stream PostgreSQL logs
#   make clean      - Remove containers and volumes

.PHONY: test test-fast start-db stop-db logs clean help

help:
	@echo "pg_dgraph Python Test Runner"
	@echo ""
	@echo "Usage:"
	@echo "  make test       - Build Docker image and run full test suite (recommended)"
	@echo "  make test-fast  - Run tests against existing container"
	@echo "  make start-db   - Start PostgreSQL container with pg_dgraph"
	@echo "  make stop-db    - Stop PostgreSQL container"
	@echo "  make logs       - Show PostgreSQL container logs"
	@echo "  make clean      - Remove all containers and volumes"

test:
	@./run_tests.sh

test-fast:
	@echo "Running tests against existing container (make start-db if not running)..."
	TEST_DATABASE_URL="postgres://postgres:postgres@localhost:5436/postgres?sslmode=disable" \
	PG_DGRAPH_TEST_FAIL_ON_NO_DB=true \
	pytest -v tests/

start-db:
	@echo "Building and starting PostgreSQL with pg_dgraph..."
	docker-compose -f ../golang/docker-compose.test.yml up -d --build
	@echo "Waiting for PostgreSQL to be ready..."
	@for i in $$(seq 1 60); do \
		if docker-compose -f ../golang/docker-compose.test.yml exec -T pg_dgraph_examples_test \
				pg_isready -U postgres > /dev/null 2>&1; then \
			echo "PostgreSQL is ready!"; \
			docker-compose -f ../golang/docker-compose.test.yml exec -T pg_dgraph_examples_test \
				psql -U postgres -c "CREATE EXTENSION IF NOT EXISTS roaringbitmap;"; \
			docker-compose -f ../golang/docker-compose.test.yml exec -T pg_dgraph_examples_test \
				psql -U postgres -c "CREATE EXTENSION IF NOT EXISTS pg_dgraph;"; \
			break; \
		fi; \
		echo "Waiting... ($$i/60)"; \
		sleep 2; \
	done

stop-db:
	docker-compose -f ../golang/docker-compose.test.yml down

logs:
	docker-compose -f ../golang/docker-compose.test.yml logs -f pg_dgraph_examples_test

clean:
	docker-compose -f ../golang/docker-compose.test.yml down -v --remove-orphans
	@echo "Cleaned up containers and volumes"
