# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Golden Axe is a blockchain data indexing and querying platform that syncs blockchain data into PostgreSQL and provides a SQL query API. The system consists of three main components:

- **`be` (Backend)**: API server at api.indexsupply.net that handles blockchain data syncing and SQL queries
- **`fe` (Frontend)**: Web application at www.indexsupply.net for account management, API keys, and billing
- **`pg_golden_axe`**: PostgreSQL extension that wraps ABI decoding functionality from `be`
- **`shared`**: Common utilities shared between `be` and `fe`

## Architecture

### Database Architecture

The system uses two separate PostgreSQL databases:

1. **`be` database**: Stores blockchain data (blocks, transactions, logs) in partitioned tables by chain ID. This is the main data store that syncing processes write to and queries read from.

2. **`fe` database**: Stores application data (user accounts, API keys, plans, billing, configuration). The `be` service reads the `config` table and `account_limits` view from this database for chain configuration and rate limiting.

**Critical dependency**: `fe` must start before `be` because `be` depends on tables/views created by `fe`.

### Data Flow

1. **Sync Process** (`be/src/sync.rs`): Reads chain configurations from `fe.config` table, fetches blocks/transactions/logs from RPC endpoints, and stores them in partitioned tables in the `be` database.

2. **Query API** (`be/src/api.rs`, `be/src/query.rs`): Accepts SQL queries via HTTP, validates and rewrites them using ABI signatures, executes against the `be` database, and returns results as JSON.

3. **Account Management** (`fe/src/account.rs`, `fe/src/api_key.rs`): Manages user accounts, API keys, and rate limiting through the `fe` database.

### Key Components

- **ABI Decoding** (`be/src/abi.rs`): Parses Solidity event signatures and generates SQL for decoding event data from raw bytes
- **SQL Rewriting** (`be/src/query.rs`): Validates user SQL queries and rewrites them to safely access blockchain data
- **Blockchain Syncing** (`be/src/sync.rs`): Concurrent fetching and storing of blockchain data from RPC endpoints
- **Rate Limiting** (`be/src/gafe.rs`): Per-API-key connection and query rate limiting
- **Cursor Pagination** (`be/src/cursor.rs`): Handles pagination for large result sets

## Common Commands

### Local Development Setup

```bash
# Install Rust toolchain (if not already installed)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Install PostgreSQL extension tooling
cargo install --locked cargo-pgrx --version="0.16.1"
cargo pgrx init --pg18 pg_config

# Install the pg_golden_axe extension
cargo pgrx install -p pg_golden_axe

# Create test database and user
createuser --superuser --createdb --createrole golden_axe
createdb golden_axe_test
```

### Running Tests

```bash
# Run all tests with output visible
cargo test -- --no-capture

# Run tests for a specific package
cargo test -p be
cargo test -p fe
cargo test -p shared
cargo test -p pg_golden_axe

# Run a specific test
cargo test -p be test_name -- --no-capture
```

### Database Setup

```bash
# Create databases
createdb be
createdb fe

# Load backend schema
psql be -f be/src/sql/schema.sql
psql be -f be/src/sql/indexes.sql
psql be -f be/src/sql/roles.sql

# Frontend schema is loaded automatically by fe on startup
```

### Running the Application

```bash
# Start frontend (must start first!)
cargo run -p fe

# Start backend (starts in separate terminal)
cargo run -p be

# Run with custom environment variables
PG_URL=postgres://localhost/be LISTEN=0.0.0.0:8000 cargo run -p be
```

### Docker Commands

```bash
# Build and start all services (Postgres, fe, be)
docker-compose up --build

# Start in detached mode
docker-compose up -d

# View logs
docker-compose logs -f be
docker-compose logs -f fe

# Access PostgreSQL
docker exec -it golden-axe-postgres psql -U golden_axe -d be

# Reload database schemas after SQL changes
./docker/load-schemas.sh

# Stop and remove all data
docker-compose down -v
```

### Build and Lint

```bash
# Build all packages
cargo build

# Build in release mode
cargo build --release

# Check code without building
cargo check

# Run clippy for linting
cargo clippy -- -D warnings

# Format code
cargo fmt
```

## Blockchain Syncing Configuration

Blockchain sync behavior is controlled via the `config` table in the `fe` database. See [SYNCING.md](SYNCING.md) for comprehensive documentation on:

- Adding custom RPC endpoints
- Tuning `batch_size` and `concurrency` parameters
- Monitoring sync progress via `/status` endpoint
- Avoiding rate limits from RPC providers

Quick example of enabling a chain:

```sql
-- Connect to fe database
psql -d fe

-- Enable syncing for a chain
UPDATE config SET enabled = true, url = 'https://your-rpc-url.com' WHERE chain = 8453;
```

## Development Workflow Patterns

### Adding a New Chain

1. Insert into `fe.config` table with RPC URL and sync parameters
2. Restart `be` service to pick up the new configuration
3. Backend will automatically create partitioned tables for the new chain
4. Monitor sync progress via `/status` endpoint or database queries

### Modifying SQL Schema

1. Update schema files: `be/src/sql/schema.sql` or `fe/src/schema.sql`
2. For local development: drop and recreate databases with new schema
3. For Docker: use `./docker/load-schemas.sh` or restart with `docker-compose down -v && docker-compose up`
4. **Important**: `fe` schema is applied automatically on startup, `be` schema is not

### Working with Partitioned Tables

The `blocks`, `txs`, and `logs` tables are partitioned by `chain`. When adding a new chain, the sync process automatically creates partitions. Manual partition creation:

```sql
-- Create partitions for chain 8453 (Base)
CREATE TABLE blocks_8453 PARTITION OF blocks FOR VALUES IN (8453);
CREATE TABLE txs_8453 PARTITION OF txs FOR VALUES IN (8453);
CREATE TABLE logs_8453 PARTITION OF logs FOR VALUES IN (8453);
```

## Important Files and Locations

- `be/src/sql/schema.sql`: Backend database schema with ABI decoding functions
- `fe/src/schema.sql`: Frontend database schema with user/billing tables
- `be/src/main.rs`: Backend server initialization and routing
- `fe/src/main.rs`: Frontend server initialization and routing
- `be/src/sync.rs`: Blockchain data fetching and syncing logic
- `be/src/query.rs`: SQL query parsing, validation, and rewriting
- `be/src/abi.rs`: Event signature parsing and ABI decoding
- `docker-compose.yml`: Docker service orchestration
- `Dockerfile.postgres`: Custom Postgres image with pg_golden_axe extension

## Environment Variables

### Backend (`be`)

- `PG_URL`: Main database connection (default: `postgres://localhost/be`)
- `PG_URL_RO`: Read-only database connection (default: uses `PG_URL`)
- `PG_URL_FE`: Frontend database connection for config/rate limits
- `LISTEN`: Server listen address (default: `0.0.0.0:8000`)
- `MAX_PG_CONNS`: Maximum database connections
- `RUST_LOG`: Logging level (e.g., `info`, `debug`)

### Frontend (`fe`)

- `PG_URL_FE`: Frontend database connection (default: `postgres://localhost/fe`)
- `PORT`: Server port (default: `8001`)
- `BE_URL`: Backend API URL for proxying queries
- `FE_URL`: Frontend URL for generated links
- `STRIPE_KEY`: Stripe API key for billing
- `POSTMARK_KEY`: Postmark API key for emails
- `RUST_LOG`: Logging level

## Testing Notes

- Tests require PostgreSQL 18 with the `pg_golden_axe` extension installed
- The `golden_axe_test` database must exist
- Some tests may require `RPC_URL` environment variable for blockchain data fetching
- GitHub Actions CI workflow handles PostgreSQL setup automatically
