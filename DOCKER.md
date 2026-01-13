# Docker Setup for Golden Axe

This guide explains how to run Golden Axe using Docker and Docker Compose.

## Prerequisites

- Docker Engine 20.10 or later
- Docker Compose 2.0 or later

## Quick Start

1. **Build and start all services:**

```bash
docker-compose up --build
```

This will:
- Build a custom PostgreSQL 18 image with the `pg_golden_axe` extension
- Build the Rust application (both `be` and `fe`)
- Start PostgreSQL with databases initialized and schemas automatically loaded
- Start the frontend service on port 8001
- Start the backend service on port 8000

**Note:** The database schemas are automatically loaded when PostgreSQL starts for the first time. No manual schema loading is required!

## Services

### PostgreSQL (`postgres`)
- **Port:** 5432
- **User:** postgres / golden_axe
- **Password:** postgres / golden_axe
- **Databases:** `be`, `fe`
- **Extension:** `pg_golden_axe` (pre-installed)

### Frontend (`fe`)
- **Port:** 8001
- **Database:** fe
- **Binary:** `/app/bin/fe`

### Backend (`be`)
- **Port:** 8000
- **Database:** be
- **Binary:** `/app/bin/be`

## Configuration

Environment variables can be configured in the `docker-compose.yml` file:

**Frontend:**
```yaml
environment:
  PG_URL_FE: postgresql://golden_axe:golden_axe@postgres:5432/fe
  PORT: "8001"
  RUST_LOG: info
```

**Backend:**
```yaml
environment:
  PG_URL: postgresql://golden_axe:golden_axe@postgres:5432/be
  PG_URL_RO: postgresql://golden_axe:golden_axe@postgres:5432/be
  PG_URL_FE: postgresql://golden_axe:golden_axe@postgres:5432/fe?application_name=be
  LISTEN: "0.0.0.0:8000"
  RUST_LOG: info
```

## Common Commands

### Start services in detached mode:
```bash
docker-compose up -d
```

### View logs:
```bash
docker-compose logs -f
# Or for specific service:
docker-compose logs -f be
docker-compose logs -f fe
```

### Stop services:
```bash
docker-compose down
```

### Stop and remove volumes (will delete database data):
```bash
docker-compose down -v
```

### Rebuild specific service:
```bash
docker-compose build --no-cache postgres
docker-compose build --no-cache fe
docker-compose build --no-cache be
```

### Execute commands in containers:
```bash
# Access PostgreSQL:
docker exec -it golden-axe-postgres psql -U golden_axe -d be

# Access backend container:
docker exec -it golden-axe-be /bin/bash

# Access frontend container:
docker exec -it golden-axe-fe /bin/bash
```

### Reload schemas (if you've modified SQL files):
```bash
# Option 1: Use the helper script
./docker/load-schemas.sh

# Option 2: Start fresh (will delete all data)
docker-compose down -v
docker-compose up

# Option 3: Manual reload
docker exec -i golden-axe-postgres psql -U golden_axe -d fe < fe/src/schema.sql
docker exec -i golden-axe-postgres psql -U golden_axe -d be < be/src/sql/schema.sql
```

### Run tests:
```bash
# You may need to adjust this based on your test setup
docker-compose exec be cargo test
docker-compose exec fe cargo test
```

## Development Workflow

For development, you may want to mount your source code as a volume to enable hot-reloading:

```yaml
volumes:
  - .:/app
```

Add this to the `fe` and `be` services in `docker-compose.yml`.

## Troubleshooting

### PostgreSQL extension not found

If you see errors about `pg_golden_axe` extension not being available:

1. Ensure the PostgreSQL image was built correctly:
   ```bash
   docker-compose build --no-cache postgres
   ```

2. Verify the extension is installed:
   ```bash
   docker exec -it golden-axe-postgres psql -U postgres -c "\dx"
   ```

### Port already in use

If ports 5432, 8001, or 8000 are already in use, modify the port mappings in `docker-compose.yml`:

```yaml
ports:
  - "15432:5432"  # Change host port (PostgreSQL)
  - "18001:8001"  # Change host port (frontend)
  - "18000:8000"  # Change host port (backend)
```

### Build errors

If you encounter build errors, try:

1. Clean Docker build cache:
   ```bash
   docker builder prune -a
   ```

2. Remove all containers and rebuild:
   ```bash
   docker-compose down -v
   docker-compose build --no-cache
   docker-compose up
   ```

### Database connection issues

Verify the services can reach PostgreSQL:

```bash
docker-compose exec be ping postgres
docker-compose exec fe ping postgres
```

## Architecture

```
┌─────────────────┐
│   Frontend (fe) │
│   Port: 8001    │
└────────┬────────┘
         │
         ├──────────┐
         │          │
         ▼          ▼
┌────────────────┬─────────────────┐
│  Database: fe  │  Database: be   │
│                │                 │
│  PostgreSQL 18 with pg_golden_axe│
│  Port: 5432                      │
└────────────────┴─────────────────┘
                 ▲
                 │
         ┌───────┴────────┐
         │   Backend (be) │
         │   Port: 8000   │
         └────────────────┘
```

## Notes

- The frontend (`fe`) must start before the backend (`be`) as per the application requirements
- The `pg_golden_axe` extension is compiled during the PostgreSQL image build, which may take several minutes
- Database schemas are automatically loaded on first startup via the init script
- Database data persists in a Docker volume named `golden-axe_postgres_data`
- First build may take 10-15 minutes due to Rust compilation and PostgreSQL extension building
- If you need to reload schemas after changes, you can use `./docker/load-schemas.sh` or delete the volume with `docker-compose down -v` and restart
