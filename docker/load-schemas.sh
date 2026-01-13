#!/bin/bash
set -e

# Helper script to manually reload database schemas into the running containers
# Note: Schemas are automatically loaded on first startup, so this is only needed
# if you've made changes to the schema files and want to reload them

echo "Loading database schemas..."

# Wait for postgres to be ready
echo "Waiting for PostgreSQL to be ready..."
until docker exec golden-axe-postgres pg_isready -U postgres; do
  sleep 1
done

echo "Loading frontend schema..."
docker exec -i golden-axe-postgres psql -U golden_axe -d fe < fe/src/schema.sql

echo "Loading backend schemas..."
docker exec -i golden-axe-postgres psql -U golden_axe -d be < be/src/sql/schema.sql
docker exec -i golden-axe-postgres psql -U golden_axe -d be < be/src/sql/indexes.sql
docker exec -i golden-axe-postgres psql -U golden_axe -d be < be/src/sql/roles.sql

echo "Schemas loaded successfully!"
