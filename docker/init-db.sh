#!/bin/bash
set -e

# This script runs as part of PostgreSQL initialization
# It creates the databases, users, and loads the schemas

echo "Initializing Golden Axe databases..."

# Create the golden_axe user with necessary privileges
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE USER golden_axe WITH PASSWORD 'golden_axe' SUPERUSER CREATEDB CREATEROLE;
EOSQL

# Create the frontend database
echo "Creating frontend database..."
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE DATABASE fe OWNER golden_axe;
EOSQL

# Create the backend database
echo "Creating backend database..."
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE DATABASE be OWNER golden_axe;
EOSQL

# Load frontend schema
echo "Loading frontend schema..."
if [ -f /docker-entrypoint-initdb.d/sql/fe/schema.sql ]; then
    psql -v ON_ERROR_STOP=1 --username golden_axe --dbname fe < /docker-entrypoint-initdb.d/sql/fe/schema.sql
    echo "Frontend schema loaded successfully!"
else
    echo "Warning: Frontend schema file not found"
fi

# Load backend schemas
echo "Loading backend schemas..."
if [ -f /docker-entrypoint-initdb.d/sql/be/schema.sql ]; then
    psql -v ON_ERROR_STOP=1 --username golden_axe --dbname be < /docker-entrypoint-initdb.d/sql/be/schema.sql
    echo "Backend schema loaded successfully!"
else
    echo "Warning: Backend schema file not found"
fi

if [ -f /docker-entrypoint-initdb.d/sql/be/indexes.sql ]; then
    psql -v ON_ERROR_STOP=1 --username golden_axe --dbname be < /docker-entrypoint-initdb.d/sql/be/indexes.sql
    echo "Backend indexes loaded successfully!"
else
    echo "Warning: Backend indexes file not found"
fi

if [ -f /docker-entrypoint-initdb.d/sql/be/roles.sql ]; then
    psql -v ON_ERROR_STOP=1 --username golden_axe --dbname be < /docker-entrypoint-initdb.d/sql/be/roles.sql
    echo "Backend roles loaded successfully!"
else
    echo "Warning: Backend roles file not found"
fi

echo "Database initialization complete!"
