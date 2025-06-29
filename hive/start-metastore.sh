#!/bin/bash
set -e

# Wait for PostgreSQL to be ready
echo "Waiting for PostgreSQL to be ready..."
until pg_isready -h hive-postgres -p 5432 -U hive; do
  echo "PostgreSQL is unavailable - retrying in 2s..."
  sleep 2
done

# Check if schema exists
echo "Checking if Hive schema needs initialization..."
if ! PGPASSWORD=$POSTGRES_PASSWORD psql -h hive-postgres -U hive -d metastore -c '\dt' | grep -q BUCKETING_COLS; then
  echo "Initializing Hive schema..."
  schematool -dbType postgres -initSchema
else
  echo "Hive schema already initialized."
fi

echo "Starting Hive Metastore..."
exec hive --service metastore
