#!/bin/sh

echo "Waiting for postgres..."
while ! nc -z $POSTGRES_HOST $POSTGRES_PORT; do
  sleep 0.1
done
echo "PostgreSQL started"

echo "Starting data migration"
python3 parser/load_data.py
echo "Finished data migration"

exec "$@"