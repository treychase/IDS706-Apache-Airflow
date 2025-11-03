#!/bin/bash
set -e

echo "🚀 Starting Airflow initialization..."

# Wait for postgres to be ready
echo "⏳ Waiting for PostgreSQL..."
while ! pg_isready -h postgres -p 5432 -U airflow > /dev/null 2>&1; do
  echo "   PostgreSQL not ready yet, waiting..."
  sleep 2
done
echo "✅ PostgreSQL is ready!"

# Initialize the database
echo "📊 Initializing Airflow database..."
airflow db init || airflow db migrate

# Create admin user
echo "👤 Creating admin user..."
airflow users create \
  --username trchase \
  --password Dannywins.7 \
  --firstname Trey \
  --lastname Chase \
  --role Admin \
  --email tc409@duke.edu 2>/dev/null || echo "   User already exists"

echo "✅ Initialization complete!"

# Start scheduler in background
echo "📅 Starting Airflow scheduler..."
airflow scheduler &

# Start webserver in foreground
echo "🌐 Starting Airflow webserver..."
exec airflow webserver