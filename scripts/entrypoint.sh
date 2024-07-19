#!/bin/sh
set -e

# Initialize the database
if [ "$1" = "scheduler" ] || [ "$1" = "webserver" ]; then
  poetry run airflow db init
  # Create admin user if it does not exist
  poetry run airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com || true
fi

# Start the requested service
exec poetry run airflow "$@"
