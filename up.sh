#!/bin/bash
PG_COMPOSE_FILE=helpers/docker-compose.yaml
PG_HOST=localhost
PG_USER=postgres
PG_DB=postgres
PG_PORT=5433
SQL_FILE=helpers/tables.sql
MIGRATE_SCRIPT=helpers/migrate_data.py

# -- 1. Start PG and RabbitMQ instances
docker-compose -f $PG_COMPOSE_FILE up -d

# -- 2. Wait for PG to finish the setup
sleep 5

# -- 3. Create tables in PG and populate with data
psql -U $PG_USER -d $PG_DB -h $PG_HOST -p $PG_PORT -f $SQL_FILE

# -- 3. Populate PG tables with data
python3 $MIGRATE_SCRIPT
