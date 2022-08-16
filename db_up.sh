#!/bin/bash
# source .env

PG_COMPOSE_FILE=docker-compose.yaml
PG_HOST=localhost
PG_USER=postgres
PG_DB=postgres
PG_PORT=5433
SQL_FILE=create_tables.sql

docker-compose -f $PG_COMPOSE_FILE up -d
sleep 1  # 1 sec for PG to accept requests

# psql -U $PG_USER -d $PG_DB -h $PG_HOST -p $PG_PORT -f $SQL_FILE

# docker-compose -f $PG_COMPOSE_FILE down -v