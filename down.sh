#!/bin/bash
# source .env

PG_COMPOSE_FILE=docker-compose.yaml

# Kill all containers
docker-compose -f $PG_COMPOSE_FILE down -v
