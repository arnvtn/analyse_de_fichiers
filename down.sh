#/bin/bash

docker compose down --rmi all

docker volume rm analysedepdfs_postgres-db-volume

docker system prune -a
