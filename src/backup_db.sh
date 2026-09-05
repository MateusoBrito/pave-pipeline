#!/bin/bash

# Carrega o arquivo .env para garantir que POSTGRES_USER e outras variáveis estejam disponíveis
if [ -f .env ]; then
    export $(cat .env | grep -v '^#' | xargs)
fi

mkdir -p ./backups
DATA=$(date +%Y%m%d_%H%M%S)

echo "Iniciando backup do MongoDB..."
sudo docker exec -t panorama-mongodb mongodump \
  --host="localhost:27017" \
  --username="$MONGO_INITDB_ROOT_USERNAME" \
  --password="$MONGO_INITDB_ROOT_PASSWORD" \
  --authenticationDatabase="admin" \
  --archive="/backups/mongodb_backup_$DATA.gz" \
  --gzip

echo "Iniciando backup do PostgreSQL..."
sudo docker exec -t panorama-postgres pg_dumpall \
  -U "${POSTGRES_USER:-postgres}" > "./backups/postgres_$DATA.sql"

echo "Backups concluídos com sucesso na pasta ./backups!"