#!/usr/bin/env bash
# Despliegue en el EC2. Uso: ./deploy.sh <tag> [api|web|ingestor|all]
# Rollback: ./deploy.sh <tag-anterior> all
set -euo pipefail

TAG="${1:?falta el tag}"
SVC="${2:-all}"
cd /opt/gps

aws ecr get-login-password --region "$AWS_REGION" \
  | docker login --username AWS --password-stdin "$ECR_REGISTRY"

echo "IMAGE_TAG=$TAG" > .env.tag

if [ "$SVC" = "all" ]; then
  # El ingestor va primero y aparte: los equipos reconectan en segundos
  # y reenvian lo no confirmado, pero conviene no mezclarlo con el resto.
  docker compose pull
  docker compose up -d --no-deps ingestor
  sleep 5
  docker compose up -d --no-deps api web
else
  docker compose pull "$SVC"
  docker compose up -d --no-deps "$SVC"
fi

sleep 8
if ! curl -fsS http://localhost/api/health > /dev/null; then
  echo "HEALTHCHECK FALLO — revisar: docker compose logs --tail=100"
  exit 1
fi

docker image prune -f --filter "until=168h"
echo "Desplegado $TAG ($SVC)"
