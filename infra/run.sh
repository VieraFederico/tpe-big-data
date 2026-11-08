#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"
export MSYS_NO_PATHCONV=1

COMPOSE=(docker compose -f infra/docker-compose.yml)
ENV_FILE="config/.env"
OUTPUT_DIRS=(bronze silver gold quarantine checkpoints)

usage() {
  cat <<'EOF'
Uso: ./infra/run.sh <comando> [args]

  list                     Lista los pasos del pipeline y si están implementados
  run [pasos]              Corre el pipeline (todos, o p. ej. bronze_batch,bronze_stream)
  check                    Prueba la conexión a Cassandra/AstraDB
  query <archivo.cql> ...  Ejecuta una consulta y guarda evidencia (-p clave=valor)
  test [args]              Corre los tests (pytest)
  shell                    Abre una terminal dentro del contenedor
  clean                    Borra las salidas del lake (bronze, silver, gold, quarantine, checkpoints)
  down                     Baja los contenedores (Cassandra local incluida)
  build                    Reconstruye la imagen (tras cambiar config/requirements.txt)
EOF
}

serving_target() {
  local value
  value=$(grep -E '^\s*(export\s+)?SERVING_TARGET=' "$ENV_FILE" 2>/dev/null | tail -1 | sed -E 's/.*=//; s/["'\''[:space:]]//g')
  echo "${value:-local}"
}

ensure_env() {
  if [[ ! -f "$ENV_FILE" ]]; then
    cp config/.env.example "$ENV_FILE"
    echo "Se creó $ENV_FILE desde el ejemplo; completalo si vas a usar AstraDB."
  fi
}

ensure_cassandra() {
  if [[ "$(serving_target)" == "local" ]]; then
    "${COMPOSE[@]}" --profile local up -d --wait cassandra
  fi
}

pipeline() {
  "${COMPOSE[@]}" run --rm pipeline "$@"
}

command="${1:-help}"
shift || true
ensure_env

case "$command" in
  list)
    pipeline python -m src.pipeline --list
    ;;
  run)
    steps="${1:-}"
    if [[ -z "$steps" || "$steps" == *serving* ]]; then ensure_cassandra; fi
    if [[ -n "$steps" ]]; then
      pipeline python -m src.pipeline --steps "$steps"
    else
      pipeline python -m src.pipeline
    fi
    ;;
  check)
    ensure_cassandra
    pipeline python -m src.serving.check_connection
    ;;
  query)
    [[ $# -ge 1 ]] || { echo "Falta el archivo .cql"; exit 1; }
    ensure_cassandra
    pipeline python -m src.serving.queries "$@"
    ;;
  test)
    pipeline python -m pytest tests "$@"
    ;;
  shell)
    pipeline bash
    ;;
  clean)
    for dir in "${OUTPUT_DIRS[@]}"; do rm -rf "data/datalake/$dir"; done
    echo "Salidas del lake borradas (landing intacto)."
    ;;
  down)
    "${COMPOSE[@]}" --profile local down
    ;;
  build)
    "${COMPOSE[@]}" build pipeline
    ;;
  help|-h|--help)
    usage
    ;;
  *)
    echo "Comando desconocido: $command"
    usage
    exit 1
    ;;
esac
