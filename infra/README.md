# Infraestructura

Entorno Docker para correr el pipeline contra **Cassandra local** o **AstraDB**, con el mismo código.

| Archivo | Qué hace |
| :--- | :--- |
| `Dockerfile` | Imagen del pipeline: Python 3.11 + Java 17 (Spark) + dependencias de `config/requirements.txt`. |
| `docker-compose.yml` | Servicio `pipeline` y, con el perfil `local`, un Cassandra 4.1. |

## Configuración

```bash
cp config/.env.example config/.env
```

El destino del serving se elige con `SERVING_TARGET` en `config/.env`:

| `SERVING_TARGET` | Destino | Qué hace falta |
| :--- | :--- | :--- |
| `local` | Cassandra en Docker | Nada; el keyspace se crea solo. |
| `astra` | AstraDB | `ASTRA_DB_APPLICATION_TOKEN` y `ASTRA_DB_API_ENDPOINT`. El secure connect bundle se descarga solo a `secrets/`. El keyspace `cloud_analytics` se crea desde la UI de Astra. |

## Ejecución

Todos los comandos se corren desde la raíz del repo.

```bash
# Local: levanta Cassandra + pipeline
docker compose -f infra/docker-compose.yml --profile local up --build

# Cloud: solo el pipeline, contra AstraDB
docker compose -f infra/docker-compose.yml up --build pipeline
```

Por defecto el contenedor corre el chequeo de conexión (`python -m src.serving.check_connection`). Para correr otro módulo:

```bash
docker compose -f infra/docker-compose.yml run --rm pipeline python -m <modulo>
```

## Volúmenes

- `src/` se monta en el contenedor: los cambios de código no requieren rebuild.
- `data/` se monta en el contenedor: las salidas del lake (bronze, silver, gold...) quedan en el host.
- `secrets/` guarda el bundle de Astra; está ignorado por git.

## Limpieza

```bash
# Baja los contenedores
docker compose -f infra/docker-compose.yml --profile local down

# Baja los contenedores y borra los datos de Cassandra local
docker compose -f infra/docker-compose.yml --profile local down -v
```
