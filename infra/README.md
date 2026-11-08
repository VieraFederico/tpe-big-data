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
| `astra` | AstraDB | Ver [Requisitos para AstraDB](#requisitos-para-astradb). |

### Requisitos para AstraDB

Antes de correr con `SERVING_TARGET=astra`:

1. **Crear el keyspace `cloud_analytics` a mano** desde la UI de Astra: *Databases → (la base) → Data Explorer → Create Keyspace*. Astra no permite `CREATE KEYSPACE` por CQL, así que el pipeline no puede crearlo. Si falta, la conexión falla con `El keyspace 'cloud_analytics' no existe en astra`.
2. **Generar un token** (*Settings → Tokens*) y copiarlo en `ASTRA_DB_APPLICATION_TOKEN`.
3. **Copiar el API endpoint** de la base (`https://<db_id>-<region>.apps.astra.datastax.com`) en `ASTRA_DB_API_ENDPOINT`.
4. Poner `SERVING_TARGET=astra` en `config/.env`.

El secure connect bundle no hace falta descargarlo manualmente: se descarga solo a `secrets/` en la primera conexión.

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
