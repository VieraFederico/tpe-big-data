# Configuración

| Archivo | Qué es |
| :--- | :--- |
| `.env.example` | Ejemplo de configuración, sin secretos. `./infra/run.sh` lo copia a `config/.env` la primera vez. |
| `.env` | La configuración de cada uno. Está en `.gitignore`: acá van los secretos de Astra. |
| `requirements.txt` | Dependencias del pipeline, con versión fija. |
| `requirements-dev.txt` | Lo anterior más las herramientas de tests y lint (pytest, ruff, mypy). |

El código lee la configuración de variables de entorno ([`src/config.py`](../src/config.py)). En Docker, compose pasa `config/.env` al contenedor; fuera de Docker, cada punto de entrada carga `config/.env` sin pisar las variables que ya están definidas. En Colab no hay `.env`: el notebook define las variables y lee los secretos de Colab Secrets.

## Variables de `.env.example`

| Variable | Para qué | Default | Cuándo cambiarla |
| :--- | :--- | :--- | :--- |
| `DATALAKE_PATH` | Raíz del lake: adentro están `landing/`, `bronze/`, `silver/`, `gold/`, `quarantine/`, `checkpoints/` y `_manifests/`. | `./data/datalake` | Para usar un lake en otra carpeta o en Drive. En Docker no hace falta: compose la fija en `/app/data/datalake`. |
| `EVIDENCE_PATH` | Carpeta donde los steps dejan reportes de calidad, analítica y la evidencia de la entrega. | `evidence/` en la raíz del repo | Para guardar la evidencia en otro lado (por ejemplo, en Colab). |
| `ANOMALY_P99_MULTIPLIER` | Un día de un service es anomalía si su cost pasa este múltiplo del p99 diario del service. Tiene que ser mayor a 0. | `3` | Para recalibrar el umbral ([analitica.md](../docs/analitica.md)). |
| `SERVING_TARGET` | Destino del serving: `local` (Cassandra en Docker) o `astra`. | `local` | `astra` para publicar en AstraDB. |
| `CASSANDRA_KEYSPACE` | Keyspace de las tablas. | `cloud_analytics` | Casi nunca; en Astra tiene que existir antes (se crea desde la consola). |
| `CASSANDRA_HOST` | Host de la Cassandra local. | `localhost` | En Docker compose lo fija en `cassandra`. Solo si la Cassandra está en otra máquina. |
| `CASSANDRA_PORT` | Puerto de la Cassandra local. | `9042` | Si ese puerto está ocupado. |
| `ASTRA_DB_APPLICATION_TOKEN` | Token de aplicación de Astra (`AstraCS:...`). Secreto. | vacío | Obligatorio con `SERVING_TARGET=astra`. |
| `ASTRA_DB_API_ENDPOINT` | Endpoint de la base (`https://<db_id>-<region>.apps.astra.datastax.com`). Con el token alcanza para descargar el secure connect bundle a `secrets/`. Secreto. | vacío | Obligatorio con `astra`, salvo que se use un bundle propio. |
| `ASTRA_DB_SECURE_BUNDLE_PATH` | Ruta a un secure connect bundle ya descargado. | sin definir | Si el bundle ya está descargado y no se quiere usar el endpoint. |

## Otras variables

No están en el ejemplo porque casi nunca hace falta tocarlas.

| Variable | Para qué | Default |
| :--- | :--- | :--- |
| `CASSANDRA_WRITE_CONCURRENCY` | Requests en vuelo por partition de Spark al cargar Cassandra. | `32` |
| `LOG_DIR` | Carpeta de los logs. | `evidence/logs` |
| `LOG_LEVEL` | Nivel de log de Python. | `INFO` |
| `SPARK_DRIVER_MEMORY` | Memoria del driver de Spark. | `2g` |
| `SPARK_SHUFFLE_PARTITIONS` | `spark.sql.shuffle.partitions`. | `8` |
| `SPARK_MASTER` | Master de Spark. | `local[*]` |
| `SPARK_LOG_LEVEL` | Nivel de log de Spark. | `WARN` |
| `ENV_FILE` | Archivo `.env` a cargar fuera de Docker. | `config/.env` |
