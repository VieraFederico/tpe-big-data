# Cloud Provider Analytics

Trabajo Práctico Especial de 72.80 Big Data (ITBA, 2C 2026). Grupo 8.

## Qué es

Un pipeline en PySpark que toma los datos crudos de un proveedor de nube (eventos de uso, clientes, facturación, tickets) y los lleva hasta Cassandra/AstraDB para que FinOps los consulte. Los eventos entran a Bronze con Structured Streaming y los siete CSV de maestros por batch; Silver aplica las reglas de calidad (lo que no sirve va a quarantine con su motivo), Gold arma los marts después de comprobar que no se perdió ninguna row en el camino, y el step `serving` los publica en Cassandra. El diseño original está en el [informe de la entrega 1](docs/TPE%20Big%20Data%20-%20Grupo%208%202C%202026.pdf); lo que vale hoy es lo que está en este repo: [decisiones](DECISIONS.md), [documentación](docs/README.md) y [evidencia](evidence/README.md).

## Estado de la entrega 2

Checklist de la consigna (sección 9.2) y dónde se ve cada ítem. La relación completa con el código está en la [matriz de requisitos](docs/matriz-requisitos.md).

| Ítem de la consigna | Dónde se ve |
| :--- | :--- |
| Correcciones de la primera entrega | [Plan de correcciones](docs/plan-correcciones.md) |
| Arquitectura actualizada | [Diagramas](docs/diagramas/arquitectura.md) |
| Tres maestros batch en Bronze | [Data lake](docs/data-lake.md#datasets-y-particiones), `evidence/entrega-2/conteos.csv` (pendiente de corrida) |
| Streaming con esquema, watermark, dedupe, late data y checkpoint | [`src/bronze/streaming/ingest.py`](src/bronze/streaming/ingest.py), [`DECISIONS.md`](DECISIONS.md) D-11 |
| Silver para eventos y al menos un maestro | [Diccionario de datos](docs/diccionario-datos.md#silver) |
| Tres features calculadas | [`gold/org_daily_usage_by_service`](docs/diccionario-datos.md#goldorg_daily_usage_by_service) |
| Tres reglas de calidad y quarantine | [Catálogo de calidad](docs/calidad.md), `evidence/entrega-2/quality/` (pendiente de corrida) |
| Mart `org_daily_usage_by_service` en Gold | [Diccionario de datos](docs/diccionario-datos.md#gold), [MapReduce](docs/mapreduce.md) |
| Keyspace y tabla Cassandra/AstraDB poblada | [Serving](docs/serving.md#evidencia-de-la-entrega-2) |
| Dos consultas CQL con resultados | [`evidence/entrega-2/queries/`](evidence/entrega-2/queries/) |
| Analítica (detección de anomalías) | [Analítica](docs/analitica.md) |
| Idempotencia demostrada | `evidence/entrega-2/idempotencia.md` (pendiente de corrida) |
| Gobierno preliminar | [Gobierno](docs/gobierno/README.md) |
| README Quickstart, logs y evidencias | [Quickstart](#quickstart-con-docker), [índice de evidencia](evidence/entrega-2/README.md) |
| Backlog final priorizado | [Backlog](docs/backlog.md) |

## Quickstart con Docker

Hace falta Docker con Compose v2 (Docker Desktop en Mac y Windows). No hacen falta credenciales: por defecto el serving es una Cassandra local que levanta el mismo script.

```bash
git clone https://github.com/VieraFederico/tpe-big-data.git
cd tpe-big-data
./infra/run.sh run
./infra/run.sh queries
```

- `run` crea `config/.env` desde el ejemplo, construye la imagen la primera vez, levanta Cassandra y corre los cinco steps (`bronze_batch`, `bronze_stream`, `silver`, `gold`, `serving`). Log en `evidence/logs/pipeline_<run_id>.log`.
- `queries` elige una org y las fechas desde Gold y corre las consultas 1 (uso diario por rango) y 2 (top 5 de services de 14 días). Imprime las dos tablas y las guarda en `evidence/local/`, que git ignora.

En PowerShell son los mismos comandos con `.\infra\run.ps1`. Para publicar en AstraDB en vez de la Cassandra local, ver [Serving](docs/serving.md#configurar-el-destino).

## Alternativa: Google Colab

[`notebooks/colab_pipeline.ipynb`](notebooks/colab_pipeline.ipynb) corre el pipeline en Colab, sin Docker, contra AstraDB. Lee el token y el endpoint de Colab Secrets y usa el dataset del repo. Detalle en [`notebooks/README.md`](notebooks/README.md).

## Mapa del repositorio

| Carpeta | Qué tiene |
| :--- | :--- |
| [`config/`](config/README.md) | `.env.example` con todas las variables y los `requirements` de Python. |
| [`data/`](data/README.md) | El lake: `landing/` (dataset provisto, versionado y de solo lectura), las zonas que escribe el pipeline y `_manifests/`. |
| [`docs/`](docs/README.md) | Documentación técnica, diagramas e informe de la entrega 1. |
| [`evidence/`](evidence/README.md) | Logs, reportes de calidad, analítica y evidencia de cada entrega. |
| [`infra/`](infra/README.md) | Dockerfile, docker-compose y los scripts `run.sh` / `run.ps1`. |
| [`notebooks/`](notebooks/README.md) | Exploración de la entrega 1 y el notebook de Colab. |
| [`src/`](src/README.md) | Código del pipeline, un paquete por step. |
| [`tests/`](tests/README.md) | Tests con pytest. |

En la raíz: [`DECISIONS.md`](DECISIONS.md), el registro de decisiones.

## Comandos

Desde la raíz del repo. Todos corren dentro del contenedor.

| Comando | Qué hace |
| :--- | :--- |
| `./infra/run.sh list` | Lista los steps del pipeline. |
| `./infra/run.sh run [steps]` | Corre todo el pipeline o los steps indicados (`run bronze_batch,bronze_stream`). |
| `./infra/run.sh queries [carpeta]` | Consultas 1 y 2 con parámetros calculados desde Gold; evidencia en `evidence/local/` o en la carpeta indicada. |
| `./infra/run.sh query <archivo.cql> -p clave=valor` | Una consulta con parámetros propios; deja `.txt` y `.csv`. Ver [Serving](docs/serving.md#parámetros-en-la-línea-de-comandos). |
| `./infra/run.sh check` | Prueba la conexión a Cassandra o AstraDB. |
| `./infra/run.sh mapreduce-check` | Compara el mart diario hecho con MapReduce contra el de DataFrames. |
| `./infra/run.sh idempotence` | Cuenta las rows del lake y de Cassandra, vuelve a correr todo el pipeline y compara. Deja `evidence/entrega-2/idempotencia.csv` y `.md`. |
| `./infra/run.sh evidence` | Regenera toda la evidencia de la entrega en `evidence/entrega-2/`: `clean`, `run`, `mapreduce-check`, `queries`, `idempotence` y copia de conteos, tamaños, schemas, calidad, analítica y logs. **Borra el lake antes de empezar.** |
| `./infra/run.sh test [args]` | Tests (los args van a pytest). |
| `./infra/run.sh lint` | `ruff check`, `ruff format --check` y `mypy --strict`. |
| `./infra/run.sh shell` | Terminal dentro del contenedor. |
| `./infra/run.sh clean` | Borra las salidas del lake (Bronze, Silver, Gold, quarantine, checkpoints); Landing queda. |
| `./infra/run.sh down` | Baja los contenedores. |
| `./infra/run.sh build` | Reconstruye la imagen (después de cambiar `config/requirements*.txt`). |

Más detalle de la infraestructura en [`infra/README.md`](infra/README.md).

## Tests

```bash
./infra/run.sh test                    # toda la suite; los de Cassandra se saltean si no está levantada
./infra/run.sh test -m integration -v  # solo los que usan la Cassandra local
./infra/run.sh lint
```

Los tests de integración necesitan la Cassandra local levantada (`docker compose -f infra/docker-compose.yml --profile local up -d --wait cassandra`) y usan un keyspace propio, así que no tocan `cloud_analytics` ni Astra. [`tests/test_docs.py`](tests/test_docs.py) revisa la documentación: links relativos que no apuntan a nada, READMEs vacíos y las secciones de este README. Más en [`tests/README.md`](tests/README.md).

## Problemas comunes

| Síntoma | Qué pasa y qué hacer |
| :--- | :--- |
| `permission denied: ./infra/run.sh` | El script perdió el permiso de ejecución. Correr `chmod +x infra/run.sh` o usar `bash infra/run.sh <comando>`. |
| `Cannot connect to the Docker daemon` o la primera corrida tarda | Docker no está levantado, o se está construyendo la imagen (solo la primera vez). Si cambió `config/requirements*.txt`, correr `./infra/run.sh build`. |
| `El keyspace 'cloud_analytics' no existe en astra` | Astra no deja crear keyspaces por CQL. Crearlo a mano desde la consola de Astra: [cómo](docs/serving.md#crear-el-keyspace-en-astradb). |
| `ReconciliationError` en el step `gold` | El gate encontró conteos que no cierran entre Landing, Bronze y Silver. `_manifests/gold.json` queda en `gate_failed` y el detalle está en `evidence/quality/<run_id>/reconciliation.csv`. Casi siempre es un lake con restos de otra versión o de una corrida cortada: `./infra/run.sh clean` y volver a correr todo. |
| `GoldNotPublishableError` en `serving` | El último `gold` no terminó bien (o no corrió). Arreglar `gold` primero; `serving` no publica un Gold que no pasó el gate. |
| Bronze no cambia al volver a correr `bronze_stream` | Es a propósito: el checkpoint recuerda qué archivos ya leyó. Para reprocesar desde cero, `./infra/run.sh clean` (borra lake y checkpoint juntos). |
| Errores de columnas o paths después de actualizar el código | El lake se escribió con una versión anterior (por ejemplo, la quarantine antes iba en otra carpeta). `./infra/run.sh clean` y correr de nuevo; Landing no se toca. |
| `queries` termina con código 1 | Gold no tiene datos para el 2025-08-31. Correr el pipeline completo antes. |
| PowerShell no deja correr el script | `powershell -ExecutionPolicy Bypass -File .\infra\run.ps1 <comando>`. |

## Integrantes

| Integrante | Legajo |
| :--- | :---: |
| Luca Maximiliano Rossi | 63730 |
| Patrick Torlaschi | 62273 |
| Nicolás Casella | 62311 |
| Santiago Nartallo | 62208 |
| Federico Viera | 62022 |

Docente: Prof. (Ad.) Diego Mosquera.
