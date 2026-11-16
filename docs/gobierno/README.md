# Gobierno de datos (preliminar)

Qué controles tiene hoy el pipeline, qué metadatos deja, de dónde sale cada tabla de Cassandra, quién responde por cada zona y quién puede leer o escribir qué. Seguridad y observabilidad completas son de la entrega final ([backlog](../backlog.md)).

## Controles

| Control | Qué hace | Dónde |
| :--- | :--- | :--- |
| Esquemas explícitos | Cada fuente se lee con un `StructType` fijo; lo que no parsea va a quarantine con la línea original. | [`src/common/schemas.py`](../../src/common/schemas.py) |
| Reglas de calidad | Quarantine, corrección con flag o flag, según la gravedad. Conteo por regla y muestras en cada corrida. | [calidad.md](../calidad.md) |
| Gate de reconciliation | `gold` no escribe si los conteos de Landing, Bronze, Silver y quarantine no cierran. | [`src/gold/reconciliation.py`](../../src/gold/reconciliation.py), [`DECISIONS.md`](../../DECISIONS.md) D-01 |
| Gate de publicación | `serving` no toca Cassandra si `_manifests/gold.json` no dice `ok`. | [`src/serving/cassandra_writer.py`](../../src/serving/cassandra_writer.py), D-09 |
| Landing de solo lectura | Ningún step escribe en `landing/`; `./infra/run.sh clean` borra las otras zonas y la deja intacta. | [`infra/run.sh`](../../infra/run.sh) |
| Idempotencia | Volver a correr no duplica: checkpoint del stream, dedupe por clave, overwrite de Silver y Gold, upsert en Cassandra. `./infra/run.sh idempotence` lo comprueba con conteos antes y después. | [`src/evidence/idempotence.py`](../../src/evidence/idempotence.py) |
| Tests | Reglas, steps, gate, MapReduce, DDL y consultas, y la documentación (links y READMEs). `./infra/run.sh lint` con `mypy --strict`. | [`tests/`](../../tests/) |

## Metadatos

| Columna o archivo | Zona | Qué dice |
| :--- | :--- | :--- |
| `ingest_ts`, `source_file` | Bronze, Silver, quarantine | Cuándo entró la row y de qué archivo de Landing. |
| `ingest_date` | Bronze, quarantine | Día de la ingesta; partition de los maestros y de la quarantine. |
| `run_id` | Todas desde Bronze, y Cassandra | Corrida que escribió la row. |
| `dq_flags` | Silver | Correcciones y alertas de calidad de la row. |
| `is_cost_anomaly`, `anomaly_score` | Silver | Pico de cost por evento. |
| `valid_from`, `valid_to`, `is_current` | `silver/orgs_scd2` | Vigencia de cada versión de una org. |
| `_rule_id`, `_quarantine_reason`, `_raw`, `_quarantine_ts` | Quarantine | Regla que falló, motivo, registro original y cuándo. |
| `_manifests/*.json` | Lake | Conteos de cada step y estado de Gold (`ok` o `gate_failed`). |
| `evidence/quality/<run_id>/` | Evidencia | Conteo por regla, muestras y resultado de cada check del gate. |
| `evidence/logs/<comando>_<run_id o timestamp>.log` | Evidencia | Log de cada corrida: conteos, tamaños en disco y duración de cada step. |

Las columnas de cada dataset están en el [diccionario de datos](../diccionario-datos.md).

## Linaje

Las tres tablas de Cassandra salen de los eventos de uso; `customers_orgs` y `resources` entran como enriquecimiento en Silver.

| Tabla de Cassandra | Gold | Silver | Bronze | Landing |
| :--- | :--- | :--- | :--- | :--- |
| `org_daily_usage_by_service` | `gold/org_daily_usage_by_service` | `silver/usage_events` (+ `silver/customers_orgs`, `silver/resources`) | `bronze/usage_events` (+ `bronze/customers_orgs`, `bronze/resources`) | `usage_events_stream/*.jsonl` (+ `customers_orgs.csv`, `resources.csv`) |
| `top_services_by_org_14d` | `gold/top_services_by_org_14d`, desde `gold/org_daily_usage_by_service` | `silver/usage_events` | `bronze/usage_events` | `usage_events_stream/*.jsonl` |
| `cost_anomaly_mart` | `gold/cost_anomaly_mart`, desde `gold/org_daily_usage_by_service` | `silver/usage_events` | `bronze/usage_events` | `usage_events_stream/*.jsonl` |

Para rastrear una row de Cassandra hacia atrás: su `run_id` es la corrida de `gold`, y `_manifests/gold.json` de esa corrida tiene en `inputs` los `run_id` de Bronze y Silver que usó. Con la key (`org_id`, fecha, `service`) se llega a los eventos de `silver/usage_events`, y cada uno tiene su `source_file` en Landing. Gold no guarda `source_file` porque cada row resume eventos de varios archivos.

## Responsables

Roles de la Tabla 10 del informe. Cada rol responde por el código, las reglas y la evidencia de su zona.

| Zona o dataset | Rol responsable |
| :--- | :--- |
| `landing/` (que no se modifique) y `data/README.md` | Arquitectura y coordinación |
| `bronze/` de los 7 maestros, `quarantine/bronze/<maestro>`, `_manifests/bronze_batch.json` | Ingesta batch |
| `bronze/usage_events`, `quarantine/bronze/usage_events`, `checkpoints/` | Ingesta streaming |
| `silver/`, `quarantine/silver/`, `_manifests/silver.json`, reglas de calidad, diccionario y linaje | Calidad, Silver y gobierno |
| `gold/`, `_manifests/gold.json`, gate de reconciliation, analítica, tablas de Cassandra y consultas | Gold, serving y analítica |
| `DECISIONS.md`, diagramas, README y `evidence/entrega-2/` | Arquitectura y coordinación |

## Acceso y secretos

| Zona o recurso | Lee | Escribe |
| :--- | :--- | :--- |
| `landing/` | `bronze_batch`, `bronze_stream`, el gate de `gold` | Nadie (se versiona en git tal como lo dio la cátedra) |
| `bronze/`, `quarantine/bronze/` | `silver`, el gate de `gold`, `idempotence`, `collect` | `bronze_batch`, `bronze_stream` |
| `silver/`, `quarantine/silver/` | `gold`, `mapreduce-check`, `idempotence`, `collect` | `silver` |
| `gold/` | `serving`, `queries`, `idempotence`, `collect` | `gold` |
| `_manifests/` | El gate de `gold`, `serving`, `collect` | `bronze_batch`, `silver`, `gold` |
| Cassandra/AstraDB `cloud_analytics` | `query`, `queries`, `idempotence` | `serving` |

El lake es un directorio local, así que cualquiera con acceso a la máquina puede leerlo; los permisos de arriba son los que respeta el código, no un control del sistema operativo.

Secretos:

- Los únicos secretos son `ASTRA_DB_APPLICATION_TOKEN` y `ASTRA_DB_API_ENDPOINT`. Van en `config/.env` o, en Colab, en Colab Secrets. `config/.env` y `secrets/` (donde se descarga el secure connect bundle) están en `.gitignore`.
- El token se genera desde la consola de Astra y lo maneja solo quien corre contra Astra; no está en ningún archivo versionado ni en los logs.
- Al copiar logs a `evidence/entrega-2/logs/`, [`src/evidence/collect.py`](../../src/evidence/collect.py) reemplaza tokens, nombres de bundle y rutas con nombres de usuario. Las rutas del lake en la evidencia son relativas a `DATALAKE_PATH`.
- Los logs originales de `evidence/logs/` se versionan sin ese reemplazo, y el de una corrida que descargó el bundle tiene el nombre del archivo. Antes de commitear evidencia nueva, este comando no tiene que devolver nada:

  ```bash
  grep -rn -E "AstraCS:|secure-connect|/home/|C:\\\\Users" evidence/
  ```
- La Cassandra local no tiene autenticación: es solo para desarrollo y tests. Docker expone su puerto 9042 en la máquina, así que no conviene levantarla en una red compartida.
