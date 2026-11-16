# Evidencia de la entrega 2

Todo lo de esta carpeta sale de una sola corrida de `./infra/run.sh evidence`: borra el lake, corre el pipeline completo, el check de MapReduce, las consultas 1 y 2 y el chequeo de idempotencia, y copia acá conteos, tamaños, schemas, calidad, analítica y logs. Las rutas del lake son relativas a `DATALAKE_PATH` y los logs se copian sin tokens, nombres de bundle ni rutas con usuarios.

## Corrida

| Dato | Valor |
| :--- | :--- |
| Fecha | 2026-11-16 |
| `run_id` | `20261116T191254Z-d2bc0d` (el de la segunda corrida, la del chequeo de idempotencia, que es el estado del lake que describen los archivos). La primera corrida fue `20261116T190339Z-4c011d`. |
| Destino del serving | `astra` (keyspace `cloud_analytics`) |
| Duración | 18 minutos y medio (de 19:03:39 a 19:22:01 UTC, según los logs) |
| Quickstart reproducido por | Santiago Nartallo |

## Archivos

| Evidencia que pide la consigna | Archivo | Qué muestra |
| :--- | :--- | :--- |
| Logs | [`logs/`](logs/) | El último log de cada comando: `pipeline_*`, `mapreduce_check_*`, `evidence_queries_*` e `idempotence_*`. |
| Rutas y tamaños | [`storage.csv`](storage.csv) | Por dataset y zona: ruta relativa al lake, particiones, archivos y MB. |
| Conteos | [`conteos.csv`](conteos.csv) | Conteos de cada step sacados de `_manifests/` (leídas, escritas, quarantine, duplicados, rows por mart). |
| Schemas | [`schemas.json`](schemas.json) | Columnas y tipos de cada dataset del lake después de la corrida. |
| Muestras de quarantine y calidad | [`quality/`](quality/) | `rule_counts.csv`, `reconciliation.csv` y `samples/` con hasta 20 rows por regla. |
| Analítica | [`analytics/`](analytics/) | `anomaly_summary.csv` y `mapreduce_check.txt`. |
| Capturas de consultas | [`queries/`](queries/) | Consultas 1 y 2: CQL, parámetros, destino, tiempo y rows en `.txt` y `.csv`. |
| Idempotencia | [`idempotencia.md`](idempotencia.md) y [`idempotencia.csv`](idempotencia.csv) | Rows de cada dataset del lake y de cada tabla de Cassandra antes y después de volver a correr todo el pipeline. |

## Serving en AstraDB del 2026-11-14

Antes de la corrida de entrega se hizo una carga en AstraDB y se corrieron las consultas 1 y 2 a mano. La corrida de entrega reemplazó esas consultas en `queries/`, con los mismos parámetros y las mismas rows (solo cambian la hora y el tiempo de cada consulta). Resumen de la carga de entrega y cómo se eligieron los parámetros: [`docs/serving.md`](../../docs/serving.md#evidencia-de-la-entrega-2). Los logs de la carga del 2026-11-14 quedan en `evidence/logs/`:

| Archivo | Qué muestra |
| :--- | :--- |
| [`../logs/pipeline_20261114T221819Z-ad1d25.log`](../logs/pipeline_20261114T221819Z-ad1d25.log) | Pipeline completo con `serving=astra`: rows escritas y fallidas por tabla y duración de cada step. |
| [`../logs/evidence_params_20261114T223439Z.log`](../logs/evidence_params_20261114T223439Z.log) | Cálculo de los parámetros desde Gold (org elegida y su cost). |
| [`../logs/queries_20261114T224249Z.log`](../logs/queries_20261114T224249Z.log), [`../logs/queries_20261114T224343Z.log`](../logs/queries_20261114T224343Z.log) | Ejecución de las consultas 1 y 2. |
| [`../logs/check_connection_20261114T221737Z.log`](../logs/check_connection_20261114T221737Z.log) | Prueba de conexión a AstraDB antes de la carga. |

## Cómo regenerar esta evidencia

La corre quien tenga las credenciales de Astra, desde la raíz del repo.

Antes de empezar:

- Docker con Compose v2 levantado.
- `config/.env` con `SERVING_TARGET=astra`, `ASTRA_DB_APPLICATION_TOKEN` y `ASTRA_DB_API_ENDPOINT` (o `ASTRA_DB_SECURE_BUNDLE_PATH` con un bundle ya descargado en `secrets/`), como explica [`config/README.md`](../../config/README.md). Con token y endpoint, el bundle se descarga solo a `secrets/` en la primera conexión. Ni `config/.env` ni `secrets/` se versionan.
- El keyspace `cloud_analytics` creado en Astra ([cómo](../../docs/serving.md#crear-el-keyspace-en-astradb)).
- `./infra/run.sh check` conecta sin errores.

Después:

```bash
time ./infra/run.sh evidence
```

**Borra las salidas del lake** (`bronze/`, `silver/`, `gold/`, `quarantine/`, `checkpoints/`; Landing queda) y las vuelve a generar desde cero, y reemplaza los archivos de esta carpeta. En Astra las tablas se recargan por upsert, sin borrar nada. Cada paso deja su log en `evidence/logs/` y la corrida se corta en el primer error.

Cuánto tarda: la carga completa del 2026-11-14 contra Astra tardó unos 7 minutos (`bronze_stream` y `gold` son lo más lento), y `evidence` corre el pipeline dos veces más los conteos de idempotencia, así que hay que contar entre 15 y 25 minutos. Contra la Cassandra local es bastante menos.

Al terminar:

1. Revisar que nada de `evidence/` tenga tokens, nombres de bundle ni rutas con usuarios, con el comando de [Gobierno](../../docs/gobierno/README.md#acceso-y-secretos). Los logs originales de `evidence/logs/` no pasan por el reemplazo que se hace al copiarlos acá.
2. Actualizar la tabla [Corrida](#corrida) con el `run_id` (el de `data/datalake/_manifests/gold.json`, que es el de la segunda corrida), la duración que mostró `time` y la fecha. Si el comando no terminó con código 0, el último log de `evidence/logs/` dice en qué paso se cortó.
