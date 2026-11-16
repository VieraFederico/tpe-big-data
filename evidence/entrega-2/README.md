# Evidencia de la entrega 2

Todo lo de esta carpeta sale de una sola corrida de `./infra/run.sh evidence`: borra el lake, corre el pipeline completo, el check de MapReduce, las consultas 1 y 2 y el chequeo de idempotencia, y copia acá conteos, tamaños, schemas, calidad, analítica y logs. Las rutas del lake son relativas a `DATALAKE_PATH` y los logs se copian sin tokens, nombres de bundle ni rutas con usuarios.

## Corrida

| Dato | Valor |
| :--- | :--- |
| Fecha | pendiente de corrida |
| `run_id` | pendiente de corrida (el de la segunda corrida, la del chequeo de idempotencia, que es el estado del lake que describen los archivos) |
| Destino del serving | pendiente de corrida |
| Duración | pendiente de corrida |
| Quickstart reproducido por | pendiente de corrida |

## Archivos

| Evidencia que pide la consigna | Archivo | Qué muestra |
| :--- | :--- | :--- |
| Logs | `logs/` (pendiente de corrida) | El último log de cada comando: `pipeline_*`, `mapreduce_check_*`, `evidence_queries_*` e `idempotence_*`. |
| Rutas y tamaños | `storage.csv` (pendiente de corrida) | Por dataset y zona: ruta relativa al lake, particiones, archivos y MB. |
| Conteos | `conteos.csv` (pendiente de corrida) | Conteos de cada step sacados de `_manifests/` (leídas, escritas, quarantine, duplicados, rows por mart). |
| Schemas | `schemas.json` (pendiente de corrida) | Columnas y tipos de cada dataset del lake después de la corrida. |
| Muestras de quarantine y calidad | `quality/` (pendiente de corrida) | `rule_counts.csv`, `reconciliation.csv` y `samples/` con hasta 20 rows por regla. |
| Analítica | `analytics/` (pendiente de corrida) | `anomaly_summary.csv` y `mapreduce_check.txt`. |
| Capturas de consultas | [`queries/`](queries/) | Consultas 1 y 2: CQL, parámetros, destino, tiempo y rows en `.txt` y `.csv`. |
| Idempotencia | `idempotencia.md` y `idempotencia.csv` (pendiente de corrida) | Rows de cada dataset del lake y de cada tabla de Cassandra antes y después de volver a correr todo el pipeline. |

## Serving en AstraDB del 2026-11-14

Antes de la corrida de entrega se hizo una carga en AstraDB y se corrieron las consultas 1 y 2 a mano. Esos son los archivos que hay hoy en `queries/`; la corrida de entrega los reemplaza. Resumen y cómo se eligieron los parámetros: [`docs/serving.md`](../../docs/serving.md#evidencia-de-la-entrega-2).

| Archivo | Qué muestra |
| :--- | :--- |
| [`queries/q1_daily_usage_by_range.txt`](queries/q1_daily_usage_by_range.txt) y [`.csv`](queries/q1_daily_usage_by_range.csv) | Consulta 1: uso y cost diario por service de `org_chj755nf` del 2025-08-18 al 2025-08-31. |
| [`queries/q2_top_services_14d.txt`](queries/q2_top_services_14d.txt) y [`.csv`](queries/q2_top_services_14d.csv) | Consulta 2: top 5 de services por cost de 14 días de `org_chj755nf` al 2025-08-31. |
| [`../logs/pipeline_20261114T221819Z-ad1d25.log`](../logs/pipeline_20261114T221819Z-ad1d25.log) | Pipeline completo con `serving=astra`: rows escritas y fallidas por tabla y duración de cada step. |
| [`../logs/evidence_params_20261114T223439Z.log`](../logs/evidence_params_20261114T223439Z.log) | Cálculo de los parámetros desde Gold (org elegida y su cost). |
| [`../logs/queries_20261114T224249Z.log`](../logs/queries_20261114T224249Z.log), [`../logs/queries_20261114T224343Z.log`](../logs/queries_20261114T224343Z.log) | Ejecución de las consultas 1 y 2. |
| [`../logs/check_connection_20261114T221737Z.log`](../logs/check_connection_20261114T221737Z.log) | Prueba de conexión a AstraDB antes de la carga. |

## Cómo regenerarla

Con `SERVING_TARGET` configurado en `config/.env` (y, para Astra, el keyspace creado):

```bash
./infra/run.sh evidence
```

**Borra las salidas del lake** antes de empezar (Landing queda). Cada paso deja su log en `evidence/logs/` y la corrida se corta en el primer error.
