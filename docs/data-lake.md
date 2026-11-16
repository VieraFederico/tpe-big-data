# Data lake

El lake vive en `DATALAKE_PATH` (`data/datalake/` en el repo, `/app/data/datalake` dentro de Docker). Cada zona es una carpeta y cada dataset una subcarpeta con Parquet particionado. El código de rutas y escritura está en [`src/common/lake.py`](../src/common/lake.py); las columnas de cada dataset, en el [diccionario de datos](diccionario-datos.md).

## Zonas

| Zona | Qué guarda | Quién escribe | Cómo escribe |
| :--- | :--- | :--- | :--- |
| `landing/` | Los archivos provistos tal cual (7 CSV y 120 JSONL). Versionada en git. | Nadie: el pipeline solo la lee. | — |
| `bronze/` | Las fuentes con esquema explícito, deduplicadas y con columnas técnicas (`ingest_ts`, `ingest_date`, `source_file`, `run_id`). Mismo grain que la fuente. | `bronze_batch`, `bronze_stream` | Maestros: overwrite de la partition de ese día. Eventos: append por micro-lote. |
| `silver/` | Datos limpios, conformados y enriquecidos, con `dq_flags`. | `silver` | Reemplazo completo en cada corrida (D-15). |
| `gold/` | Marts de negocio; cada row es un agregado. | `gold`, solo si pasa el gate | Reemplazo completo en cada corrida. |
| `quarantine/bronze/`, `quarantine/silver/` | Rows rechazadas por cada capa, con `_rule_id`, `_quarantine_reason`, `_raw` y `_quarantine_ts` (D-16). | `bronze_batch`, `bronze_stream`, `silver` | Como la capa que las rechaza. |
| `checkpoints/` | Estado del stream: archivos ya leídos y keys del dedupe. | `bronze_stream` (lo maneja Spark) | — |
| `_manifests/` | Un JSON por step con sus conteos; lo lee el gate. | `bronze_batch`, `silver`, `gold` | Se pisa en cada corrida. |

Las rutas siguen la forma `{zona}/{dataset}/{partition}=valor/`, por ejemplo `bronze/usage_events/event_date=2025-08-31/`. La quarantine agrega la capa: `quarantine/{capa}/{dataset}/ingest_date=valor/`.

## Formato

Todo lo que no es Landing ni manifests es Parquet con compresión snappy (el default de Spark): se leen solo las columnas que usa cada consulta y las columnas opcionales de los eventos v2 conviven con los v1 sin problemas. Los manifests son JSON chicos para leerlos sin Spark.

Antes de escribir, las rows se agrupan por la columna de partición (`REBALANCE` en batch, `repartition` en los micro-lotes del stream), así cada partition queda en pocos archivos. `evidence/entrega-2/storage.csv` (pendiente de corrida) tiene particiones, archivos y MB de cada dataset en la corrida de entrega.

## Datasets y particiones

| Dataset | Ruta | Partition | Por qué |
| :--- | :--- | :--- | :--- |
| 7 maestros (`customers_orgs`, `users`, `resources`, `billing_monthly`, `support_tickets`, `marketing_touches`, `nps_surveys`) | `bronze/<maestro>/` | `ingest_date` | Cada corrida es una foto completa; un día nuevo agrega un snapshot y repetir el mismo día lo reemplaza. |
| `usage_events` | `bronze/usage_events/` | `event_date` | Las consultas filtran por fecha de uso. |
| 6 maestros (todos menos `billing_monthly`) | `silver/<maestro>/` | ninguna | Son cientos de rows; salen del último snapshot de Bronze (D-19). |
| `billing_monthly` | `silver/billing_monthly/` | `billing_month` (`yyyy-MM`) | La facturación se consulta por mes. |
| `orgs_scd2` | `silver/orgs_scd2/` | ninguna | Una row por versión de cada org; sale de todos los snapshots. |
| `usage_events` | `silver/usage_events/` | `event_date` | Igual que en Bronze. |
| `org_daily_usage_by_service` | `gold/org_daily_usage_by_service/` | `usage_date` | Consulta 1 por rango de fechas. |
| `top_services_by_org_14d` | `gold/top_services_by_org_14d/` | `as_of_date` | Una partition por fecha de corte (D-02). |
| `cost_anomaly_mart` | `gold/cost_anomaly_mart/` | `usage_date` | Igual que el mart diario. Si no hay anomalías queda solo `_SUCCESS`. |
| 7 maestros y `usage_events` | `quarantine/bronze/<dataset>/` | `ingest_date` | Para ver qué rechazó cada corrida. Si no hay rows queda solo `_SUCCESS`. |
| `usage_events` | `quarantine/silver/usage_events/` | `ingest_date` | Los maestros no tienen reglas que manden a quarantine en Silver. |
| stream de eventos | `checkpoints/bronze_usage_events/` | — | Formato interno de Spark. |
| `bronze_batch.json`, `silver.json`, `gold.json` | `_manifests/` | — | Un archivo por step. |

No se particiona por service: 6 services en 60 días darían 360 carpetas de unas 120 rows cada una (D-12).

## Retención

Es la del informe (Tabla 8). **Hoy no se borra nada automáticamente**: la tabla dice cuánto tiempo se puede conservar cada zona, y la retención automática está en el [backlog](backlog.md) y en las decisiones abiertas (D-25). Como Landing no se borra nunca, cualquier zona se puede regenerar con `./infra/run.sh clean` y `./infra/run.sh run`.

| Zona | Retención | Por qué |
| :--- | :--- | :--- |
| `landing/` | Indefinida | Es la fuente de verdad para reprocesar. |
| `bronze/` | 30 snapshots de maestros, 13 meses de eventos | Alcanza para volver a correr el último mes; la historia de las orgs ya está en `orgs_scd2`. |
| `silver/` | 13 meses; `orgs_scd2` completa | Comparar cada mes con el mismo mes del año anterior. |
| `gold/` | 25 meses | Tendencias de dos años más el mes en curso. |
| `checkpoints/` | Mientras el stream esté activo | Si se borra, el stream vuelve a leer Landing desde el principio; `clean` lo borra junto con Bronze. |
| `quarantine/` | 90 días; las muestras de `evidence/` quedan | Tiempo para investigar y corregir. |

En la práctica Silver y Gold ya se reescriben enteros en cada corrida, así que nunca tienen más que lo que hay en Bronze.

## Convenciones de nombres

- **Datasets.** En Bronze y Silver, el nombre del archivo de Landing sin extensión (`customers_orgs`). Los derivados llevan un sufijo (`orgs_scd2`). En Gold, el nombre dice contenido y grain (`org_daily_usage_by_service`).
- **Columnas.** Minúscula y snake_case. Fechas terminan en `_date`, timestamps en `_ts`, montos en USD en `_usd`, booleanos empiezan con `is_`. Las columnas de quarantine empiezan con `_` (`_rule_id`).
- **Serving.** Keyspace `cloud_analytics`; cada tabla se llama igual que el mart del que se carga.
- **Corridas.** `run_id` = `YYYYMMDDTHHMMSSZ-xxxxxx` (UTC más 6 caracteres al azar). Queda en cada row escrita, en los manifests, en el nombre del log (`evidence/logs/pipeline_<run_id>.log`) y en las carpetas de `evidence/quality/` y `evidence/analytics/`.
- **Fechas.** Todo en UTC (`spark.sql.session.timeZone = UTC`).
