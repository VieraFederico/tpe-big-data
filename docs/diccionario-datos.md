# Diccionario de datos

Datasets del lake (`data/datalake/`) por zona, los manifests y las tablas del serving. Las fechas están en UTC y, al leer un dataset con Spark, la columna de partition queda al final del schema. Las rutas y particiones están en [data-lake.md](data-lake.md) y las reglas de calidad en [calidad.md](calidad.md).

| Zona | Datasets |
| :--- | :--- |
| [Bronze](#bronze) | 7 maestros y `usage_events` |
| [Silver](#silver) | 7 maestros, `orgs_scd2` y `usage_events` |
| [Quarantine](#quarantine) | `quarantine/bronze/<dataset>` y `quarantine/silver/usage_events` |
| [Gold](#gold) | `org_daily_usage_by_service`, `top_services_by_org_14d`, `cost_anomaly_mart` |
| [Manifests](#_manifestsbronze_batchjson-y-_manifestssilverjson) | `bronze_batch.json`, `silver.json`, `gold.json` |
| [Cassandra/AstraDB](#cassandraastradb) | Una tabla por mart de Gold |

## Columnas técnicas

Las agrega Bronze a todas las fuentes ([`src/common/columns.py`](../src/common/columns.py)) y siguen en las zonas siguientes según se indica en cada dataset.

| Columna | Tipo | Descripción |
| :--- | :--- | :--- |
| `ingest_ts` | timestamp | Momento de la ingesta. Todas las fuentes de una corrida de `bronze_batch` comparten el mismo valor. |
| `ingest_date` | date | Fecha de `ingest_ts`. En los maestros de Bronze y en la quarantine es la partition. |
| `source_file` | string | Archivo de Landing del que salió la row. |
| `run_id` | string | Corrida que escribió la row. En Silver y Gold es la corrida de ese step, no la de Bronze. |
| `dq_flags` | array<string> | Solo Silver: flags de calidad de la row, ordenados y sin repetidos; vacío si no hay ninguno. |

## Bronze

Lo escriben `bronze_batch` ([`src/bronze/batch/ingest.py`](../src/bronze/batch/ingest.py)) y `bronze_stream` ([`src/bronze/streaming/ingest.py`](../src/bronze/streaming/ingest.py)) con los esquemas explícitos de [`src/common/schemas.py`](../src/common/schemas.py). Mismo grain que la fuente, sin rows repetidas por clave natural. Cada maestro lleva las columnas de la fuente más `ingest_ts`, `ingest_date` (partition), `source_file` y `run_id`.

### `bronze/customers_orgs`

Una row por organización cliente. Clave: `org_id`.

| Columna | Tipo | Descripción |
| :--- | :--- | :--- |
| `org_id` | string | Organización. |
| `org_name` | string | Nombre. |
| `industry` | string | Industria. |
| `hq_region` | string | Región de la sede. |
| `plan_tier` | string | Plan contratado. |
| `is_enterprise` | boolean | Si es cliente enterprise. |
| `signup_date` | date | Alta como cliente. |
| `sales_rep` | string | Vendedor asignado. |
| `lifecycle_stage` | string | Etapa del ciclo de vida. |
| `marketing_source` | string | Canal por el que llegó. |
| `nps_score` | double | NPS de la org, en [-100, 100]. |
| `ingest_ts`, `ingest_date`, `source_file`, `run_id` | timestamp, date, string, string | Columnas técnicas. |

### `bronze/users`

Una row por usuario. Clave: `user_id`.

| Columna | Tipo | Descripción |
| :--- | :--- | :--- |
| `user_id` | string | Usuario. |
| `org_id` | string | Organización del usuario. |
| `email` | string | Email. |
| `role` | string | Rol dentro de la org. |
| `active` | boolean | Si el usuario está activo. |
| `created_at` | date | Alta del usuario. |
| `last_login` | date | Último login. |
| `ingest_ts`, `ingest_date`, `source_file`, `run_id` | timestamp, date, string, string | Columnas técnicas. |

### `bronze/resources`

Una row por recurso cloud. Clave: `resource_id`.

| Columna | Tipo | Descripción |
| :--- | :--- | :--- |
| `resource_id` | string | Recurso. |
| `org_id` | string | Organización dueña. |
| `service` | string | Service del recurso. |
| `region` | string | Región. |
| `created_at` | date | Alta del recurso. |
| `state` | string | Estado del recurso. |
| `tags_json` | string | Tags como texto JSON (`["env:prod"]`). |
| `ingest_ts`, `ingest_date`, `source_file`, `run_id` | timestamp, date, string, string | Columnas técnicas. |

### `bronze/billing_monthly`

Una row por factura mensual. Clave: `invoice_id`.

| Columna | Tipo | Descripción |
| :--- | :--- | :--- |
| `invoice_id` | string | Factura. |
| `org_id` | string | Organización facturada. |
| `month` | date | Primer día del mes facturado. |
| `subtotal` | double | Subtotal en la moneda de la factura. |
| `credits` | double | Créditos o ajustes; null en el CSV en muchas facturas. |
| `taxes` | double | Impuestos. |
| `currency` | string | Moneda de la factura. |
| `exchange_rate_to_usd` | double | Tipo de cambio a USD. |
| `ingest_ts`, `ingest_date`, `source_file`, `run_id` | timestamp, date, string, string | Columnas técnicas. |

### `bronze/support_tickets`

Una row por ticket. Clave: `ticket_id`.

| Columna | Tipo | Descripción |
| :--- | :--- | :--- |
| `ticket_id` | string | Ticket. |
| `org_id` | string | Organización que lo abrió. |
| `category` | string | Categoría. |
| `severity` | string | Severidad (`critical`, `high`, ...). |
| `created_at` | date | Apertura. |
| `resolved_at` | date | Resolución; null si sigue abierto. |
| `csat` | double | Satisfacción, en [1, 5]. |
| `sla_breached` | boolean | Si se incumplió el SLA. |
| `ingest_ts`, `ingest_date`, `source_file`, `run_id` | timestamp, date, string, string | Columnas técnicas. |

### `bronze/marketing_touches`

Una row por interacción de marketing. Clave: `touch_id`.

| Columna | Tipo | Descripción |
| :--- | :--- | :--- |
| `touch_id` | string | Interacción. |
| `org_id` | string | Organización. |
| `campaign` | string | Campaña. |
| `channel` | string | Canal. |
| `timestamp` | date | Fecha de la interacción (en el CSV viene solo la fecha). |
| `clicked` | boolean | Si hubo click. |
| `converted` | boolean | Si convirtió. |
| `ingest_ts`, `ingest_date`, `source_file`, `run_id` | timestamp, date, string, string | Columnas técnicas. |

### `bronze/nps_surveys`

Una row por encuesta. Clave: `(org_id, survey_date)`; la fuente no trae id propio.

| Columna | Tipo | Descripción |
| :--- | :--- | :--- |
| `org_id` | string | Organización. |
| `survey_date` | date | Fecha de la encuesta. |
| `nps_score` | double | NPS, en [-100, 100]. |
| `comment` | string | Comentario libre. |
| `ingest_ts`, `ingest_date`, `source_file`, `run_id` | timestamp, date, string, string | Columnas técnicas. |

### `bronze/usage_events`

Una row por evento de uso, sin `event_id` repetidos dentro del watermark. Esquema unificado v1 + v2. Partition: `event_date`.

| Columna | Tipo | Descripción |
| :--- | :--- | :--- |
| `event_id` | string | Evento. |
| `timestamp` | string | Timestamp como viene en el JSONL. |
| `org_id` | string | Organización. |
| `resource_id` | string | Recurso que generó el evento. |
| `service` | string | Service. |
| `region` | string | Región. |
| `metric` | string | Métrica que mide el evento (`requests`, `cpu_hours`, `storage_gb_hours`). |
| `value` | string | Valor de la métrica. Se lee como texto porque llega como número, como texto o null. |
| `unit` | string | Unidad de `value`; null en parte de los eventos. |
| `cost_usd_increment` | double | Cost en USD que suma el evento. |
| `schema_version` | int | 1 o 2. |
| `carbon_kg` | double | Solo v2. |
| `genai_tokens` | bigint | Solo v2 y service `genai`. |
| `event_ts` | timestamp | `timestamp` parseado. |
| `event_date` | date | Fecha de `event_ts`; partition. |
| `ingest_ts`, `ingest_date`, `source_file`, `run_id` | timestamp, date, string, string | Columnas técnicas. |

## Silver

Lo escribe el step `silver` ([`src/silver/build.py`](../src/silver/build.py)) y se reemplaza entero en cada corrida (ver `DECISIONS.md`, D-15). Las reglas que corrigen o marcan cada columna están en [calidad.md](calidad.md).

### Maestros de Silver

Uno por maestro de Bronze, desde el snapshot con el `ingest_date` más alto (D-19). Mismo grain y clave que en Bronze. Columnas: las de la fuente (con las categorías normalizadas y los valores corregidos), más `dq_flags`, `ingest_ts`, `source_file` y `run_id`. No llevan `ingest_date`. Cada uno agrega:

| Dataset | Columna | Tipo | Descripción |
| :--- | :--- | :--- | :--- |
| `silver/customers_orgs` | — | — | `nps_score` fuera de rango pasa a null. |
| `silver/users` | — | — | Sin columnas nuevas. |
| `silver/resources` | `tags` | array<string> | `tags_json` parseado; null si no es un array JSON válido o si es null. |
| `silver/billing_monthly` | `revenue_usd` | double | `(subtotal − credits + taxes) × exchange_rate_to_usd`, con `credits` null pasado a 0. |
| `silver/billing_monthly` | `billing_month` | string | `month` como `yyyy-MM`; partition. |
| `silver/support_tickets` | — | — | `csat` fuera de rango pasa a null. |
| `silver/marketing_touches` | — | — | Sin columnas nuevas. |
| `silver/nps_surveys` | — | — | `nps_score` fuera de rango pasa a null. |

### `silver/orgs_scd2`

Historia de cada organización: una row por versión. Una versión nueva empieza cuando cambia cualquier columna de negocio respecto del snapshot anterior. Sale de todos los snapshots de `bronze/customers_orgs`. Sin partition.

| Columna | Tipo | Descripción |
| :--- | :--- | :--- |
| `org_id`, `org_name`, `industry`, `hq_region`, `plan_tier`, `is_enterprise`, `signup_date`, `sales_rep`, `lifecycle_stage`, `marketing_source`, `nps_score` | igual que en `bronze/customers_orgs` | Columnas de negocio de esa versión. |
| `valid_from` | date | `ingest_date` del primer snapshot de la versión. |
| `valid_to` | date | `valid_from` de la versión siguiente; null en la actual. |
| `is_current` | boolean | True solo en la última versión. |
| `ingest_ts`, `source_file`, `run_id` | timestamp, string, string | Columnas técnicas. |

### `silver/usage_events`

Una row por evento válido (después del dedupe y de las reglas). Partition: `event_date`.

| Columna | Tipo | Descripción |
| :--- | :--- | :--- |
| `event_id`, `org_id`, `resource_id` | string | Como en Bronze. |
| `service`, `region`, `metric` | string | Como en Bronze, sin espacios y en minúscula. |
| `unit` | string | Normalizada; imputada desde `metric` si faltaba (`UNIT_IMPUTED`). |
| `event_ts` | timestamp | Momento del evento. |
| `event_date` | date | Fecha del evento; partition. |
| `schema_version` | int | 1 o 2. |
| `value_numeric` | double | `value` convertido a número; null si `value` era null (`VALUE_NULL`). |
| `requests`, `cpu_hours`, `storage_gb_hours` | double | `value_numeric` en la columna de su `metric` y 0 en las otras dos (D-18). |
| `cost_usd_increment` | double | Cost del evento; puede ser negativo chico (`COST_NEGATIVE_SMALL`). |
| `carbon_kg` | double | Null en los eventos v1; nunca se imputa 0. |
| `genai_tokens` | bigint | Null en v1 y fuera del service `genai`. |
| `org_name`, `industry`, `hq_region`, `plan_tier` | string | De `silver/customers_orgs`; null si la org no existe (`ORG_ORPHAN`). |
| `is_enterprise` | boolean | Ídem. |
| `resource_state` | string | `state` de `silver/resources`; null si el recurso no existe (`RESOURCE_ORPHAN`). |
| `is_cost_anomaly` | boolean | `cost_usd_increment > 3 × p99` del service, sobre los eventos válidos. |
| `anomaly_score` | double | `cost_usd_increment / p99`; null si el p99 del service no es positivo. |
| `dq_flags` | array<string> | Flags de la row. |
| `ingest_ts`, `source_file` | timestamp, string | Los de Bronze. |
| `run_id` | string | Corrida de `silver`. |

## Quarantine

Rows rechazadas, con las columnas de entrada de la capa que las rechazó más cuatro columnas propias. Partition: `ingest_date`.

| Columna | Tipo | Descripción |
| :--- | :--- | :--- |
| `_rule_id` | string | Primera regla que viola la row ([calidad.md](calidad.md)). |
| `_quarantine_reason` | string | La regla explicada en texto. |
| `_raw` | string | Registro original: `_corrupt_record` si no parseó, si no el JSON de todas sus columnas de entrada. En Silver es el registro de Bronze sin normalizar. |
| `_quarantine_ts` | timestamp | Cuándo se mandó a quarantine. |

| Dataset | Grain | Columnas de entrada |
| :--- | :--- | :--- |
| `quarantine/bronze/<maestro>` | Una row de un CSV que no parseó o no tiene clave. | Las de la fuente, `_corrupt_record` (string, la línea cruda si no parseó) y `ingest_ts`, `ingest_date`, `source_file`, `run_id`. |
| `quarantine/bronze/usage_events` | Un evento que no parseó, sin clave o sin timestamp válido. | Las de `bronze/usage_events` más `_corrupt_record`. |
| `quarantine/silver/usage_events` | Un evento de Bronze que viola una regla de Silver. | Las de `bronze/usage_events`, con las categorías normalizadas y el `run_id` de `silver`. |

## `_manifests/bronze_batch.json` y `_manifests/silver.json`

Los escriben `bronze_batch` y `silver` al terminar; los lee el gate de Gold. Los dos tienen `run_id`, `written_at` y `datasets`.

| Manifest | Campo | Descripción |
| :--- | :--- | :--- |
| `bronze_batch.json` | `ingest_date` | Fecha del snapshot que escribió la corrida. |
| `bronze_batch.json` | `datasets.<fuente>` | `read` (rows de Landing), `quarantine`, `written`, `exact_duplicates`, `key_conflicts` y `duplicates` (la suma de los dos anteriores). |
| `silver.json` | `datasets.<dataset>` | `bronze_rows`, `silver_rows`, `quarantine_rows` y `duplicates_dropped`; para `orgs_scd2`, solo `silver_rows`. |

## Gold

Lo escribe el step `gold` ([`src/gold/build.py`](../src/gold/build.py)) a partir de `silver/usage_events`, solo si el gate de reconciliation pasa (ver [`DECISIONS.md`](../DECISIONS.md), D-01). Los tres marts son parquet particionado por fecha y se recalculan completos en cada corrida: el overwrite borra todas las partitions viejas, así que no quedan fechas de corridas anteriores. Cada row lleva el `run_id` de la corrida que la escribió.

### `gold/org_daily_usage_by_service`

Cost y uso por organización, día y service. Grain: una row por `(org_id, usage_date, service)` con al menos un event. Partition: `usage_date`.

| Columna | Tipo | Descripción |
| :--- | :--- | :--- |
| `org_id` | string | Organización. |
| `service` | string | Service del event (`compute`, `storage`, `database`, `networking`, `analytics`, `genai`). |
| `usage_date` | date | Día de uso (`event_date` del event). |
| `daily_cost_usd` | double | Suma de `cost_usd_increment` del día, incluyendo los costs negativos chicos que Silver deja pasar. |
| `requests` | double | Suma de `requests`. |
| `cpu_hours` | double | Suma de `cpu_hours`. |
| `storage_gb_hours` | double | Suma de `storage_gb_hours`. |
| `carbon_kg` | double | Suma de `carbon_kg` de los events que lo traen; null si ningún event del grupo lo trae (events v1). |
| `genai_tokens` | bigint | Suma de `genai_tokens`, con el mismo criterio de null que `carbon_kg`. |
| `event_count` | int | Cantidad de events del grupo. |
| `cost_anomaly_events` | int | Events del grupo con `is_cost_anomaly = true` en Silver. |
| `run_id` | string | Corrida que escribió la row. |

### `gold/top_services_by_org_14d`

Ranking de services por cost de los últimos 14 días, para cada organización y cada fecha de corte. Grain: una row por `(org_id, as_of_date, service)`. Partition: `as_of_date`.

Las `as_of_date` van del primer al último `usage_date` de todo el mart diario (el mismo rango para todas las orgs). La ventana es `[as_of_date − 13, as_of_date]`, 14 días calendario. Un service aparece si tiene al menos un día con datos en la ventana; una org sin días en la ventana no tiene rows para esa fecha. Las primeras fechas del rango tienen ventanas con menos de 14 días de datos: `window_start_date` lo deja a la vista.

| Columna | Tipo | Descripción |
| :--- | :--- | :--- |
| `org_id` | string | Organización. |
| `service` | string | Service. |
| `as_of_date` | date | Fecha de corte (último día de la ventana). |
| `window_start_date` | date | Primer día de la ventana (`as_of_date − 13`). |
| `rank` | int | 1 = service con más `cost_14d_usd` de la org en esa fecha. Empates por `service` ascendente; consecutivo, sin huecos ni repetidos. |
| `cost_14d_usd` | double | Suma de `daily_cost_usd` en la ventana. |
| `requests_14d` | double | Suma de `requests` en la ventana. |
| `run_id` | string | Corrida que escribió la row. |

### `gold/cost_anomaly_mart`

Días con un cost anormalmente alto. Grain: una row por `(org_id, usage_date, service)` del mart diario que supera el umbral. Partition: `usage_date`. El método está en [analitica.md](analitica.md).

| Columna | Tipo | Descripción |
| :--- | :--- | :--- |
| `org_id` | string | Organización. |
| `service` | string | Service. |
| `usage_date` | date | Día. Se usa `usage_date` y no `date` (ver `DECISIONS.md`, D-03). |
| `daily_cost_usd` | double | Cost del día (el del mart diario). |
| `p99_usd` | double | Percentil 99 exacto de `daily_cost_usd` del service, sobre todas las orgs y días. |
| `threshold_usd` | double | `multiplier × p99_usd`. |
| `anomaly_score` | double | `daily_cost_usd / p99_usd`. |
| `robust_z` | double | `0.6745 × (daily_cost_usd − mediana) / MAD` del service; null si el MAD es 0. |
| `detection_method` | string | `p99_x<multiplier>`, sin ceros de más: `p99_x3`, `p99_x2.5`. |
| `run_id` | string | Corrida que escribió la row. |

Si en una corrida no hay ninguna anomalía, la carpeta queda solo con `_SUCCESS` y Spark no la puede leer (no tiene de dónde sacar el schema). En ese caso `_manifests/gold.json` dice 0 rows.

### Rows en la corrida completa

Corrida `20261114T183817Z-e844b8` sobre el dataset del repo:

| Mart | Rows | Partitions | Archivos |
| :--- | ---: | ---: | ---: |
| `org_daily_usage_by_service` | 11050 | 60 (2025-07-03 a 2025-08-31) | 60 |
| `top_services_by_org_14d` | 15599 | 60 (2025-07-03 a 2025-08-31) | 60 |
| `cost_anomaly_mart` | 37 | 25 | 25 |

## `_manifests/gold.json`

Lo escribe el step `gold` en cada corrida, pase o no el gate. El step `serving` lo lee para decidir si publica.

| Campo | Tipo | Descripción |
| :--- | :--- | :--- |
| `run_id` | string | Corrida. |
| `written_at` | string | Cuándo se escribió (UTC, `YYYY-MM-DDTHH:MM:SSZ`). |
| `status` | string | `ok` si el gate pasó y se escribieron los marts; `gate_failed` si no. |
| `inputs` | object | Solo con `ok`: `bronze_batch_run_id` y `silver_run_id`, los `run_id` de `_manifests/bronze_batch.json` y `_manifests/silver.json` que se usaron. |
| `marts` | object | Rows escritas por mart (`org_daily_usage_by_service`, `top_services_by_org_14d`, `cost_anomaly_mart`). Vacío con `gate_failed`. |
| `failed_checks` | list | Solo con `gate_failed`: los `check_id` que no cerraron, sin repetir. El detalle (dataset, esperado y real) está en el log y en `reconciliation.csv`. |

Ejemplo de la corrida completa:

```json
{
  "run_id": "20261114T183817Z-e844b8",
  "written_at": "2026-11-14T18:38:42Z",
  "status": "ok",
  "inputs": {
    "bronze_batch_run_id": "20261114T183817Z-e844b8",
    "silver_run_id": "20261114T183817Z-e844b8"
  },
  "marts": {
    "org_daily_usage_by_service": 11050,
    "top_services_by_org_14d": 15599,
    "cost_anomaly_mart": 37
  }
}
```

## Cassandra/AstraDB

Tablas del keyspace `cloud_analytics`, una por mart de Gold, con el mismo nombre y las mismas columnas (incluido `run_id`). Las crea y las carga el step `serving` ([`src/serving/cassandra_writer.py`](../src/serving/cassandra_writer.py)) desde `gold/<mart>`, solo si `_manifests/gold.json` dice `ok`. Los DDL están en [`src/serving/cql/schema/`](../src/serving/cql/schema/) y las consultas en [`src/serving/cql/queries/`](../src/serving/cql/queries/).

Cada columna significa lo mismo que en su mart (ver arriba); acá van el tipo en CQL y el rol en la primary key. Los tipos se mapean así: `string → text`, `date → date`, `double → double`, `bigint → bigint`, `int → int`. Por qué esas keys y cómo se carga: [serving.md](serving.md) y [`DECISIONS.md`](../DECISIONS.md), D-06 a D-09. Las rows de cada tabla son las del mart (`marts` en `_manifests/gold.json`).

### `org_daily_usage_by_service`

- Primary key: `((org_id), usage_date, service)`.
- Clustering order: `usage_date DESC, service ASC`.
- Responde la consulta 1, uso y cost diario por service de una org en un rango de fechas: [`q1_daily_usage_by_range.cql`](../src/serving/cql/queries/q1_daily_usage_by_range.cql) (`org_id`, `from_date`, `to_date`). Conteo por org: [`count_org_daily_usage_by_service.cql`](../src/serving/cql/queries/count_org_daily_usage_by_service.cql) (`org_id`).

| Columna | Tipo | En la primary key |
| :--- | :--- | :--- |
| `org_id` | text | Partition key. |
| `usage_date` | date | Clustering, descendente. |
| `service` | text | Clustering, ascendente. |
| `daily_cost_usd` | double | |
| `requests` | double | |
| `cpu_hours` | double | |
| `storage_gb_hours` | double | |
| `carbon_kg` | double | |
| `genai_tokens` | bigint | |
| `event_count` | int | |
| `cost_anomaly_events` | int | |
| `run_id` | text | |

### `top_services_by_org_14d`

- Primary key: `((org_id, as_of_date), rank)`.
- Clustering order: `rank ASC`.
- Responde la consulta 2, top-N de services por cost de los últimos 14 días de una org a una fecha de corte: [`q2_top_services_14d.cql`](../src/serving/cql/queries/q2_top_services_14d.cql) (`org_id`, `as_of_date`, `n` entero). Conteo por org y fecha: [`count_top_services_by_org_14d.cql`](../src/serving/cql/queries/count_top_services_by_org_14d.cql) (`org_id`, `as_of_date`).

| Columna | Tipo | En la primary key |
| :--- | :--- | :--- |
| `org_id` | text | Partition key. |
| `as_of_date` | date | Partition key. |
| `rank` | int | Clustering, ascendente. |
| `service` | text | |
| `window_start_date` | date | |
| `cost_14d_usd` | double | |
| `requests_14d` | double | |
| `run_id` | text | |

### `cost_anomaly_mart`

- Primary key: `((org_id), usage_date, service)`.
- Clustering order: `usage_date DESC, service ASC`.
- Responde las anomalías de una org en un rango de fechas: [`q_cost_anomalies_by_org.cql`](../src/serving/cql/queries/q_cost_anomalies_by_org.cql) (`org_id`, `from_date`, `to_date`). Conteo por org: [`count_cost_anomaly_mart.cql`](../src/serving/cql/queries/count_cost_anomaly_mart.cql) (`org_id`).

| Columna | Tipo | En la primary key |
| :--- | :--- | :--- |
| `org_id` | text | Partition key. |
| `usage_date` | date | Clustering, descendente. |
| `service` | text | Clustering, ascendente. |
| `daily_cost_usd` | double | |
| `p99_usd` | double | |
| `threshold_usd` | double | |
| `anomaly_score` | double | |
| `robust_z` | double | |
| `detection_method` | text | |
| `run_id` | text | |
