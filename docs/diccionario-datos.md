# Diccionario de datos

Datasets del lake (`data/datalake/`). Por ahora tiene la capa Gold y su manifest. Las fechas están en UTC y, al leer un dataset con Spark, la columna de partition queda al final del schema.

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
