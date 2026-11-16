# Silver

Step `silver` ([`build.py`](build.py)). Reemplaza Silver entero en cada corrida.

| Módulo | Qué hace |
| :--- | :--- |
| [`master_data.py`](master_data.py) | Último snapshot de cada maestro, normalización, correcciones, `revenue_usd` y `tags`. |
| [`scd2.py`](scd2.py) | `orgs_scd2`: una row por versión de cada org. |
| [`events.py`](events.py) | Dedupe por `event_id`, reglas de quarantine, `value_numeric`, `unit` imputada y columnas por métrica. |
| [`enrichment.py`](enrichment.py) | Join con `customers_orgs` y `resources` (broadcast) y flags de huérfanos. |
| [`anomalies.py`](anomalies.py) | `is_cost_anomaly` y `anomaly_score` por evento (3 × p99 del service). |

Deja `_manifests/silver.json` y el reporte de calidad en `evidence/quality/<run_id>/`. Las reglas están en [`docs/calidad.md`](../../docs/calidad.md).
