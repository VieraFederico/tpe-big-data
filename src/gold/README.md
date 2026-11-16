# Gold

Step `gold` ([`build.py`](build.py)). Primero corre el gate y, si pasa, reemplaza los tres marts y escribe `_manifests/gold.json`.

| Módulo | Qué hace |
| :--- | :--- |
| [`reconciliation.py`](reconciliation.py) | Gate de reconciliation: compara Landing, Bronze, Silver y quarantine con los manifests y deja `reconciliation.csv`. |
| [`marts.py`](marts.py) | `org_daily_usage_by_service` y `top_services_by_org_14d`. |
| [`anomalies.py`](anomalies.py) | `cost_anomaly_mart` (p99 × multiplier y `robust_z`) y `anomaly_summary.csv`. |
| [`mapreduce.py`](mapreduce.py) | El mart diario escrito como MapReduce sobre RDDs y la comparación de `./infra/run.sh mapreduce-check`. |

Columnas en [`docs/diccionario-datos.md`](../../docs/diccionario-datos.md#gold); MapReduce en [`docs/mapreduce.md`](../../docs/mapreduce.md); anomalías en [`docs/analitica.md`](../../docs/analitica.md).
