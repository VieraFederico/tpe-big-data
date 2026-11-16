# Evidencia

Todo lo que dejan las corridas: logs, reportes de calidad, analítica y la evidencia armada para cada entrega. La carpeta se puede mover con `EVIDENCE_PATH` (y los logs con `LOG_DIR`).

| Carpeta | Qué tiene | Quién la escribe |
| :--- | :--- | :--- |
| `logs/` | Un log por corrida o comando: `pipeline_<run_id>.log`, `mapreduce_check_<ts>.log`, `queries_<ts>.log`, `evidence_queries_<ts>.log`, `evidence_params_<ts>.log`, `idempotence_<run_id>.log`, `evidence_collect_<ts>.log`, `check_connection_<ts>.log`. Tienen conteos, tamaños en disco y duración de cada step. | Todos los puntos de entrada |
| `quality/<run_id>/` | `rule_counts.csv` (rows afectadas por cada regla de Silver), `samples/<dataset>__<rule_id>.csv` (hasta 20 rows por regla con casos) y `reconciliation.csv` (cada check del gate con esperado, real y si pasó). | `silver` y `gold` |
| `analytics/<run_id>/` | `anomaly_summary.csv` (anomalías por service, para calibrar el umbral) y `mapreduce_check.txt` (comparación MapReduce contra DataFrames). | `gold` y `./infra/run.sh mapreduce-check` |
| [`entrega-2/`](entrega-2/README.md) | La evidencia de la entrega 2, con su propio índice. | `./infra/run.sh evidence` |
| `local/` | Consultas de `./infra/run.sh queries` sin carpeta. Está en `.gitignore`. | `./infra/run.sh queries` |

`<ts>` es el timestamp UTC del comando (`YYYYMMDDTHHMMSSZ`) y `<run_id>` el de la corrida, que también queda en cada row que escribe ([data lake](../docs/data-lake.md#convenciones-de-nombres)). `quality/`, `analytics/` y `logs/` se versionan para que la evidencia de cada corrida se pueda revisar sin volver a correr nada.

Nada de esta carpeta tiene secretos: el código no loguea el token de Astra, y la copia de logs a `entrega-2/logs/` borra nombres de bundle y rutas con usuarios.
