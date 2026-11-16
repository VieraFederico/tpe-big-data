# Código del pipeline

[`pipeline.py`](pipeline.py) corre los steps en orden (`python -m src.pipeline --steps ...`, o `./infra/run.sh run`). Cada step es un módulo con una función `run(spark, settings)`.

| Paquete | Qué tiene |
| :--- | :--- |
| [`bronze/batch/`](bronze/batch/README.md) | Step `bronze_batch`: los 7 CSV de Landing a Bronze. |
| [`bronze/streaming/`](bronze/streaming/README.md) | Step `bronze_stream`: los eventos JSONL a Bronze con Structured Streaming. |
| [`silver/`](silver/README.md) | Step `silver`: reglas, conformado, enrichment y `orgs_scd2`. |
| [`quality/`](quality/README.md) | Catálogo de reglas y flags, separación en quarantine y reporte por regla. |
| [`gold/`](gold/README.md) | Step `gold`: gate de reconciliation, marts, anomalías y la versión MapReduce. |
| [`serving/`](serving/README.md) | Step `serving`, conexión a Cassandra/AstraDB, DDL y consultas CQL. |
| [`evidence/`](evidence/) | Comandos `queries`, `idempotence` y la copia de la evidencia de la entrega. |
| [`analytics/`](analytics/README.md) | Vacío por ahora. |
| [`common/`](common/) | Esquemas de las fuentes, columnas técnicas, lectura y escritura del lake, manifests, Spark y logging. |

La configuración está en [`config.py`](config.py) ([variables](../config/README.md)).
