# Evidencia de la entrega 2

## Serving en AstraDB

Carga de los marts de Gold en AstraDB y consultas 1 y 2 del informe, corridas el 2026-11-14 contra el keyspace `cloud_analytics`. Resumen de los resultados y cómo se eligieron los parámetros: [`docs/serving.md`](../../docs/serving.md#evidencia-de-la-entrega-2).

| Archivo | Qué muestra |
| :--- | :--- |
| [`queries/q1_daily_usage_by_range.txt`](queries/q1_daily_usage_by_range.txt) y [`.csv`](queries/q1_daily_usage_by_range.csv) | Consulta 1: uso y cost diario por service de `org_chj755nf` del 2025-08-18 al 2025-08-31. |
| [`queries/q2_top_services_14d.txt`](queries/q2_top_services_14d.txt) y [`.csv`](queries/q2_top_services_14d.csv) | Consulta 2: top 5 de services por cost de 14 días de `org_chj755nf` al 2025-08-31. |
| [`../logs/pipeline_20261114T221819Z-ad1d25.log`](../logs/pipeline_20261114T221819Z-ad1d25.log) | Pipeline completo con `serving=astra`: rows escritas y fallidas por tabla y duración de cada step. |
| [`../logs/evidence_params_20261114T223439Z.log`](../logs/evidence_params_20261114T223439Z.log) | Cálculo de los parámetros desde Gold (org elegida y su cost). |
| [`../logs/queries_20261114T224249Z.log`](../logs/queries_20261114T224249Z.log), [`../logs/queries_20261114T224343Z.log`](../logs/queries_20261114T224343Z.log) | Ejecución de las consultas 1 y 2. |
| [`../logs/check_connection_20261114T221737Z.log`](../logs/check_connection_20261114T221737Z.log) | Prueba de conexión a AstraDB antes de la carga. |

Para reproducirla, con `SERVING_TARGET=astra` en `config/.env` y el keyspace creado en Astra:

```bash
./infra/run.sh check
./infra/run.sh run bronze_batch,bronze_stream,silver,gold,serving
./infra/run.sh shell        # adentro: python -m src.serving.evidence_params
./infra/run.sh query src/serving/cql/queries/q1_daily_usage_by_range.cql \
  -p org_id=org_chj755nf -p from_date=2025-08-18 -p to_date=2025-08-31 \
  --evidence-dir evidence/entrega-2/queries
./infra/run.sh query src/serving/cql/queries/q2_top_services_14d.cql \
  -p org_id=org_chj755nf -p as_of_date=2025-08-31 -p n:int=5 \
  --evidence-dir evidence/entrega-2/queries
```

Cada comando nuevo deja su propio log en `evidence/logs/` y las consultas pisan los `.txt` y `.csv` de `queries/`.
