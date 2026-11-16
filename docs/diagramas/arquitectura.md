# Arquitectura

Versión 2, 2026-11-16. Reemplaza a la Figura 1 del informe de la entrega 1 y muestra lo que está implementado, no lo planeado. Las rutas son relativas a `DATALAKE_PATH` (`data/datalake/` en el repo).

## Arquitectura implementada

```mermaid
flowchart TD
    subgraph LANDING["landing/ (solo lectura)"]
        CSV["7 CSV de maestros"]
        JSONL["usage_events_stream/*.jsonl<br/>120 archivos"]
    end

    subgraph LAKE["Data lake en Parquet"]
        BRONZE["bronze/<br/>maestros por ingest_date<br/>usage_events por event_date"]
        QB["quarantine/bronze/<br/>BRZ_PARSE, BRZ_NULL_KEY, BRZ_BAD_TS"]
        CKP[("checkpoints/bronze_usage_events")]
        SILVER["silver/<br/>7 maestros, orgs_scd2,<br/>usage_events por event_date"]
        QS["quarantine/silver/<br/>reglas SLV_*"]
        MAN[("_manifests/<br/>bronze_batch.json, silver.json, gold.json")]
        GATE{"Gate de reconciliation"}
        DAILY["gold/org_daily_usage_by_service"]
        TOP["gold/top_services_by_org_14d"]
        ANA["Analítica: p99 × multiplier y robust_z"]
        ANOM["gold/cost_anomaly_mart"]
    end

    CAS[("Cassandra local o AstraDB<br/>keyspace cloud_analytics")]
    CQL["Consultas CQL de FinOps"]
    EVI["evidence/<br/>quality, analytics, logs"]

    CSV -- "bronze_batch: esquema explícito, dedupe por clave natural" --> BRONZE
    JSONL -- "bronze_stream: Structured Streaming, watermark 61 días, dedupe" --> BRONZE
    CSV -.-> QB
    JSONL -.-> QB
    JSONL -.- CKP
    BRONZE -- "silver: reglas, flags, joins, SCD2" --> SILVER
    BRONZE -.-> QS
    BRONZE -.-> MAN
    SILVER -.-> MAN
    SILVER --> GATE
    MAN --> GATE
    GATE -- "cierran todos los checks" --> DAILY
    DAILY --> TOP
    DAILY --> ANA --> ANOM
    DAILY & TOP & ANOM -- "serving: gate de publicación, upsert" --> CAS
    CAS --> CQL
    SILVER -.-> EVI
    GATE -.-> EVI
    ANA -.-> EVI
```

Las flechas punteadas son lo que se escribe como control o evidencia: rows rechazadas, conteos de cada step (`_manifests/`), el checkpoint del stream y los reportes de `evidence/`. El gate compara Landing, `bronze/`, `quarantine/bronze/`, `silver/` y `quarantine/silver/` contra los manifests antes de escribir Gold ([`DECISIONS.md`](../../DECISIONS.md), D-01).

## Flujo de los steps

`./infra/run.sh run` corre los cinco steps en este orden, con un mismo `run_id`. Si uno falla, la corrida se corta.

```mermaid
flowchart TD
    S1["1. bronze_batch<br/>CSV → bronze/, quarantine/bronze/<br/>escribe _manifests/bronze_batch.json"]
    S2["2. bronze_stream<br/>JSONL nuevos → bronze/usage_events, quarantine/bronze/usage_events<br/>trigger availableNow, checkpoint"]
    S3["3. silver<br/>reglas y flags, joins, orgs_scd2 → silver/, quarantine/silver/<br/>escribe _manifests/silver.json y evidence/quality/run_id/"]
    G{"4. gold<br/>¿cierran los checks del gate?"}
    S4["Escribe los 3 marts y anomaly_summary.csv<br/>_manifests/gold.json con status ok"]
    F1["_manifests/gold.json con status gate_failed<br/>ReconciliationError, gold/ queda como estaba"]
    P{"5. serving<br/>¿gold.json dice ok?"}
    S5["CREATE TABLE IF NOT EXISTS<br/>y upsert de los 3 marts"]
    F2["GoldNotPublishableError<br/>no se abre la conexión"]

    S1 --> S2 --> S3 --> G
    G -- sí --> S4 --> P
    G -- no --> F1
    P -- sí --> S5
    P -- no --> F2
```

`serving` también se puede correr solo (`./infra/run.sh run serving`); por eso vuelve a mirar `gold.json` antes de publicar.

## Cambios respecto de la Figura 1 del informe

- **Quarantine por capa.** La figura tenía una sola quarantine alimentada desde Bronze. Ahora Bronze escribe en `quarantine/bronze/` y Silver en `quarantine/silver/`, con `_rule_id`, `_quarantine_reason` y `_raw` (D-16).
- **Gate de reconciliation y manifests.** Entre Silver y Gold hay un control que antes no estaba en el diagrama: cada step deja sus conteos en `_manifests/` y `gold` no escribe nada si no cierran (D-01).
- **Gate de publicación.** `serving` solo publica si `_manifests/gold.json` dice `ok` (D-09).
- **Checkpoints a la vista.** El checkpoint del stream es una zona más del lake.
- **Analítica sobre Gold, no después de Cassandra.** La detección de anomalías corre dentro del step `gold` sobre el mart diario y su resultado es un mart más (`cost_anomaly_mart`) que se publica en Cassandra. Silver sigue marcando picos por evento (`is_cost_anomaly`). Detalle en [analitica.md](../analitica.md).
- **Recálculo completo.** Silver y Gold se recalculan enteros en cada corrida, no por fecha afectada (D-15), y la carga a Cassandra es completa por upsert, no por fecha.
- **Stream por corrida.** `bronze_stream` usa `availableNow`: procesa los archivos nuevos y termina, en vez de quedar corriendo (D-11).
- **Gold con tres marts.** `revenue_by_org_month`, `tickets_by_org_date` y `genai_tokens_by_org_date` todavía no están ([backlog](../backlog.md)).
- **Consumo.** Por ahora son las consultas CQL que se corren con `./infra/run.sh query` y `queries`; no hay dashboards.
- **Evidencia.** Cada step deja logs, reportes de calidad y analítica en `evidence/`.
