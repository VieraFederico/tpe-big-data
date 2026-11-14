-- Uso y costo diario por organización y servicio (consulta 1: rango de fechas de una org).
-- Una partición por org; dentro, los días más recientes primero y los servicios en orden.
CREATE TABLE IF NOT EXISTS org_daily_usage_by_service (
    org_id text,
    service text,
    usage_date date,
    daily_cost_usd double,
    requests double,
    cpu_hours double,
    storage_gb_hours double,
    carbon_kg double,
    genai_tokens bigint,
    event_count int,
    cost_anomaly_events int,
    run_id text,
    PRIMARY KEY ((org_id), usage_date, service)
) WITH CLUSTERING ORDER BY (usage_date DESC, service ASC);
