-- Días con costo anómalo por organización y servicio.
-- Misma clave que el uso diario: una partición por org, los días más recientes primero.
CREATE TABLE IF NOT EXISTS cost_anomaly_mart (
    org_id text,
    service text,
    usage_date date,
    daily_cost_usd double,
    p99_usd double,
    threshold_usd double,
    anomaly_score double,
    robust_z double,
    detection_method text,
    run_id text,
    PRIMARY KEY ((org_id), usage_date, service)
) WITH CLUSTERING ORDER BY (usage_date DESC, service ASC);
