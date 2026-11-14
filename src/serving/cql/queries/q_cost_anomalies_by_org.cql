-- Días con costo anómalo de una organización en un rango de fechas, por servicio.
-- Lee una sola partición (org_id); el rango va sobre la columna de clustering usage_date.
-- Uso: ./infra/run.sh query src/serving/cql/queries/q_cost_anomalies_by_org.cql
--        -p org_id=org_xxx -p from_date=2025-08-18 -p to_date=2025-08-31
SELECT usage_date, service, daily_cost_usd, threshold_usd, anomaly_score
FROM cost_anomaly_mart
WHERE org_id = %(org_id)s
  AND usage_date >= %(from_date)s
  AND usage_date <= %(to_date)s
ORDER BY usage_date DESC, service ASC;
