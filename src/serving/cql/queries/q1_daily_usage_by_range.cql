-- Consulta 1: uso y costo diario por servicio de una organización en un rango de fechas.
-- Lee una sola partición (org_id); el rango va sobre la columna de clustering usage_date.
-- Uso: ./infra/run.sh query src/serving/cql/queries/q1_daily_usage_by_range.cql
--        -p org_id=org_xxx -p from_date=2025-08-18 -p to_date=2025-08-31
SELECT usage_date, service, daily_cost_usd, requests, cpu_hours, storage_gb_hours,
       genai_tokens, carbon_kg
FROM org_daily_usage_by_service
WHERE org_id = %(org_id)s
  AND usage_date >= %(from_date)s
  AND usage_date <= %(to_date)s
ORDER BY usage_date DESC, service ASC;
