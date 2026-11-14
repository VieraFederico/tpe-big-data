-- Filas de una organización en org_daily_usage_by_service (para comparar con Gold tras recargar).
SELECT COUNT(*) AS row_count
FROM org_daily_usage_by_service
WHERE org_id = %(org_id)s;
