-- Filas de una organización en cost_anomaly_mart (para comparar con Gold tras recargar).
SELECT COUNT(*) AS row_count
FROM cost_anomaly_mart
WHERE org_id = %(org_id)s;
