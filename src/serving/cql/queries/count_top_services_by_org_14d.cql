-- Filas de una organización a una fecha de corte en top_services_by_org_14d.
SELECT COUNT(*) AS row_count
FROM top_services_by_org_14d
WHERE org_id = %(org_id)s
  AND as_of_date = %(as_of_date)s;
