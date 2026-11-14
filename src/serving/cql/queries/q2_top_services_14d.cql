-- Consulta 2: top-N de servicios por costo de los últimos 14 días de una organización.
-- La partición es (org_id, as_of_date) y el rank es la clustering, así que LIMIT n es el top-N.
-- n va como entero (n:int): si llega como texto, Cassandra rechaza el LIMIT.
-- Uso: ./infra/run.sh query src/serving/cql/queries/q2_top_services_14d.cql
--        -p org_id=org_xxx -p as_of_date=2025-08-31 -p n:int=5
SELECT rank, service, cost_14d_usd, requests_14d, window_start_date
FROM top_services_by_org_14d
WHERE org_id = %(org_id)s
  AND as_of_date = %(as_of_date)s
ORDER BY rank ASC
LIMIT %(n)s;
