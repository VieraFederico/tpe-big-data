-- Top de servicios por costo de los últimos 14 días (consulta 2: top-N de una org a una fecha).
-- La fecha de corte va en la partición; el rank como clustering hace que LIMIT n sea el top-N.
CREATE TABLE IF NOT EXISTS top_services_by_org_14d (
    org_id text,
    as_of_date date,
    rank int,
    service text,
    window_start_date date,
    cost_14d_usd double,
    requests_14d double,
    run_id text,
    PRIMARY KEY ((org_id, as_of_date), rank)
) WITH CLUSTERING ORDER BY (rank ASC);
