# Idempotencia

- Fecha: 2026-11-16 19:21 UTC
- `run_id` de la segunda corrida: `20261116T191254Z-d2bc0d`
- Serving: `astra` (keyspace `cloud_analytics`)

Se contaron las filas de cada dataset del lake y de cada tabla del serving, se volvió a correr el pipeline completo y se contaron de nuevo. De los maestros de `bronze/` y `quarantine/bronze/` se cuenta solo el snapshot con el `ingest_date` más alto, porque correr otro día agrega un snapshot nuevo. En Cassandra se suma `count_<tabla>.cql` por cada partición presente en el mart de Gold (en `top_services_by_org_14d`, solo `as_of_date = 2025-08-31`).

| Objeto | Antes | Después | Igual |
| :--- | ---: | ---: | :---: |
| `bronze/billing_monthly` | 240 | 240 | true |
| `bronze/customers_orgs` | 80 | 80 | true |
| `bronze/marketing_touches` | 1500 | 1500 | true |
| `bronze/nps_surveys` | 92 | 92 | true |
| `bronze/resources` | 400 | 400 | true |
| `bronze/support_tickets` | 1000 | 1000 | true |
| `bronze/usage_events` | 43200 | 43200 | true |
| `bronze/users` | 800 | 800 | true |
| `cassandra/cost_anomaly_mart` | 37 | 37 | true |
| `cassandra/org_daily_usage_by_service` | 11050 | 11050 | true |
| `cassandra/top_services_by_org_14d` | 262 | 262 | true |
| `gold/cost_anomaly_mart` | 37 | 37 | true |
| `gold/org_daily_usage_by_service` | 11050 | 11050 | true |
| `gold/top_services_by_org_14d` | 15599 | 15599 | true |
| `quarantine/bronze/billing_monthly` | 0 | 0 | true |
| `quarantine/bronze/customers_orgs` | 0 | 0 | true |
| `quarantine/bronze/marketing_touches` | 0 | 0 | true |
| `quarantine/bronze/nps_surveys` | 0 | 0 | true |
| `quarantine/bronze/resources` | 0 | 0 | true |
| `quarantine/bronze/support_tickets` | 0 | 0 | true |
| `quarantine/bronze/usage_events` | 0 | 0 | true |
| `quarantine/bronze/users` | 0 | 0 | true |
| `quarantine/silver/usage_events` | 211 | 211 | true |
| `silver/billing_monthly` | 240 | 240 | true |
| `silver/customers_orgs` | 80 | 80 | true |
| `silver/marketing_touches` | 1500 | 1500 | true |
| `silver/nps_surveys` | 92 | 92 | true |
| `silver/orgs_scd2` | 80 | 80 | true |
| `silver/resources` | 400 | 400 | true |
| `silver/support_tickets` | 1000 | 1000 | true |
| `silver/usage_events` | 42989 | 42989 | true |
| `silver/users` | 800 | 800 | true |

**Conclusión:** Todos los conteos son iguales: volver a correr el pipeline completo no duplica ni pierde filas en el lake ni en el serving.
