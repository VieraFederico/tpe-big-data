# Decisiones

Registro de las decisiones de diseño del proyecto y de dónde nos apartamos del informe.

## D-01: Gate de reconciliation antes de escribir Gold

- **Fecha**: 2026-11-14
- **Contexto**: El informe promete que todo lo que llegó a Landing está en Bronze, en quarantine o descartado como duplicado, y que todo lo de Bronze pasó a Silver o a quarantine. Si algo se pierde en el camino (o un write quedó a medias), Gold y Cassandra mostrarían números incompletos sin que nadie se entere.
- **Decisión**: El step `gold` ([`src/gold/reconciliation.py`](src/gold/reconciliation.py)) corre estos checks antes de escribir cualquier mart:

  | `check_id` | Qué compara |
  | :--- | :--- |
  | `MANIFESTS_PRESENT` | Que existan `_manifests/bronze_batch.json` y `_manifests/silver.json`. |
  | `BRZ_SILVER_BALANCE` | Por dataset de Silver: `bronze_rows = silver_rows + quarantine_rows + duplicates_dropped` del manifest. |
  | `SILVER_ROWS_ON_DISK` | `silver_rows` del manifest contra las rows en `silver/<dataset>`. |
  | `SILVER_QUARANTINE_ON_DISK` | `quarantine_rows` del manifest contra `quarantine/silver/<dataset>`. |
  | `BRONZE_INPUT_ON_DISK` | `bronze_rows` del manifest contra el input de Bronze (snapshot actual para master data, todo para events). |
  | `LANDING_BRONZE_BATCH` | Por CSV: rows de Landing (leído con el mismo reader PERMISSIVE de Bronze) = `written + quarantine + duplicates` del manifest, y `written` y `quarantine` contra lo que hay en disco en esa `ingest_date`. |
  | `LANDING_BRONZE_STREAM` | Keys de dedupe distintas en Landing = rows en `bronze/usage_events` + `quarantine/bronze/usage_events`. La key se calcula con la misma función que usa el stream (`event_dedupe_key`: `event_id` o, si es null, un hash SHA-256 del record). |

  Cada comparación es una row de `evidence/quality/<run_id>/reconciliation.csv` (`check_id`, `dataset`, `expected`, `actual`, `passed`); cuando un check compara varias cosas, `dataset` dice cuál (por ejemplo `customers_orgs`, `bronze/customers_orgs` y `quarantine/bronze/customers_orgs`). Si alguno no cierra, el step loguea cada uno con esperado y real, escribe `_manifests/gold.json` con `status = "gate_failed"` y la lista de `failed_checks`, no toca `gold/` y termina con `ReconciliationError`. Un dataset o archivo que falta cuenta como 0 rows, así el gate falla en vez de romperse. `orgs_scd2` solo entra en `SILVER_ROWS_ON_DISK`, porque no tiene una relación uno a uno con las rows de Bronze.
- **Alternativas descartadas**: Contar las líneas crudas de Landing para los events (con duplicados no cerraría nunca). Copiar la expresión de la key de dedupe en Gold (se podría desfasar de la del stream). Una sola row por fuente con los valores combinados en el CSV (no se ve qué parte falló).
- **Consecuencias**: En la corrida completa pasan los 57 checks. Si el stream descarta events tardíos por el watermark, o duplicados más separados que el watermark, `LANDING_BRONZE_STREAM` falla: es a propósito, porque muestra la pérdida. El step `serving` lee `gold.json` para no publicar si el gate falló.
- **Estado**: Aceptada.

## D-02: `top_services_by_org_14d` para todas las fechas de corte

- **Fecha**: 2026-11-14
- **Contexto**: La consulta 2 del informe pide el top de services por cost de los últimos 14 días, anclada en 2025-08-31. Si el mart se calcula solo para esa fecha, consultar otra fecha obliga a recalcular Gold.
- **Decisión**: El mart se calcula para cada `as_of_date` entre el primer y el último `usage_date` del mart diario. El rango es global (el mismo para todas las orgs), no el de cada org. Se arma la lista de fechas con `sequence(min, max)`, se cruza con el mart diario por `usage_date BETWEEN as_of_date − 13 AND as_of_date` y se suma por `(org_id, service, as_of_date)`. `rank` es `row_number()` por `(org_id, as_of_date)` ordenado por `cost_14d_usd` descendente y `service` ascendente, así no hay empates ni huecos. `window_start_date` muestra dónde empieza la ventana, que al principio del rango tiene menos de 14 días de datos.
- **Alternativas descartadas**: Solo la fecha ancla (no deja consultar otras fechas). Una ventana `rangeBetween` sobre el mart diario (solo da rows en los días en que el service tuvo datos, así que le faltan fechas de corte en las que el service sí tiene días en la ventana). Rango de fechas por org.
- **Consecuencias**: En la corrida completa son 15599 rows (60 fechas). El join entre fechas y mart diario es un nested loop con las fechas en broadcast; va bien para unos 60 días × 11 mil rows, pero crece con días × rows.
- **Estado**: Aceptada.

## D-03: `usage_date` en vez de `date` en `cost_anomaly_mart`

- **Fecha**: 2026-11-14
- **Contexto**: La tabla de claves del informe usa la columna `date` para `cost_anomaly_mart`, pero la convención de nombres del mismo informe dice que las fechas terminan en `_date`.
- **Decisión**: La columna se llama `usage_date`, igual que en `org_daily_usage_by_service`, de donde sale.
- **Alternativas descartadas**: Usar `date` como en la tabla del informe.
- **Consecuencias**: Es un desvío del informe; las tablas de Cassandra que salgan de este mart también usan `usage_date`.
- **Estado**: Aceptada.

## D-04: Umbral de anomalías en 3 × p99 por service

- **Fecha**: 2026-11-14
- **Contexto**: El informe propone marcar como anomalía un cost diario mayor a 3 × p99 y dice que el valor final se ajusta mirando cuántos casos detecta.
- **Decisión**: Una row del mart diario es anomalía si `p99_usd > 0` y `daily_cost_usd > ANOMALY_P99_MULTIPLIER × p99_usd`, con el p99 exacto por service sobre todas las orgs. El multiplier es configurable por variable de entorno y queda en `3`. El robust z (MAD) se guarda como segunda opinión pero no decide. Calibración sobre la corrida completa (11050 rows en el mart diario):

  | multiplier | rows detectadas |
  | ---: | ---: |
  | 2 | 73 |
  | 2.5 | 61 |
  | **3** | **37** |
  | 4 | 7 |
  | 5 | 1 |

  Con `robust_z > 3.5` serían 247. Con 3 quedan 37 casos (0.33%), todos con `robust_z` mayor a 18; con 2.5 casi se duplican, sobre todo en `compute`, y con 4 `compute` y `database` se quedan sin casos. Detalle en [docs/analitica.md](docs/analitica.md).
- **Alternativas descartadas**: Decidir con el robust z (marca 247 días, demasiados por la cola larga de los costs). Umbral fijo en USD (los services tienen escalas de cost muy distintas). Un multiplier más alto o más bajo (ver tabla).
- **Consecuencias**: `evidence/analytics/<run_id>/anomaly_summary.csv` deja los conteos de cada corrida para volver a calibrar. Si cambia el multiplier, cambia `detection_method` (`p99_x3`, `p99_x2.5`) y las rows del mart, sin tocar código.
- **Estado**: Aceptada.

## D-05: MapReduce como implementación de referencia del mart diario

- **Fecha**: 2026-11-14
- **Contexto**: El informe explica `org_daily_usage_by_service` con las fases de MapReduce. Spark permite escribirlo así sobre RDDs o con DataFrames.
- **Decisión**: El pipeline usa la versión con DataFrames, que optimiza Catalyst. La versión MapReduce ([`src/gold/mapreduce.py`](src/gold/mapreduce.py): `map_event`, `reduce_vectors`, `org_daily_usage_mapreduce`) queda como referencia y se compara con la otra en un test y con `./infra/run.sh mapreduce-check` sobre el dataset completo. Ninguna de las dos agrega `run_id`; lo agrega el step al escribir, así se pueden comparar directo.
- **Alternativas descartadas**: Escribir Gold con la versión de RDDs (cada row pasa por Python y Spark no puede optimizar). No tener la versión MapReduce (la lógica del informe quedaría sin mostrar).
- **Consecuencias**: Hay dos implementaciones de la misma regla; si cambia el mart diario hay que cambiar las dos, y el test lo detecta si no. En la corrida completa dieron las mismas 11050 keys y diferencia 0 en todas las métricas. Detalle en [docs/mapreduce.md](docs/mapreduce.md).
- **Estado**: Aceptada.

## D-06: Primary key de `org_daily_usage_by_service` en Cassandra

- **Fecha**: 2026-11-14
- **Contexto**: La consulta 1 del informe pide el uso y cost diario por service de una org en un rango de fechas, con los días más recientes primero. En Cassandra una consulta sin `ALLOW FILTERING` tiene que fijar la partition key completa y solo puede filtrar por rango y ordenar por las clustering columns.
- **Decisión**: `PRIMARY KEY ((org_id), usage_date, service)` con `CLUSTERING ORDER BY (usage_date DESC, service ASC)`, como en la tabla de claves del informe. La consulta ([`q1_daily_usage_by_range.cql`](src/serving/cql/queries/q1_daily_usage_by_range.cql)) fija `org_id`, filtra `usage_date` entre `from_date` y `to_date` y lee una sola partition, ya ordenada.
- **Alternativas descartadas**: Partition por `(org_id, usage_date)` (un rango de fechas tendría que leer una partition por día). Partition por `usage_date` (consultar una org necesitaría `ALLOW FILTERING`, y la partition de cada día tendría las 80 orgs). Partir la partition por mes (con dos meses de datos no hace falta).
- **Consecuencias**: Son 80 partitions, de como mucho 60 días × 6 services = 360 rows con el dataset del repo. La partition de una org crece con los días; con años de datos habría que partirla por mes. La tabla no sirve para consultar un día de todas las orgs.
- **Estado**: Aceptada.

## D-07: Primary key de `top_services_by_org_14d` en Cassandra

- **Fecha**: 2026-11-14
- **Contexto**: La consulta 2 del informe pide el top-N de services por cost de los últimos 14 días de una org, siempre a una fecha de corte dada. El mart tiene una row por org, fecha de corte y service, con un `rank` sin empates ni huecos (D-02).
- **Decisión**: `PRIMARY KEY ((org_id, as_of_date), rank)` con `CLUSTERING ORDER BY (rank ASC)`. La consulta ([`q2_top_services_14d.cql`](src/serving/cql/queries/q2_top_services_14d.cql)) fija la org y la fecha y hace `ORDER BY rank ASC LIMIT n`: lee una partition de como mucho 6 rows y el `LIMIT` ya es el top-N.
- **Alternativas descartadas**: Partition solo por `org_id` con `as_of_date` como clustering (también respondería, pero la consulta siempre fija la fecha y así cada partition es chica, como en la tabla de claves del informe). `service` como clustering (el top habría que ordenarlo en el cliente y `LIMIT n` no serviría).
- **Consecuencias**: `service` no está en la key: si `rank` se repitiera en una misma org y fecha, una row pisaría a otra. No pasa porque `rank` sale de `row_number()`. Para ver el top de varias fechas hay que hacer una consulta por fecha.
- **Estado**: Aceptada.

## D-08: Primary key de `cost_anomaly_mart` en Cassandra

- **Fecha**: 2026-11-14
- **Contexto**: Las anomalías se consultan por org en un rango de fechas, igual que la consulta 1. El mart tiene una row por org, día y service, y la fecha se llama `usage_date` (D-03).
- **Decisión**: La misma key que el mart diario: `PRIMARY KEY ((org_id), usage_date, service)` con `CLUSTERING ORDER BY (usage_date DESC, service ASC)`. La consulta es [`q_cost_anomalies_by_org.cql`](src/serving/cql/queries/q_cost_anomalies_by_org.cql).
- **Alternativas descartadas**: Partition por `usage_date` o por `service` (ver las anomalías de un día o de un service para todas las orgs no es una consulta del informe, y la consulta por org necesitaría `ALLOW FILTERING`).
- **Consecuencias**: Las partitions son muy chicas (37 rows en total en la corrida completa). Una org sin anomalías no tiene partition y la consulta devuelve 0 rows.
- **Estado**: Aceptada.

## D-09: Carga completa por upsert con gate de publicación

- **Fecha**: 2026-11-14
- **Contexto**: Gold se recalcula completo en cada corrida y el step `serving` lo publica en Cassandra o AstraDB. Volver a correr `serving` no puede duplicar rows, y no se tiene que publicar un Gold cuya reconciliation falló (D-01).
- **Decisión**: El step `serving` ([`src/serving/cassandra_writer.py`](src/serving/cassandra_writer.py)) primero lee `_manifests/gold.json`: si no existe o su `status` no es `ok`, termina con `GoldNotPublishableError` antes de abrir la conexión, sin crear ni escribir tablas. Si está `ok`, corre los `CREATE TABLE IF NOT EXISTS` de `src/serving/cql/schema/` y carga cada mart entero en su tabla con un `INSERT` preparado, en paralelo desde los workers de Spark. Cassandra trata cada `INSERT` como upsert por primary key. Los null se mandan como `UNSET` para no generar tombstones. Si alguna row falla, el step falla (`WriteFailedError`).
- **Alternativas descartadas**: Vaciar las tablas antes de cargar (mientras se recarga las consultas verían tablas vacías o a medias). Borrar las keys que ya no están en Gold (habría que leer la tabla entera para compararla). Carga incremental por fecha o `run_id` (Gold se recalcula entero de todas formas). Poner el gate dentro de la función que escribe (también la usa el streaming, que no depende de Gold). Comparar las cantidades del manifest con los parquet (la reconciliation ya se hizo en `gold`). Mandar los null como null (cada uno es un tombstone).
- **Consecuencias**: Recargar el mismo Gold deja las mismas rows; los `count_*.cql` lo muestran. Si un Gold nuevo ya no tiene una key, su row queda en la tabla, y un null no borra un valor anterior; con Landing inmutable y Gold recalculado completo no pasa, y si pasa hay que vaciar la tabla antes de cargar. `CREATE TABLE IF NOT EXISTS` no cambia una tabla existente: si cambia un DDL hay que borrar la tabla. Detalle en [docs/serving.md](docs/serving.md).
- **Estado**: Aceptada.
