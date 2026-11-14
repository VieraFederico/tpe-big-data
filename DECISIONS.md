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
