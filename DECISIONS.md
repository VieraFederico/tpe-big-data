# Decisiones

Registro de las decisiones de diseño del proyecto y de dónde nos apartamos del informe de la entrega 1. Cada entrada tiene fecha, contexto, decisión, alternativas descartadas, consecuencias y estado (`vigente`, `reemplazada por D-NN` o `abierta`). Lo que todavía no está decidido va en [Decisiones abiertas](#decisiones-abiertas), con prioridad y fecha límite.

| Tema | Decisiones |
| :--- | :--- |
| Arquitectura y entorno | D-10 (patrón híbrido), D-11 (watermark), D-15 (recálculo completo), D-20 (Docker) |
| Data lake | D-12 (particiones), D-16 (quarantine por capa), D-19 (snapshot de maestros) |
| Calidad | D-13 (regla de costo), D-14 (`value` como texto), D-17 (tipo de cambio) |
| Gold y analítica | D-01 (gate), D-02 (top 14 días), D-03 (`usage_date`), D-04 (umbral de anomalías), D-05 (MapReduce), D-18 (métricas como columnas) |
| Serving | D-06, D-07, D-08 (primary keys), D-09 (carga por upsert) |

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
- **Estado**: vigente

## D-02: `top_services_by_org_14d` para todas las fechas de corte

- **Fecha**: 2026-11-14
- **Contexto**: La consulta 2 del informe pide el top de services por cost de los últimos 14 días, anclada en 2025-08-31. Si el mart se calcula solo para esa fecha, consultar otra fecha obliga a recalcular Gold.
- **Decisión**: El mart se calcula para cada `as_of_date` entre el primer y el último `usage_date` del mart diario. El rango es global (el mismo para todas las orgs), no el de cada org. Se arma la lista de fechas con `sequence(min, max)`, se cruza con el mart diario por `usage_date BETWEEN as_of_date − 13 AND as_of_date` y se suma por `(org_id, service, as_of_date)`. `rank` es `row_number()` por `(org_id, as_of_date)` ordenado por `cost_14d_usd` descendente y `service` ascendente, así no hay empates ni huecos. `window_start_date` muestra dónde empieza la ventana, que al principio del rango tiene menos de 14 días de datos.
- **Alternativas descartadas**: Solo la fecha ancla (no deja consultar otras fechas). Una ventana `rangeBetween` sobre el mart diario (solo da rows en los días en que el service tuvo datos, así que le faltan fechas de corte en las que el service sí tiene días en la ventana). Rango de fechas por org.
- **Consecuencias**: En la corrida completa son 15599 rows (60 fechas). El join entre fechas y mart diario es un nested loop con las fechas en broadcast; va bien para unos 60 días × 11 mil rows, pero crece con días × rows.
- **Estado**: vigente

## D-03: `usage_date` en vez de `date` en `cost_anomaly_mart`

- **Fecha**: 2026-11-14
- **Contexto**: La tabla de claves del informe usa la columna `date` para `cost_anomaly_mart`, pero la convención de nombres del mismo informe dice que las fechas terminan en `_date`.
- **Decisión**: La columna se llama `usage_date`, igual que en `org_daily_usage_by_service`, de donde sale.
- **Alternativas descartadas**: Usar `date` como en la tabla del informe.
- **Consecuencias**: Es un desvío del informe; las tablas de Cassandra que salgan de este mart también usan `usage_date`.
- **Estado**: vigente

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
- **Estado**: vigente

## D-05: MapReduce como implementación de referencia del mart diario

- **Fecha**: 2026-11-14
- **Contexto**: El informe explica `org_daily_usage_by_service` con las fases de MapReduce. Spark permite escribirlo así sobre RDDs o con DataFrames.
- **Decisión**: El pipeline usa la versión con DataFrames, que optimiza Catalyst. La versión MapReduce ([`src/gold/mapreduce.py`](src/gold/mapreduce.py): `map_event`, `reduce_vectors`, `org_daily_usage_mapreduce`) queda como referencia y se compara con la otra en un test y con `./infra/run.sh mapreduce-check` sobre el dataset completo. Ninguna de las dos agrega `run_id`; lo agrega el step al escribir, así se pueden comparar directo.
- **Alternativas descartadas**: Escribir Gold con la versión de RDDs (cada row pasa por Python y Spark no puede optimizar). No tener la versión MapReduce (la lógica del informe quedaría sin mostrar).
- **Consecuencias**: Hay dos implementaciones de la misma regla; si cambia el mart diario hay que cambiar las dos, y el test lo detecta si no. En la corrida completa dieron las mismas 11050 keys y diferencia 0 en todas las métricas. Detalle en [docs/mapreduce.md](docs/mapreduce.md).
- **Estado**: vigente

## D-06: Primary key de `org_daily_usage_by_service` en Cassandra

- **Fecha**: 2026-11-14
- **Contexto**: La consulta 1 del informe pide el uso y cost diario por service de una org en un rango de fechas, con los días más recientes primero. En Cassandra una consulta sin `ALLOW FILTERING` tiene que fijar la partition key completa y solo puede filtrar por rango y ordenar por las clustering columns.
- **Decisión**: `PRIMARY KEY ((org_id), usage_date, service)` con `CLUSTERING ORDER BY (usage_date DESC, service ASC)`, como en la tabla de claves del informe. La consulta ([`q1_daily_usage_by_range.cql`](src/serving/cql/queries/q1_daily_usage_by_range.cql)) fija `org_id`, filtra `usage_date` entre `from_date` y `to_date` y lee una sola partition, ya ordenada.
- **Alternativas descartadas**: Partition por `(org_id, usage_date)` (un rango de fechas tendría que leer una partition por día). Partition por `usage_date` (consultar una org necesitaría `ALLOW FILTERING`, y la partition de cada día tendría las 80 orgs). Partir la partition por mes (con dos meses de datos no hace falta).
- **Consecuencias**: Son 80 partitions, de como mucho 60 días × 6 services = 360 rows con el dataset del repo. La partition de una org crece con los días; con años de datos habría que partirla por mes. La tabla no sirve para consultar un día de todas las orgs.
- **Estado**: vigente

## D-07: Primary key de `top_services_by_org_14d` en Cassandra

- **Fecha**: 2026-11-14
- **Contexto**: La consulta 2 del informe pide el top-N de services por cost de los últimos 14 días de una org, siempre a una fecha de corte dada. El mart tiene una row por org, fecha de corte y service, con un `rank` sin empates ni huecos (D-02).
- **Decisión**: `PRIMARY KEY ((org_id, as_of_date), rank)` con `CLUSTERING ORDER BY (rank ASC)`. La consulta ([`q2_top_services_14d.cql`](src/serving/cql/queries/q2_top_services_14d.cql)) fija la org y la fecha y hace `ORDER BY rank ASC LIMIT n`: lee una partition de como mucho 6 rows y el `LIMIT` ya es el top-N.
- **Alternativas descartadas**: Partition solo por `org_id` con `as_of_date` como clustering (también respondería, pero la consulta siempre fija la fecha y así cada partition es chica, como en la tabla de claves del informe). `service` como clustering (el top habría que ordenarlo en el cliente y `LIMIT n` no serviría).
- **Consecuencias**: `service` no está en la key: si `rank` se repitiera en una misma org y fecha, una row pisaría a otra. No pasa porque `rank` sale de `row_number()`. Para ver el top de varias fechas hay que hacer una consulta por fecha.
- **Estado**: vigente

## D-08: Primary key de `cost_anomaly_mart` en Cassandra

- **Fecha**: 2026-11-14
- **Contexto**: Las anomalías se consultan por org en un rango de fechas, igual que la consulta 1. El mart tiene una row por org, día y service, y la fecha se llama `usage_date` (D-03).
- **Decisión**: La misma key que el mart diario: `PRIMARY KEY ((org_id), usage_date, service)` con `CLUSTERING ORDER BY (usage_date DESC, service ASC)`. La consulta es [`q_cost_anomalies_by_org.cql`](src/serving/cql/queries/q_cost_anomalies_by_org.cql).
- **Alternativas descartadas**: Partition por `usage_date` o por `service` (ver las anomalías de un día o de un service para todas las orgs no es una consulta del informe, y la consulta por org necesitaría `ALLOW FILTERING`).
- **Consecuencias**: Las partitions son muy chicas (37 rows en total en la corrida completa). Una org sin anomalías no tiene partition y la consulta devuelve 0 rows.
- **Estado**: vigente

## D-09: Carga completa por upsert con gate de publicación

- **Fecha**: 2026-11-14
- **Contexto**: Gold se recalcula completo en cada corrida y el step `serving` lo publica en Cassandra o AstraDB. Volver a correr `serving` no puede duplicar rows, y no se tiene que publicar un Gold cuya reconciliation falló (D-01).
- **Decisión**: El step `serving` ([`src/serving/cassandra_writer.py`](src/serving/cassandra_writer.py)) primero lee `_manifests/gold.json`: si no existe o su `status` no es `ok`, termina con `GoldNotPublishableError` antes de abrir la conexión, sin crear ni escribir tablas. Si está `ok`, corre los `CREATE TABLE IF NOT EXISTS` de `src/serving/cql/schema/` y carga cada mart entero en su tabla con un `INSERT` preparado, en paralelo desde los workers de Spark. Cassandra trata cada `INSERT` como upsert por primary key. Los null se mandan como `UNSET` para no generar tombstones. Si alguna row falla, el step falla (`WriteFailedError`).
- **Alternativas descartadas**: Vaciar las tablas antes de cargar (mientras se recarga las consultas verían tablas vacías o a medias). Borrar las keys que ya no están en Gold (habría que leer la tabla entera para compararla). Carga incremental por fecha o `run_id` (Gold se recalcula entero de todas formas). Poner el gate dentro de la función que escribe (también la usa el streaming, que no depende de Gold). Comparar las cantidades del manifest con los parquet (la reconciliation ya se hizo en `gold`). Mandar los null como null (cada uno es un tombstone).
- **Consecuencias**: Recargar el mismo Gold deja las mismas rows; los `count_*.cql` lo muestran. Si un Gold nuevo ya no tiene una key, su row queda en la tabla, y un null no borra un valor anterior; con Landing inmutable y Gold recalculado completo no pasa, y si pasa hay que vaciar la tabla antes de cargar. `CREATE TABLE IF NOT EXISTS` no cambia una tabla existente: si cambia un DDL hay que borrar la tabla. Detalle en [docs/serving.md](docs/serving.md).
- **Estado**: vigente

## D-10: Patrón híbrido: ingesta streaming, procesamiento batch

- **Fecha**: 2026-11-08
- **Contexto**: La consigna pide streaming para los eventos y batch para maestros y facturación, con un patrón Lambda, Kappa o híbrido justificado. Cada uno de los 120 archivos de eventos cubre los 60 días del dataset: el 99 % de los eventos llega después de otros más nuevos (informe, sección 3.3).
- **Decisión**: La velocidad se resuelve en la ingesta y toda la lógica de negocio vive en una sola capa batch. `bronze_stream` lee los JSONL con Structured Streaming (esquema explícito, watermark, dedupe, checkpoint) y `bronze_batch` carga los siete CSV. `silver`, `gold` y `serving` corren en batch sobre Bronze para las dos fuentes.
- **Alternativas descartadas**: Lambda (los marts se calcularían dos veces, en la capa batch y en la de velocidad, y con eventos tan desordenados los resultados provisorios cambiarían con cada archivo). Kappa (habría que tratar como stream CSV que son fotos diarias o mensuales, y agregar en el stream obligaría a mantener abiertas las sumas de los 60 días de cada org y service).
- **Consecuencias**: Hay una sola implementación de cada regla y de cada mart. La latencia de Gold es la de la próxima corrida batch, no la del micro-lote. Ante un cambio de reglas o un evento muy tardío se vuelve a correr desde Landing, que no se borra nunca.
- **Estado**: vigente

## D-11: Watermark de 61 días en `bronze_stream`

- **Fecha**: 2026-11-08
- **Contexto**: El stream deduplica por `event_id` con `dropDuplicatesWithinWatermark`, que descarta lo que llega detrás del watermark. Como los archivos están desordenados, un watermark de minutos descartaría casi todos los eventos desde el segundo micro-lote.
- **Decisión**: `WATERMARK = "61 days"` sobre `event_ts` ([`src/bronze/streaming/ingest.py`](src/bronze/streaming/ingest.py)): el rango completo del dataset más un día de margen. Los eventos sin `event_id` se deduplican por un hash SHA-256 del record. El trigger es `availableNow`: cada corrida procesa los archivos nuevos y termina, y el checkpoint en `checkpoints/bronze_usage_events` recuerda qué archivos ya se leyeron y el estado del dedupe.
- **Alternativas descartadas**: Un watermark corto (pierde datos). Sin watermark (el estado del dedupe crece sin límite). Un stream que queda corriendo (no hace falta para reproducir la entrega y complica el Quickstart).
- **Consecuencias**: Con el dataset del repo no se descarta ningún evento (`dropped_by_watermark: 0` en el log de la corrida `20261114T221819Z-ad1d25`). El estado del dedupe guarda 61 días de keys. En producción el margen se ajustaría a los retrasos reales, y lo que llegue más tarde se reprocesa desde Landing. El gate de reconciliation (D-01) hace visible cualquier evento perdido.
- **Estado**: vigente

## D-12: Particiones por fecha, nunca por service

- **Fecha**: 2026-11-13
- **Contexto**: La consigna sugiere particionar por fecha y/o service. Casi todas las consultas filtran por un rango de fechas, y hay 6 services en 60 días.
- **Decisión**: Cada dataset se particiona por la fecha con la que se lo consulta o se lo reemplaza: maestros de Bronze por `ingest_date` (un snapshot por corrida), eventos de Bronze y Silver por `event_date`, `silver/billing_monthly` por `billing_month`, marts de Gold por `usage_date` o `as_of_date` y la quarantine por `ingest_date`. Los maestros de Silver y `orgs_scd2` no se particionan. Antes de escribir se agrupan las rows por la columna de partición (`REBALANCE`, o `repartition` en los micro-lotes) para dejar pocos archivos por partition. La tabla completa está en [docs/data-lake.md](docs/data-lake.md#datasets-y-particiones).
- **Alternativas descartadas**: Partición por service (6 × 60 = 360 carpetas de unas 120 rows cada una: muchos archivos chicos que hacen más lenta la lectura). Particionar los maestros chicos de Silver (cientos de rows; una carpeta por valor no ayuda).
- **Consecuencias**: Filtrar por service obliga a leer todas las fechas del rango, que con este volumen es barato. `evidence/entrega-2/storage.csv` muestra particiones, archivos y MB por dataset.
- **Estado**: vigente

## D-13: Regla de costo híbrida

- **Fecha**: 2026-11-13
- **Contexto**: El informe mandaba a quarantine todo `cost_usd_increment < 0`. La consigna pide `cost_usd_increment >= -0.01` con flag de anomalía. En el dataset hay 211 eventos por debajo de -0.01 y 5 entre -0.01 y 0.
- **Decisión**: `SLV_COST_INVALID` manda a quarantine los costs null o `< -0.01`. Los que están en `[-0.01, 0)` quedan en Silver con su valor original y el flag `COST_NEGATIVE_SMALL` en `dq_flags` ([`src/quality/catalog.py`](src/quality/catalog.py), `MIN_ACCEPTED_COST`).
- **Alternativas descartadas**: Quarantine para todo cost negativo (como el informe: pierde ajustes de redondeo legítimos). Pasar los negativos chicos a 0 (cambia el dato sin necesidad).
- **Consecuencias**: Los 211 eventos que restarían cost real no llegan a Gold. `daily_cost_usd` incluye los 5 ajustes negativos chicos.
- **Estado**: vigente

## D-14: `value` como texto sin flag propio

- **Fecha**: 2026-11-13
- **Contexto**: El informe prometía castear el `value` que llega como texto numérico y marcarlo con un flag. Bronze lee `value` como string con un esquema explícito, así que `120.0` y `"120.0"` llegan a Silver iguales.
- **Decisión**: Todo `value` que se puede convertir a número se castea igual a `value_numeric`, sin flag. El que no se puede convertir va a quarantine (`SLV_VALUE_NOT_NUMERIC`) y el null queda con `VALUE_NULL`.
- **Alternativas descartadas**: Guardar el record crudo de cada evento válido en Bronze para saber si venía entre comillas (cambia el contrato de Bronze por un flag que no cambia ningún resultado).
- **Consecuencias**: Es un desvío del informe (Tabla 6). Queda en el backlog como deseable.
- **Estado**: vigente

## D-15: Silver y Gold se recalculan completos en cada corrida

- **Fecha**: 2026-11-13
- **Contexto**: El informe proponía recalcular solo las particiones de las fechas afectadas por eventos nuevos. Pero el p99 por service de Silver, el p99 y el MAD de las anomalías de Gold y `orgs_scd2` dependen de todos los datos.
- **Decisión**: Cada corrida de `silver` reemplaza todos los datasets de Silver y `quarantine/silver/`, y cada corrida de `gold` reemplaza los tres marts. El overwrite borra también las partitions viejas (`replace_all_partitions`), así no quedan fechas de corridas anteriores.
- **Alternativas descartadas**: Recálculo por fecha (cambiaría resultados de fechas viejas sin reescribirlas, porque los umbrales se mueven con los datos nuevos).
- **Consecuencias**: Con unos 43 mil eventos el recálculo tarda segundos. Volver a correr da los mismos datos, que es lo que muestra `idempotencia.csv`. Con años de datos habría que recalcular por fechas y fijar los umbrales por ventana: queda en el backlog.
- **Estado**: vigente

## D-16: Quarantine por capa con `_rule_id`, `_quarantine_reason` y `_raw`

- **Fecha**: 2026-11-13
- **Contexto**: El informe tenía una sola quarantine alimentada desde Bronze. Con reglas en dos capas, mezclar las rows no deja ver qué capa las rechazó, y para revisar un caso hace falta el registro original.
- **Decisión**: Bronze escribe en `quarantine/bronze/<dataset>/` y Silver en `quarantine/silver/<dataset>/`, las dos particionadas por `ingest_date`. Toda row en quarantine lleva `_rule_id` (la primera regla que viola, en el orden del catálogo), `_quarantine_reason` (texto legible), `_raw` (el `_corrupt_record` si no parseó, si no el JSON de todas sus columnas de entrada) y `_quarantine_ts`. Cada corrida de `silver` deja en `evidence/quality/<run_id>/` el conteo por regla y una muestra de hasta 20 rows por regla con casos.
- **Alternativas descartadas**: Una sola carpeta de quarantine (no se distingue la capa). Guardar solo el `rule_id` (no alcanza para reprocesar).
- **Consecuencias**: El cambio de carpeta obligó a borrar el lake una vez (`./infra/run.sh clean`). El gate (D-01) cuenta las rows de las dos quarantines.
- **Estado**: vigente

## D-17: Tipo de cambio de USD distinto de 1: se usa y se marca

- **Fecha**: 2026-11-13
- **Contexto**: En 160 facturas de `billing_monthly` con `currency = USD` el `exchange_rate_to_usd` no es 1. El informe lo dejó como decisión abierta (sección 9.3).
- **Decisión**: `revenue_usd = (subtotal − credits + taxes) × exchange_rate_to_usd` con el valor provisto, y esas facturas llevan el flag `FX_USD_NOT_ONE`.
- **Alternativas descartadas**: Forzar el tipo de cambio a 1 en las facturas en USD (corregir un dato sin saber cuál de los dos campos está mal). Mandarlas a quarantine (se pierde dos tercios de la facturación).
- **Consecuencias**: `revenue_by_org_month` (backlog) va a poder mostrar cuánto revenue viene de facturas marcadas.
- **Estado**: vigente

## D-18: Métricas de uso como columnas fijas

- **Fecha**: 2026-11-13
- **Contexto**: Los eventos traen una sola métrica por row (`metric` + `value`). El informe dejó abierto si guardarlas como columnas fijas o como un `map<text, double>`.
- **Decisión**: Silver arma una columna por métrica conocida (`requests`, `cpu_hours`, `storage_gb_hours`): el `value_numeric` va en la columna de su `metric` y las otras dos quedan en 0. Gold y Cassandra usan las mismas columnas.
- **Alternativas descartadas**: Un `map<text, double>` (más difícil de sumar en Spark y de consultar en Cassandra, y solo conviene si aparecen métricas nuevas seguido).
- **Consecuencias**: Una métrica nueva necesita una columna en Silver, en los marts y en los DDL. Una métrica desconocida con `unit` null va a quarantine (`SLV_UNIT_UNKNOWN`).
- **Estado**: vigente

## D-19: Maestros de Silver desde el último snapshot

- **Fecha**: 2026-11-13
- **Contexto**: Bronze guarda un snapshot de cada maestro por `ingest_date`. Silver tiene que elegir con qué snapshot trabajar y con qué versión de la org enriquecer cada evento.
- **Decisión**: Cada maestro de Silver sale del snapshot con el `ingest_date` más alto. `orgs_scd2` usa todos los snapshots y guarda una versión por cambio. Los eventos se enriquecen con el snapshot actual de `customers_orgs`, no con la versión de `orgs_scd2` vigente en la fecha del evento.
- **Alternativas descartadas**: Join temporal contra `orgs_scd2` (con un solo snapshot en el dataset da lo mismo y es más complejo).
- **Consecuencias**: Si una org cambia de plan, los eventos viejos muestran el plan nuevo. El join temporal queda en el backlog. El chequeo de idempotencia cuenta solo el último snapshot de cada maestro, porque correr otro día agrega uno nuevo a propósito.
- **Estado**: vigente

## D-20: Docker como entorno de referencia

- **Fecha**: 2026-11-08
- **Contexto**: El informe planteaba correr todo en Google Colab. La consigna pide un Quickstart reproducible desde un entorno limpio, y Colab cambia de versiones sin aviso y no tiene una Cassandra para probar.
- **Decisión**: El entorno de referencia es la imagen de [`infra/Dockerfile`](infra/Dockerfile) (Python 3.11, Java 17 y las versiones fijas de `config/requirements.txt`) con una Cassandra 4.1 local en `docker-compose.yml`. Todo se corre con `./infra/run.sh` o `.\infra\run.ps1`. Colab queda como alternativa contra AstraDB.
- **Alternativas descartadas**: Solo Colab (no reproducible, sin Cassandra local). Instalar todo en la máquina de cada uno (versiones distintas de Java y Spark).
- **Consecuencias**: Para el Quickstart alcanza con Docker. El mismo código corre en los dos entornos; lo que cambia son las variables de `config/.env`.
- **Estado**: vigente

## Decisiones abiertas

| Id | Tema | Contexto | Opción propuesta | Prioridad | Fecha límite |
| :--- | :--- | :--- | :--- | :--- | :--- |
| D-21 | Borrado de rows viejas en Cassandra | La carga es por upsert (D-09): si un Gold nuevo ya no tiene una key, su row queda en la tabla. | Hacer `TRUNCATE` de la tabla antes de cargar solo cuando cambian los datos de entrada, con un flag del step `serving`. | media | 2026-12-01 |
| D-22 | Recálculo por fechas afectadas | Hoy Silver y Gold se recalculan completos (D-15). Con más volumen deja de alcanzar. | Fijar los umbrales de anomalías por ventana y recalcular solo las fechas que trajeron eventos nuevos. | baja | 2026-12-07 |
| D-23 | Join temporal con `orgs_scd2` | Los eventos se enriquecen con el snapshot actual (D-19). | Join por `event_date` entre `valid_from` y `valid_to` cuando haya más de un snapshot de `customers_orgs`. | baja | 2026-12-07 |
| D-24 | Watermark de producción | El watermark de 61 días (D-11) existe por cómo vino el dataset. | Hacerlo configurable por variable de entorno, con 48 horas por defecto fuera del dataset de la cátedra. | baja | 2026-12-07 |
| D-25 | Retención automática | La retención del informe (Tabla 8) está documentada, pero nada se borra solo. | Un comando `retention` que borre partitions vencidas por zona, sin tocar Landing. | baja | 2026-12-07 |
