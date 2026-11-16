# Serving: Cassandra y AstraDB

El step `serving` publica los tres marts de Gold en el keyspace `cloud_analytics`, en una Cassandra local (Docker) o en AstraDB, para que FinOps los consulte con baja latencia. Cada mart va a una tabla con su mismo nombre y sus mismas columnas. Las consultas son archivos `.cql` con parámetros que se corren con `./infra/run.sh query` y dejan evidencia en `.txt` y `.csv`.

| Qué | Dónde |
| :--- | :--- |
| DDL de las tablas | [`src/serving/cql/schema/`](../src/serving/cql/schema/) |
| Consultas | [`src/serving/cql/queries/`](../src/serving/cql/queries/) |
| Step `serving` (gate y carga) | [`src/serving/cassandra_writer.py`](../src/serving/cassandra_writer.py) |
| Conexión a Cassandra/Astra | [`src/serving/cassandra_client.py`](../src/serving/cassandra_client.py) |
| CLI de consultas | [`src/serving/queries.py`](../src/serving/queries.py) |
| Parámetros de la evidencia | [`src/serving/evidence_params.py`](../src/serving/evidence_params.py) |
| Columnas y tipos de cada tabla | [diccionario de datos](diccionario-datos.md#cassandraastradb) |

## Diseño query-first

En Cassandra la tabla se arma a partir de la consulta que tiene que responder, no al revés. Una consulta solo es eficiente si fija la partition key completa (así lee una sola partition, en un solo nodo) y, dentro de la partition, filtra u ordena por las clustering columns en el orden en que están guardadas. Todo lo demás necesita `ALLOW FILTERING`, que recorre la tabla entera. Ninguna consulta del proyecto lo usa: cada una fija la partition key completa de su tabla.

| Tabla | Consulta que responde | Partition key | Clustering |
| :--- | :--- | :--- | :--- |
| `org_daily_usage_by_service` | Consulta 1: uso y cost diario por service de una org en un rango de fechas | `org_id` | `usage_date DESC, service ASC` |
| `top_services_by_org_14d` | Consulta 2: top-N de services por cost de los últimos 14 días de una org a una fecha de corte | `(org_id, as_of_date)` | `rank ASC` |
| `cost_anomaly_mart` | Días con cost anómalo de una org en un rango de fechas | `org_id` | `usage_date DESC, service ASC` |

- **`org_daily_usage_by_service`.** La consulta 1 siempre es de una org, así que `org_id` es la partition key: la carga se reparte entre las 80 orgs y la consulta lee una partition. `usage_date` como primera clustering column permite el rango `usage_date >= from AND usage_date <= to` sobre datos ya ordenados, y el orden descendente devuelve primero los días más recientes. `service` completa la key (hay una row por org, día y service) y ordena los services de cada día. Con el dataset del repo una partition tiene como mucho 60 días × 6 services = 360 rows.
- **`top_services_by_org_14d`.** La consulta 2 siempre fija la org y la fecha de corte, así que las dos van en la partition key: cada consulta lee una partition de como mucho 6 rows (un service por row). `rank` es la clustering column en orden ascendente, así que `LIMIT n` devuelve directamente el top-N sin ordenar nada en el cliente. `service` no hace falta en la key porque `rank` ya es único dentro de cada `(org_id, as_of_date)` (sale de un `row_number()`, ver `DECISIONS.md`, D-02).
- **`cost_anomaly_mart`.** Tiene la misma key que el mart diario, del que sale: una partition por org con los días más recientes primero. La columna de fecha es `usage_date`, no `date` como en la tabla de claves del informe (`DECISIONS.md`, D-03).

Las decisiones están en [`DECISIONS.md`](../DECISIONS.md), D-06 a D-09.

## Cómo se carga

`./infra/run.sh run` (o `run ...,serving`) corre el step `serving` después de `gold`. El step hace, en este orden:

1. **Gate de publicación.** Lee `_manifests/gold.json`. Si no existe o su `status` no es `ok` (por ejemplo `gate_failed`, cuando la reconciliation no cerró, ver `DECISIONS.md`, D-01), termina con `GoldNotPublishableError` antes de abrir la conexión: no crea ni escribe ninguna tabla. El mensaje dice qué estado encontró (`missing` si falta el archivo). El gate solo mira el `status`; no compara `run_id` ni cantidades.
2. **Tablas.** Abre una sesión y corre cada DDL de `src/serving/cql/schema/`. Son `CREATE TABLE IF NOT EXISTS`, así que una tabla que ya existe queda como está.
3. **Carga completa.** Para cada DDL `<mart>.cql` lee `gold/<mart>` entero y lo escribe en la tabla `<mart>`. Cada partition de Spark escribe desde su worker con un `INSERT` preparado y requests concurrentes (`CASSANDRA_WRITE_CONCURRENCY`, default 32). Se insertan todas las columnas del mart, incluido `run_id`, así que la tabla tiene que tener exactamente esas columnas.

Detalles de la carga:

- **Upsert.** En Cassandra un `INSERT` con una primary key que ya existe reemplaza esa row. Por eso volver a correr `serving` sobre el mismo Gold no duplica nada: la cantidad de rows de cada partition queda igual.
- **Nulls como `UNSET`.** Los valores null (por ejemplo `carbon_kg` y `genai_tokens` de los events v1) se mandan como `UNSET`: la columna no se escribe. Así no se generan tombstones, que en Cassandra ocupan lugar y hacen más lentas las lecturas.
- **Errores.** Si Cassandra rechaza alguna row, el step termina con `WriteFailedError` y el log muestra el primer error de cada partition.
- **Log.** Cada tabla deja una línea `Carga <tabla>: N filas escritas, M fallidas en Xs` en `evidence/logs/pipeline_<run_id>.log`, y el step termina con `== serving: ok en Xs`. N tiene que coincidir con las rows de ese mart en `marts` de `_manifests/gold.json`.

## Configurar el destino

El destino se elige con `SERVING_TARGET` en `config/.env` (se crea desde `config/.env.example` la primera vez que se usa `./infra/run.sh`):

| Variable | Para qué | Notas |
| :--- | :--- | :--- |
| `SERVING_TARGET` | `local` (Cassandra en Docker) o `astra` | Default `local`. |
| `CASSANDRA_KEYSPACE` | Keyspace de las tablas | `cloud_analytics`. |
| `CASSANDRA_HOST`, `CASSANDRA_PORT` | Cassandra local | En Docker `CASSANDRA_HOST` queda en `cassandra`, el servicio de compose. |
| `ASTRA_DB_APPLICATION_TOKEN` | Token de aplicación de Astra | Secreto. |
| `ASTRA_DB_API_ENDPOINT` | `https://<db_id>-<region>.apps.astra.datastax.com` | Secreto. Con el endpoint y el token, el secure connect bundle se descarga solo a `secrets/` en la primera conexión. |
| `ASTRA_DB_SECURE_BUNDLE_PATH` | Bundle ya descargado | Opcional. |
| `CASSANDRA_WRITE_CONCURRENCY` | Requests en vuelo por partition de Spark | Opcional, default 32. |

- Los únicos secretos son `ASTRA_DB_APPLICATION_TOKEN` y `ASTRA_DB_API_ENDPOINT`. `config/.env` y `secrets/` están ignorados por git; el token no va en ningún archivo versionado.
- Dentro de Docker, `infra/docker-compose.yml` fija `DATALAKE_PATH=/app/data/datalake` y `CASSANDRA_HOST=cassandra`. Esas variables le ganan al `config/.env`, y el código carga el `.env` sin pisar las que ya están definidas, así que lo que diga el `.env` para esas dos no cambia nada en Docker.
- En Colab no hay `config/.env`: el notebook [`colab_pipeline.ipynb`](../notebooks/colab_pipeline.ipynb) lee el token y el endpoint de Colab Secrets y define el resto (`SERVING_TARGET=astra`, `CASSANDRA_KEYSPACE`, `DATALAKE_PATH`) como variables de entorno.
- Con `SERVING_TARGET=local`, `./infra/run.sh` levanta la Cassandra local antes de `check`, `query` y `run` con `serving`, y el keyspace se crea solo. Con `astra` no levanta nada.

### Crear el keyspace en AstraDB

Astra no permite `CREATE KEYSPACE` por CQL, así que el pipeline no lo puede crear. Antes de la primera carga:

1. En la consola de Astra: *Databases → (la base) → Data Explorer → Create Keyspace*, con el nombre `cloud_analytics`.
2. Generar un token con permiso de escritura sobre la base (*Settings → Tokens*) y copiarlo en `ASTRA_DB_APPLICATION_TOKEN`.
3. Copiar el API endpoint de la base en `ASTRA_DB_API_ENDPOINT` y poner `SERVING_TARGET=astra`.

Las tablas no hace falta crearlas: las crea el step `serving` desde los DDL. Si el keyspace falta, la conexión falla con `El keyspace 'cloud_analytics' no existe en astra`. Más detalle en [`infra/README.md`](../infra/README.md#requisitos-para-astradb).

## Cargar y consultar en AstraDB

Con `SERVING_TARGET=astra` en `config/.env` y el keyspace creado, desde la raíz del repo:

```bash
# 1. Probar la conexión: loguea la versión de Cassandra y las tablas del keyspace
./infra/run.sh check

# 2. Pipeline completo hasta serving (equivale a ./infra/run.sh run)
./infra/run.sh run bronze_batch,bronze_stream,silver,gold,serving

# 3. Parámetros de las consultas, calculados desde Gold
./infra/run.sh shell
python -m src.serving.evidence_params      # adentro del contenedor; después, exit

# 4. Consultas 1 y 2 con esos parámetros, guardando la evidencia
./infra/run.sh query src/serving/cql/queries/q1_daily_usage_by_range.cql \
  -p org_id=<org_id> -p from_date=2025-08-18 -p to_date=2025-08-31 \
  --evidence-dir evidence/entrega-2/queries
./infra/run.sh query src/serving/cql/queries/q2_top_services_14d.cql \
  -p org_id=<org_id> -p as_of_date=2025-08-31 -p n:int=5 \
  --evidence-dir evidence/entrega-2/queries
```

En PowerShell los mismos comandos van con `.\infra\run.ps1`.

Después del paso 2, revisar en `evidence/logs/pipeline_<run_id>.log` las tres líneas `Carga <tabla>: N filas escritas, 0 fallidas` y comparar cada N con `_manifests/gold.json`.

### Parámetros de la evidencia

`python -m src.serving.evidence_params` imprime un JSON con los parámetros, por ejemplo:

```json
{"org_id": "org_xxxxxxxx", "from_date": "2025-08-18", "to_date": "2025-08-31", "as_of_date": "2025-08-31", "n": 5}
```

- La fecha de corte es 2025-08-31, el último día del dataset: es `as_of_date` de la consulta 2 y `to_date` de la consulta 1.
- `from_date` es 2025-08-18, así el rango de la consulta 1 son los mismos 14 días que la ventana del top.
- `org_id` es la org con mayor suma de `cost_14d_usd` en `top_services_by_org_14d` para 2025-08-31; si dos empatan, la de `org_id` menor. Así las dos consultas muestran la org que más gasta y devuelven rows seguro.
- `n` es 5.

Las fechas salen como texto ISO, listas para pasar con `-p`. Si Gold no tiene rows de `top_services_by_org_14d` para 2025-08-31, el comando termina con código 1 y un mensaje que pide correr el pipeline hasta `gold`. El JSON va solo a stdout; los logs van a stderr.

### Parámetros en la línea de comandos

- Cada parámetro va con `-p clave=valor` y llega a la consulta como texto. Para otro tipo: `-p clave:int=valor` o `-p clave:float=valor`.
- `n` tiene que ir como `-p n:int=5`. Con `-p n=5` la consulta queda `LIMIT '5'` y Cassandra la rechaza.
- Las fechas van como texto ISO (`2025-08-18`); Cassandra las convierte a `date`.

## Consultas

Todas usan parámetros con nombre (`%(org_id)s`) y fijan la partition key completa de su tabla. Las de rango y top tienen un `ORDER BY` explícito igual al orden de clustering de la tabla.

| Archivo | Parámetros | Devuelve |
| :--- | :--- | :--- |
| `q1_daily_usage_by_range.cql` | `org_id`, `from_date`, `to_date` | `usage_date`, `service`, `daily_cost_usd`, `requests`, `cpu_hours`, `storage_gb_hours`, `genai_tokens`, `carbon_kg` de la org con `from_date <= usage_date <= to_date`, por `usage_date` descendente y `service` ascendente. |
| `q2_top_services_14d.cql` | `org_id`, `as_of_date`, `n` (int) | Hasta `n` rows con `rank`, `service`, `cost_14d_usd`, `requests_14d`, `window_start_date`, por `rank` ascendente. |
| `q_cost_anomalies_by_org.cql` | `org_id`, `from_date`, `to_date` | `usage_date`, `service`, `daily_cost_usd`, `threshold_usd`, `anomaly_score` de las anomalías de la org en el rango, mismo orden que la consulta 1. |
| `count_org_daily_usage_by_service.cql` | `org_id` | Una row con `row_count`: rows de la org. |
| `count_top_services_by_org_14d.cql` | `org_id`, `as_of_date` | Una row con `row_count`: rows de la org a esa fecha de corte. |
| `count_cost_anomaly_mart.cql` | `org_id` | Una row con `row_count`: anomalías de la org. |

La consulta de anomalías se corre igual que la 1, con los mismos parámetros. Una org sin anomalías en el rango devuelve 0 rows.

### Comprobar que recargar no duplica

1. Contar las rows de una partition de cada tabla:

   ```bash
   ./infra/run.sh query src/serving/cql/queries/count_org_daily_usage_by_service.cql -p org_id=<org_id>
   ./infra/run.sh query src/serving/cql/queries/count_top_services_by_org_14d.cql \
     -p org_id=<org_id> -p as_of_date=2025-08-31
   ./infra/run.sh query src/serving/cql/queries/count_cost_anomaly_mart.cql -p org_id=<org_id>
   ```

2. Volver a cargar el mismo Gold: `./infra/run.sh run serving`.
3. Repetir los conteos: tienen que dar lo mismo que antes, que es la cantidad de rows de esa partition en Gold.

Para no pisar la evidencia del primer conteo, se puede agregar `--name <nombre>` o un `--evidence-dir` distinto.

## Evidencia

`./infra/run.sh query` imprime el resultado y guarda dos archivos en `--evidence-dir` (default `evidence/`), con el nombre del `.cql` (o el de `--name`):

- `<nombre>.txt`: `Consulta`, `Ejecutada` (UTC), `Destino` (`astra / keyspace cloud_analytics` o `local / ...`), `Parámetros`, `Filas` (el total), `Tiempo`, el CQL tal cual (con los comentarios del archivo) y las primeras 50 rows.
- `<nombre>.csv`: encabezado y rows, hasta `--max-rows` (default 1000).

`Tiempo` lo mide el cliente desde que manda la consulta hasta que recorre todas las páginas del resultado, así que incluye la red entre el contenedor y Astra.

Logs de cada comando, en `evidence/logs/`:

| Comando | Log |
| :--- | :--- |
| `./infra/run.sh run ...` | `pipeline_<run_id>.log` (líneas `Carga <tabla>: ...` y `== serving: ok en Xs`) |
| `./infra/run.sh query ...` | `queries_<timestamp>.log` |
| `python -m src.serving.evidence_params` | `evidence_params_<timestamp>.log` |
| `./infra/run.sh check` | `check_connection_<timestamp>.log` |

El código no escribe el token en la evidencia ni en los logs.

### Evidencia de la entrega 2

La carga y las consultas 1 y 2 se corrieron contra AstraDB el 2026-11-14 con la secuencia de [Cargar y consultar en AstraDB](#cargar-y-consultar-en-astradb). El índice de los archivos está en [`evidence/entrega-2/README.md`](../evidence/entrega-2/README.md).

**Carga.** Corrida `20261114T221819Z-ad1d25`, pipeline completo con `serving=astra`. Log: [`evidence/logs/pipeline_20261114T221819Z-ad1d25.log`](../evidence/logs/pipeline_20261114T221819Z-ad1d25.log).

| Tabla | Rows escritas | Fallidas | Tiempo |
| :--- | ---: | ---: | ---: |
| `org_daily_usage_by_service` | 11050 | 0 | 21.75s |
| `top_services_by_org_14d` | 15599 | 0 | 14.43s |
| `cost_anomaly_mart` | 37 | 0 | 8.13s |

Las rows coinciden con `marts` de `_manifests/gold.json` de esa corrida. El step `serving` entero (gate, DDL y las tres cargas) tardó 59.3s.

**Parámetros.** `evidence_params` eligió `org_chj755nf`, la org con más cost de 14 días al 2025-08-31 (1039.98 USD sumando sus services; log [`evidence/logs/evidence_params_20261114T223439Z.log`](../evidence/logs/evidence_params_20261114T223439Z.log)). El resto: `from_date = 2025-08-18`, `to_date = as_of_date = 2025-08-31`, `n = 5`.

**Consultas.** Las dos leen de `astra / keyspace cloud_analytics`:

| Consulta | Parámetros | Rows | Tiempo | Evidencia |
| :--- | :--- | ---: | ---: | :--- |
| 1. Uso diario por rango | `org_id=org_chj755nf`, `from_date=2025-08-18`, `to_date=2025-08-31` | 59 | 0.194s | [`q1_daily_usage_by_range.txt`](../evidence/entrega-2/queries/q1_daily_usage_by_range.txt), [`.csv`](../evidence/entrega-2/queries/q1_daily_usage_by_range.csv) |
| 2. Top-N de 14 días | `org_id=org_chj755nf`, `as_of_date=2025-08-31`, `n=5` | 5 | 0.191s | [`q2_top_services_14d.txt`](../evidence/entrega-2/queries/q2_top_services_14d.txt), [`.csv`](../evidence/entrega-2/queries/q2_top_services_14d.csv) |

La consulta 2 devuelve, de `rank` 1 a 5: `genai`, `compute`, `analytics`, `storage` y `networking`, todos con la ventana desde el 2025-08-18.

## Límites

- **Las rows viejas no se borran.** La carga no borra nada: si un Gold nuevo ya no tiene una key que estaba en la tabla, esa row queda. Con Landing inmutable y Gold recalculado completo en cada corrida no pasa, pero si cambian los datos de entrada hay que vaciar la tabla (`TRUNCATE <tabla>` desde la consola CQL) antes de volver a cargar.
- **Un null no borra un valor anterior.** Como los null van como `UNSET`, si una row ya tenía un valor en una columna y el Gold nuevo trae null, queda el valor viejo. Mismo remedio que el punto anterior.
- **Cambiar un DDL no cambia la tabla.** `CREATE TABLE IF NOT EXISTS` no toca una tabla existente. Si cambian las columnas o la key, hay que borrar la tabla (`DROP TABLE <tabla>`) para que `serving` la cree de nuevo.
- **Solo consultas por partition.** Cada tabla responde su consulta y nada más: por ejemplo, no se puede pedir el top de todas las fechas de una org ni las anomalías de un día para todas las orgs sin `ALLOW FILTERING`. Una consulta nueva de ese tipo necesita su propia tabla.
- **Ventanas cortas al principio.** En las primeras fechas de corte la ventana de 14 días tiene menos días con datos; `window_start_date` lo muestra (ver el [diccionario de datos](diccionario-datos.md#goldtop_services_by_org_14d)).
- **Tamaño de partition.** La partition de una org en `org_daily_usage_by_service` crece con los días. Con dos meses son 360 rows como mucho; con años de datos convendría partir por org y mes.
- Las consultas 3 a 5 del informe y sus tablas todavía no están.

## Tests

Sin Cassandra (corren siempre con `./infra/run.sh test`):

- [`tests/test_cql_schema.py`](../tests/test_cql_schema.py): un DDL por mart, con el nombre de su archivo, las columnas y tipos del mart (comparados con `MART_SCHEMAS` de [`src/gold/build.py`](../src/gold/build.py)) y la primary key y el orden de la tabla de arriba.
- [`tests/test_cql_queries.py`](../tests/test_cql_queries.py): las consultas esperadas y sus parámetros, sin `ALLOW FILTERING`, con la partition key completa en el `WHERE` (leída del DDL) y sin `%` sueltos.
- [`tests/test_serving_gate.py`](../tests/test_serving_gate.py): el gate no abre la sesión sin `gold.json` o con un `status` que no es `ok`.
- [`tests/test_evidence_params.py`](../tests/test_evidence_params.py): elección de la org, desempate y código 1 sin datos.

Con Cassandra local (marker `integration`): [`tests/test_serving_integration.py`](../tests/test_serving_integration.py) prueba el writer y [`tests/test_serving_queries_integration.py`](../tests/test_serving_queries_integration.py) publica un Gold de prueba con el step `serving` y corre cada consulta con el mismo código que el CLI: carga completa, recarga sin duplicados, consultas 1 y 2 y anomalías. Para correrlos:

```bash
docker compose -f infra/docker-compose.yml --profile local up -d --wait cassandra
./infra/run.sh test -m integration -v     # solo los de Cassandra
./infra/run.sh test                       # toda la suite
```

Sin Cassandra se saltean. Siempre van contra la Cassandra local, en el keyspace `tpe_integration_tests`, aunque `config/.env` diga `SERVING_TARGET=astra`: no tocan `cloud_analytics` ni Astra. Al empezar, `test_serving_queries_integration.py` borra las tres tablas de ese keyspace para que se creen de nuevo desde los DDL actuales, y cada test usa partitions propias, así que el orden no importa.

## Cambiar una tabla o agregar una consulta

- El nombre del DDL es el de la tabla y el del mart: `schema/<mart>.cql` crea `<mart>` y se carga desde `gold/<mart>`. `serving` carga todos los DDL de la carpeta, así que cada uno necesita su mart en Gold.
- Si cambia una columna de un mart, primero se actualiza `MART_SCHEMAS` en `src/gold/build.py` (un test de Gold compara lo escrito con ese schema) y después el DDL (`test_cql_schema.py` los compara). En una base que ya tiene la tabla, borrarla antes de cargar.
- Un archivo de consulta tiene una sola sentencia, comentarios `--` en español al principio (qué responde, qué partition lee y una línea de uso) y parámetros solo como `%(nombre)s`. El texto entero, comentarios incluidos, pasa por el formateo con `%` del driver: un `%` suelto en cualquier lado rompe la consulta y un `%(nombre)s` en un comentario también se reemplaza. Los comentarios llegan a Cassandra y aparecen en la evidencia.
- Al agregar o renombrar una consulta o un parámetro, actualizar `EXPECTED_PARAMS` en `tests/test_cql_queries.py`.
- Al agregar un mart, sumar sus rows de prueba a `LOAD_GOLD` y `QUERY_GOLD` en `tests/test_serving_queries_integration.py`, porque `serving` lee todos los marts.
- Cada tabla necesita también su `queries/count_<tabla>.cql`, que cuenta una partition: el chequeo de idempotencia ([`src/evidence/idempotence.py`](../src/evidence/idempotence.py)) lo corre para cada partition del mart en Gold, sin escanear la tabla. Toma `org_id` como partition key; si la tabla tiene otra, agregarla en `SERVING_PARTITION_COLUMNS` (como `top_services_by_org_14d`, que usa `(org_id, as_of_date)` y solo cuenta el 2025-08-31).
