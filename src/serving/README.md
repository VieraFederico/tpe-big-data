# Serving

| Módulo | Qué hace |
| :--- | :--- |
| [`cassandra_writer.py`](cassandra_writer.py) | Step `serving`: gate de publicación, `CREATE TABLE IF NOT EXISTS` y carga por upsert de cada mart. |
| [`cassandra_client.py`](cassandra_client.py) | Sesión contra la Cassandra local o AstraDB según `SERVING_TARGET`. |
| [`astra_bundle.py`](astra_bundle.py) | Descarga del secure connect bundle de Astra a `secrets/`. |
| [`queries.py`](queries.py) | CLI de `./infra/run.sh query`: corre un `.cql` con parámetros y guarda `.txt` y `.csv`. |
| [`evidence_params.py`](evidence_params.py) | Elige la org y las fechas de las consultas 1 y 2 desde Gold. |
| [`check_connection.py`](check_connection.py) | `./infra/run.sh check`. |
| [`errors.py`](errors.py) | Errores del serving. |
| [`cql/schema/`](cql/schema/) | Un DDL por mart. |
| [`cql/queries/`](cql/queries/) | Consultas y conteos por partition. |

Diseño de las tablas y cómo cargar en Astra: [`docs/serving.md`](../../docs/serving.md).
