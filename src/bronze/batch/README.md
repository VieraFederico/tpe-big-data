# Bronze batch

Step `bronze_batch` ([`ingest.py`](ingest.py)). Lee los 7 CSV de Landing con su esquema explícito en modo PERMISSIVE, agrega las columnas técnicas, manda a `quarantine/bronze/<fuente>` lo que no parsea o no tiene clave natural, deja una row por clave (gana la última) y escribe `bronze/<fuente>` particionado por `ingest_date`. Al final escribe `_manifests/bronze_batch.json` con los conteos de cada fuente.

Volver a correr el mismo día reemplaza el snapshot de ese día; otro día agrega uno nuevo.
