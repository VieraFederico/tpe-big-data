# Documentación

| Documento | Qué tiene |
| :--- | :--- |
| [diccionario-datos.md](diccionario-datos.md) | Columnas, tipos, grain y partitions de los marts de Gold y de `_manifests/gold.json`, y columnas y primary keys de las tablas de Cassandra/AstraDB. |
| [serving.md](serving.md) | Tablas de Cassandra/AstraDB pensadas desde las consultas, carga con upsert y gate de publicación, cómo configurar Astra y correr las consultas, evidencia y límites. |
| [analitica.md](analitica.md) | Detección de anomalías de cost diario: método, resumen de la corrida y calibración del multiplier. |
| [mapreduce.md](mapreduce.md) | El mart diario como job MapReduce (map → shuffle → reduce) y cómo se comprueba que da lo mismo que la versión con DataFrames. |
