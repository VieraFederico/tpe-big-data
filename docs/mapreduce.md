# MapReduce del mart diario

El mart `gold/org_daily_usage_by_service` responde la primera consulta de FinOps: cost y uso por organización, día y service. Sale de sumar los events de `silver/usage_events` agrupados por `(org_id, usage_date, service)`, que es justo el caso típico de MapReduce: un map que le pone una key a cada event, un shuffle que junta los events de la misma key y un reduce que los suma.

En el pipeline el mart se arma con DataFrames ([`org_daily_usage_by_service`](../src/gold/marts.py)). Además está escrito a mano como job MapReduce sobre RDDs en [`src/gold/mapreduce.py`](../src/gold/mapreduce.py), con una función por fase, para mostrar la lógica y comprobar que las dos dan lo mismo.

## Fases

**Map** ([`map_event`](../src/gold/mapreduce.py)). Cada event emite un par `(key, vector)`:

- key: `(org_id, usage_date, service)`, con `usage_date = event_date`.
- vector: `(cost_usd_increment, requests, cpu_hours, storage_gb_hours, carbon_kg, genai_tokens, 1, is_cost_anomaly)`. Las métricas van tal cual, con sus null (los events v1 no traen `carbon_kg` ni `genai_tokens`). El `1` sirve para contar events y `is_cost_anomaly` va como 1/0.

**Shuffle**. Lo hace `reduceByKey`: manda todos los vectores de una misma key a la misma partition. Antes de mandarlos ya los va sumando dentro de cada partition (combiner), así viaja un vector por key y partition en vez de un vector por event.

**Reduce** ([`reduce_vectors`](../src/gold/mapreduce.py)). Suma dos vectores componente a componente. Si un lado es null queda el otro, y si los dos son null queda null. Es lo mismo que hace `sum` en Spark SQL: un grupo donde todos los events tienen `carbon_kg` null da `carbon_kg` null, y si al menos uno tiene valor da la suma de los que no son null. Como la suma es asociativa y conmutativa, no importa en qué orden se combinen los vectores.

**Salida** ([`org_daily_usage_mapreduce`](../src/gold/mapreduce.py)). Hace `rdd.map(map_event).reduceByKey(reduce_vectors)` y arma un DataFrame con las mismas columnas y tipos que el mart (ver [diccionario de datos](diccionario-datos.md#goldorg_daily_usage_by_service)), sin `run_id`. En el mart la suma de `cost_usd_increment` se llama `daily_cost_usd`.

## Por qué el pipeline usa la versión con DataFrames

Las dos hacen el mismo trabajo, pero con DataFrames Spark ve la consulta entera y Catalyst la optimiza: lee de los parquet solo las columnas que usa, genera código para la agregación y todo corre en la JVM sobre el formato binario de Spark. Con RDDs cada row se pasa a un worker de Python, se arma una tupla y se vuelve a la JVM, y Spark no sabe qué hace la función, así que no puede optimizar nada. Además la versión con DataFrames se lee más fácil y reusa las constantes de columnas del resto del código.

La versión con RDDs queda como referencia y como check, no se usa para escribir Gold.

## Cómo se prueba que dan lo mismo

- Test: [`tests/test_gold_mapreduce.py`](../tests/test_gold_mapreduce.py) arma un Silver con 50 events variados (3 orgs, 4 services, 5 días, events v1 y v2, nulls, costs negativos chicos) y compara las dos implementaciones: mismas keys y cada métrica con diferencia menor a `1e-9`. También chequea que tengan las mismas columnas y tipos.
- Sobre el dataset completo: `./infra/run.sh mapreduce-check` (o `.\infra\run.ps1 mapreduce-check`) lee `silver/usage_events`, corre las dos versiones, hace un full join por key y guarda el reporte en `evidence/analytics/<run_id>/mapreduce_check.txt`. El reporte tiene las rows de cada una, cuántas keys aparecen en una sola (con hasta 20 de ejemplo) y la diferencia absoluta máxima por métrica. Si una métrica es null de un lado y tiene valor del otro, cuenta como diferencia `inf`.

Resultado sobre el dataset del repo (Silver de la corrida `20261114T183817Z-e844b8`):

```text
rows_dataframe: 11050
rows_mapreduce: 11050
keys_only_dataframe: 0
keys_only_mapreduce: 0
max_abs_diff_daily_cost_usd: 0.0
max_abs_diff_requests: 0.0
max_abs_diff_cpu_hours: 0.0
max_abs_diff_storage_gb_hours: 0.0
max_abs_diff_carbon_kg: 0.0
max_abs_diff_genai_tokens: 0.0
max_abs_diff_event_count: 0.0
max_abs_diff_cost_anomaly_events: 0.0
```

Las sumas de doubles pueden variar en los últimos decimales según el orden en que se combinen, por eso el criterio es una diferencia máxima menor a `1e-6`; en esta corrida dio exactamente 0.
