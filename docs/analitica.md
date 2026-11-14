# Analítica: anomalías de cost diario

El componente de analítica detecta días con un cost anormalmente alto para una organización y un service. Es detección estadística (percentiles y MAD), sin modelos entrenados. El resultado es el mart `gold/cost_anomaly_mart` (columnas en el [diccionario de datos](diccionario-datos.md#goldcost_anomaly_mart)) y un resumen para calibrar el umbral.

Código: [`src/gold/anomalies.py`](../src/gold/anomalies.py).

## Método

Se trabaja sobre el mart diario `gold/org_daily_usage_by_service`: cada row es el `daily_cost_usd` de una org, un día y un service. Las estadísticas se calculan por service, juntando todas las orgs.

- **p99 × multiplier (decide qué entra al mart).** `p99_usd` es el percentil 99 exacto de `daily_cost_usd` entre todas las rows del service. Una row es anomalía si `p99_usd > 0` y `daily_cost_usd > multiplier × p99_usd`. Ese producto es `threshold_usd`, y `anomaly_score = daily_cost_usd / p99_usd` dice cuántas veces el p99 costó ese día. Si el p99 de un service es 0, ese service no tiene anomalías.
- **robust z (segunda opinión, no decide).** `robust_z = 0.6745 × (daily_cost_usd − mediana) / MAD`, con la mediana y el MAD (mediana de los desvíos absolutos a la mediana) del service. Es null si el MAD es 0 (todos los días cuestan lo mismo). Se guarda en el mart y se cuenta en el resumen para comparar, pero no cambia qué rows entran.

El multiplier sale de la variable de entorno `ANOMALY_P99_MULTIPLIER` (default `3`, tiene que ser un número mayor a 0; se configura en `config/.env`). Cada row del mart dice con qué se detectó en `detection_method`: `p99_x3`, `p99_x2.5`, etc.

Esto es aparte de la marca `is_cost_anomaly` de Silver, que se calcula por event (no por día) con un 3 × p99 fijo y no depende de `ANOMALY_P99_MULTIPLIER`. En el mart diario se resume como `cost_anomaly_events`.

## Resumen de la corrida

Cada corrida de `gold` que pasa el gate deja `evidence/analytics/<run_id>/anomaly_summary.csv`, una row por service:

- `days_rows`: rows del service en el mart diario.
- `p99_usd`, `multiplier`, `threshold_usd`: el umbral usado.
- `anomalies_p99`: rows que entraron al mart (coincide con el mart).
- `anomalies_robust_z`: rows con `robust_z > 3.5` (solo para arriba; los null no cuentan).

Corrida `20261114T183817Z-e844b8` sobre el dataset completo (11050 rows en el mart diario, 80 orgs, del 2025-07-03 al 2025-08-31), con `multiplier = 3`:

| service | days_rows | p99_usd | threshold_usd | anomalies_p99 | anomalies_robust_z |
| :--- | ---: | ---: | ---: | ---: | ---: |
| analytics | 1289 | 47.46 | 142.37 | 7 | 25 |
| compute | 2839 | 87.14 | 261.41 | 3 | 61 |
| database | 1919 | 42.83 | 128.50 | 8 | 35 |
| genai | 1131 | 79.80 | 239.39 | 4 | 28 |
| networking | 1955 | 7.05 | 21.14 | 10 | 53 |
| storage | 1917 | 17.14 | 51.42 | 5 | 45 |
| **total** | 11050 | | | 37 | 247 |

Los 37 días del mart son de 31 orgs distintas, con `anomaly_score` entre 3.04 y 5.42. El más alto es `org_pht0hl9x` en `analytics` el 2025-08-25: 257.04 USD contra un p99 de 47.46.

## Cuántos casos detecta cada método

- **p99 × 3**: 37 rows, el 0.33% del mart diario.
- **robust z > 3.5**: 247 rows, el 2.2%. Los costs diarios tienen una cola larga a la derecha y el MAD es chico comparado con esa cola, así que el robust z marca muchos días que son altos pero no raros.
- Todo lo que detecta p99 × 3 también lo detecta el robust z: el `robust_z` más bajo del mart es 18.1, muy por encima de 3.5. O sea que el p99 × 3 se queda con los casos más extremos de los que ve el robust z.

Para elegir el multiplier se contó cuántas rows quedarían con otros valores (mismo p99, misma corrida):

| multiplier | rows | analytics | compute | database | genai | networking | storage |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 2 | 73 | 8 | 25 | 10 | 5 | 12 | 13 |
| 2.5 | 61 | 8 | 14 | 10 | 5 | 12 | 12 |
| **3** | **37** | 7 | 3 | 8 | 4 | 10 | 5 |
| 4 | 7 | 3 | 0 | 0 | 2 | 1 | 1 |
| 5 | 1 | 1 | 0 | 0 | 0 | 0 | 0 |

## Multiplier elegido: 3

Se deja el default `3`:

- Da una cantidad de casos que alguien puede revisar a mano (37 en dos meses, menos de uno por día) y todos son más de 3 veces el p99 de su service.
- Bajarlo a 2.5 casi duplica los casos y la mayoría de los nuevos son de `compute` (de 3 a 14), que es el service con más rows y una cola más pesada: son días caros pero frecuentes, no picos aislados.
- Subirlo a 4 deja solo 7 casos y `compute` y `database` se quedan sin ninguno, así que se pierden picos claros (todos los de 3 tienen `robust_z` > 18).

La calibración no sugiere cambiar el default. Si con datos nuevos el resumen cambia mucho, se vuelve a mirar `anomaly_summary.csv` y se ajusta `ANOMALY_P99_MULTIPLIER` sin tocar código.
