# Calidad de datos

Cada regla tiene una acción según qué tan grave es el problema: si la row no se puede usar va a **quarantine**; si se puede corregir de forma confiable se **corrige y se marca** en `dq_flags`; si es válida pero llamativa queda con un **flag**. Las reglas están declaradas en [`src/quality/catalog.py`](../src/quality/catalog.py) y se aplican con [`src/quality/rules.py`](../src/quality/rules.py) (quarantine) y [`src/quality/flags.py`](../src/quality/flags.py) (flags).

- Una row en quarantine queda con la **primera** regla que viola, en el orden de las tablas de abajo.
- `dq_flags` es un `array<string>` ordenado y sin repetidos con todos los flags que aplican; vacío si no aplica ninguno.
- Un valor null nunca está "fuera de rango": las reglas de rango solo miran valores presentes.
- Cada corrida de `silver` deja en `evidence/quality/<run_id>/` el archivo `rule_counts.csv` (una row por regla, aunque afecte 0 rows) y `samples/<dataset>__<rule_id>.csv` con hasta 20 rows de cada regla que tuvo casos. Las rows rechazadas completas están en `quarantine/` (ver [data lake](data-lake.md)).

Las cantidades de abajo son de la corrida `20261114T221819Z-ad1d25` sobre el dataset completo ([`rule_counts.csv`](../evidence/quality/20261114T221819Z-ad1d25/rule_counts.csv)). Las de la corrida de entrega van a `evidence/entrega-2/quality/` (pendiente de corrida).

## Bronze

Las aplican `bronze_batch` (a los 7 CSV) y `bronze_stream` (a los eventos). Lo rechazado va a `quarantine/bronze/<dataset>/`. Los conteos están en `_manifests/bronze_batch.json` (`quarantine` por fuente) y en el log del stream (`quarantine_total`).

| `rule_id` | Dataset | Condición | Acción | Rows |
| :--- | :--- | :--- | :--- | ---: |
| `BRZ_PARSE` | todos | La row no respeta el esquema explícito (`_corrupt_record` no null). | quarantine | 0 |
| `BRZ_NULL_KEY` | todos | Alguna columna de la clave natural es null. | quarantine | 0 |
| `BRZ_BAD_TS` | `usage_events` | `timestamp` null o que no se puede parsear. | quarantine | 0 |

Además, `bronze_batch` deja una row por clave natural (gana la del último archivo y, dentro de él, la última row) y `bronze_stream` descarta eventos repetidos por `event_id` dentro del watermark (D-11). Ninguna de las dos cosas manda rows a quarantine; se cuentan como duplicados.

## Silver: eventos de uso

| `rule_id` | Condición | Acción | Rows | Muestra |
| :--- | :--- | :--- | ---: | :--- |
| `SLV_NULL_EVENT_ID` | `event_id` null | quarantine | 0 | |
| `SLV_NULL_DIMENSION` | `org_id`, `service` o `metric` null | quarantine | 0 | |
| `SLV_SCHEMA_VERSION` | `schema_version` no es 1 ni 2; v1 con `carbon_kg` o `genai_tokens`; v2 sin `carbon_kg`; `genai_tokens` en un service que no es `genai` | quarantine | 0 | |
| `SLV_VALUE_NOT_NUMERIC` | `value` presente que no se puede convertir a número | quarantine | 0 | |
| `SLV_UNIT_UNKNOWN` | `value` presente, `unit` null y `metric` sin unidad conocida | quarantine | 0 | |
| `SLV_COST_INVALID` | `cost_usd_increment` null o menor a -0.01 (D-13) | quarantine | 211 | [muestra](../evidence/quality/20261114T221819Z-ad1d25/samples/usage_events__SLV_COST_INVALID.csv) |
| `SLV_DUPLICATE_EVENT_ID` | `event_id` repetido: queda el de menor `ingest_ts` (después `source_file` y el JSON del record) | descarte (se cuenta, no va a quarantine) | 0 | |
| `VALUE_NULL` | `value` null; `value_numeric` queda null | flag | 873 | [muestra](../evidence/quality/20261114T221819Z-ad1d25/samples/usage_events__VALUE_NULL.csv) |
| `UNIT_IMPUTED` | `unit` null con `value` presente: se completa desde `metric` (`requests → count`, `cpu_hours → hours`, `storage_gb_hours → gb_hours`) | corrige y marca | 2033 | [muestra](../evidence/quality/20261114T221819Z-ad1d25/samples/usage_events__UNIT_IMPUTED.csv) |
| `COST_NEGATIVE_SMALL` | `-0.01 <= cost_usd_increment < 0`; el cost queda como está | flag | 5 | [muestra](../evidence/quality/20261114T221819Z-ad1d25/samples/usage_events__COST_NEGATIVE_SMALL.csv) |
| `ORG_ORPHAN` | `org_id` sin organización en `silver/customers_orgs` | flag | 0 | |
| `RESOURCE_ORPHAN` | `resource_id` sin recurso en `silver/resources` | flag | 0 | |

Las reglas de quarantine se evalúan en el orden de la tabla, después de descartar los duplicados: un `event_id` repetido cuenta una sola vez. Lo rechazado va a `quarantine/silver/usage_events/`, con el registro de Bronze sin normalizar en `_raw`.

Silver también normaliza las categorías (`service`, `region`, `metric`, `unit` sin espacios y en minúscula) y marca picos por evento: `is_cost_anomaly` si el cost supera 3 veces el p99 de su service. Eso no es una regla de calidad; la analítica por día está en [analitica.md](analitica.md).

## Silver: maestros

Ninguna regla de los maestros manda rows a quarantine: una clave null ya la rechazó Bronze.

| `rule_id` | Dataset | Condición | Acción | Rows | Muestra |
| :--- | :--- | :--- | :--- | ---: | :--- |
| `NPS_OUT_OF_RANGE` | `customers_orgs` | `nps_score` fuera de [-100, 100]; pasa a null | corrige y marca | 1 | [muestra](../evidence/quality/20261114T221819Z-ad1d25/samples/customers_orgs__NPS_OUT_OF_RANGE.csv) |
| `NPS_OUT_OF_RANGE` | `nps_surveys` | ídem | corrige y marca | 0 | |
| `LOGIN_BEFORE_CREATED` | `users` | `last_login` anterior a `created_at` | flag | 232 | [muestra](../evidence/quality/20261114T221819Z-ad1d25/samples/users__LOGIN_BEFORE_CREATED.csv) |
| `TAGS_INVALID_JSON` | `resources` | `tags_json` no es un array JSON válido; `tags` queda null | flag | 0 | |
| `CREDITS_IMPUTED` | `billing_monthly` | `credits` null; pasa a 0.0 | corrige y marca | 137 | [muestra](../evidence/quality/20261114T221819Z-ad1d25/samples/billing_monthly__CREDITS_IMPUTED.csv) |
| `SUBTOTAL_NEGATIVE` | `billing_monthly` | `subtotal` negativo | flag | 13 | [muestra](../evidence/quality/20261114T221819Z-ad1d25/samples/billing_monthly__SUBTOTAL_NEGATIVE.csv) |
| `FX_USD_NOT_ONE` | `billing_monthly` | `currency = USD` con `exchange_rate_to_usd` distinto de 1 (D-17) | flag | 160 | [muestra](../evidence/quality/20261114T221819Z-ad1d25/samples/billing_monthly__FX_USD_NOT_ONE.csv) |
| `CSAT_OUT_OF_RANGE` | `support_tickets` | `csat` fuera de [1, 5]; pasa a null | corrige y marca | 40 | [muestra](../evidence/quality/20261114T221819Z-ad1d25/samples/support_tickets__CSAT_OUT_OF_RANGE.csv) |

Las categorías de los maestros (`service`, `region`, `severity`, `plan_tier`, `channel`, `state`) se pasan a minúscula y `currency` a mayúscula.

## Diferencias con el informe

- El informe mandaba a quarantine todo cost negativo; ahora solo los menores a -0.01 (D-13).
- El `value` que llega como texto numérico se castea sin flag propio (D-14).
- Las reglas de la Tabla 2 del informe que no estaban en la Tabla 6 (plan enterprise con `is_enterprise` falso, `converted` sin `clicked`) quedaron en el [backlog](backlog.md).

## Reconciliation

Además de las reglas por row, antes de escribir Gold se comprueba que ninguna row se perdió: todo lo de Landing está en Bronze, en quarantine o se descartó como duplicado, y todo lo de Bronze está en Silver o en quarantine. Los checks y su resultado (`evidence/quality/<run_id>/reconciliation.csv`) están en [`DECISIONS.md`](../DECISIONS.md), D-01.
