# Backlog

Lo que falta después de la entrega 2, en las tres categorías que pide la consigna. Las estimaciones son en horas-persona: las de marts, consultas, presentación, video y defensa reparten las de la Tabla 11 del informe; el resto son estimaciones nuevas. Prioridades: alta, media, baja. Las decisiones que todavía no se tomaron están en [`DECISIONS.md`](../DECISIONS.md#decisiones-abiertas).

## Obligatorio para la entrega final (2026-12-07)

| Ítem | Prioridad | Estimación | Origen |
| :--- | :--- | ---: | :--- |
| Mart `revenue_by_org_month`, su tabla en Cassandra y la consulta 4 (revenue mensual con créditos e impuestos en USD) | alta | 7 | Consigna 7.3 y 7.4; informe 1.3 |
| Mart `tickets_by_org_date`, su tabla y la consulta 3 (tickets críticos y tasa de SLA breach por día, últimos 30 días) | alta | 7 | Consigna 7.3 y 7.4; informe 1.3 |
| Mart `genai_tokens_by_org_date`, su tabla y la consulta 5 (tokens GenAI y cost estimado por día) | alta | 6 | Consigna 7.3 y 7.4; informe 1.3 |
| Presentación ejecutiva | alta | 10 | Consigna 7.5 |
| Video explicativo | alta | 6 | Consigna 7.5 |
| Defensa oral remota (demo y respuesta técnica) | alta | 8 | Consigna 7.5 |
| Seguridad y observabilidad del gobierno (más allá del preliminar) | media | 4 | Consigna 7.2, ítem 7 |

## Deseable

| Ítem | Prioridad | Estimación | Origen |
| :--- | :--- | ---: | :--- |
| Recálculo de Silver y Gold solo para las fechas afectadas | media | 10 | Informe 2.2 y 7.2; D-15, D-22 |
| Borrar de Cassandra las rows que ya no están en Gold | media | 4 | D-09, D-21 |
| Tests propios del enrichment de eventos y del step `silver` completo | media | 3 | Matriz de requisitos |
| Reglas `ENTERPRISE_MISMATCH` (plan enterprise con `is_enterprise` falso) y `CONVERTED_WITHOUT_CLICK` (`converted` sin `clicked`) | baja | 3 | Informe, Tabla 2 |
| Flag para el `value` que llega como texto numérico | baja | 4 | Informe, Tabla 6; D-14 |
| Retención automática por zona | baja | 4 | Informe, Tabla 8; D-25 |
| Join temporal de los eventos contra `orgs_scd2` | baja | 4 | D-19, D-23 |
| Watermark configurable para producción | baja | 2 | Informe 7.3; D-24 |
| Que el gate de Gold tome un manifest incompleto (sin `datasets` o `ingest_date`) como check fallido; hoy corta con `KeyError` | baja | 1 | D-01 |

## Fuera de alcance

| Ítem | Prioridad | Estimación | Origen |
| :--- | :--- | ---: | :--- |
| Kafka u otro broker para los eventos | baja | 16 | Informe 9.2 (latencia sin broker) |
| Orquestador (Airflow) para programar las corridas | baja | 12 | Consigna 4.1; hoy alcanza con `./infra/run.sh` |
| Modelos de ML entrenados para las anomalías | baja | 20 | Consigna 6.2 (se acepta analítica equivalente); D-04 |

Ninguno de estos pone en riesgo el flujo end-to-end; quedan registrados por si el alcance cambia.
