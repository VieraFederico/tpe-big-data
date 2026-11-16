# Plan de correcciones

Qué hicimos con la devolución de la entrega 1 y dónde la implementación se apartó del informe. El informe queda como documento histórico; lo vigente es lo que está en el repo.

Cada corrección tiene prioridad (alta, media, baja), el rol responsable (los de la Tabla 10 del informe, los mismos de [Gobierno](gobierno/README.md#responsables)), la fecha objetivo y la evidencia que la muestra. Lo que se cerró para la entrega 2 tiene fecha 2026-11-16; lo que pasa a la entrega final, 2026-12-07.

## Devolución de la entrega 1

Una row por dimensión de la rúbrica de la entrega 1. La nota más baja fue Ingeniería y documentación (7.5): casi todo estaba en el PDF y en un notebook.

| Dimensión | Observación | Acción | Prioridad | Responsable | Fecha objetivo | Evidencia | Estado |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| Comprensión y justificación | Sin observación puntual. | Se mantuvieron los usuarios y las cinco preguntas del informe; el README resume el problema y qué consultas ya se responden. | baja | Arquitectura y coordinación | 2026-11-16 | [README](../README.md#qué-es), [matriz](matriz-requisitos.md) | hecho |
| Arquitectura | Sin observación puntual; el diagrama tenía que pasar a reflejar lo implementado. | Diagrama nuevo en Mermaid con lo que existe, flujo de steps y lista de cambios respecto de la Figura 1. | alta | Arquitectura y coordinación | 2026-11-16 | [diagramas/arquitectura.md](diagramas/arquitectura.md) | hecho |
| Data lake | Sin observación puntual. | Zonas, rutas reales, partition de cada dataset (incluidas quarantine y manifests), retención y nombres en un documento versionado. | media | Arquitectura y coordinación | 2026-11-16 | [data-lake.md](data-lake.md) | hecho |
| Datos | Sin observación puntual. | El perfil de calidad del informe pasó a reglas ejecutables con conteo por regla y muestras; diccionario de todas las zonas. | media | Calidad, Silver y gobierno | 2026-11-16 | [calidad.md](calidad.md), [diccionario-datos.md](diccionario-datos.md) | hecho |
| Procesamiento batch | Sin observación puntual. | La lógica MapReduce del informe está implementada y se compara con la versión de DataFrames en un test y sobre el dataset completo. | media | Gold, serving y analítica | 2026-11-16 | [mapreduce.md](mapreduce.md) | hecho |
| Ingeniería y documentación | Separar documentación, decisiones, evidencia, código, configuración y tests en artefactos versionados, y que el README sea la entrada reproducible al trabajo. | README con Quickstart de 4 comandos; `DECISIONS.md`; `docs/` con índice; `evidence/` por entrega; `config/README.md`; un test que falla si un link de la documentación está roto o un README está vacío. | alta | Arquitectura y coordinación | 2026-11-16 | [README](../README.md), [`DECISIONS.md`](../DECISIONS.md), [docs/README.md](README.md), [`evidence/README.md`](../evidence/README.md), [`tests/test_docs.py`](../tests/test_docs.py) | hecho |
| Defensa / respuesta al feedback | Hacer explícitas las decisiones abiertas y las prioridades. | Decisiones abiertas con prioridad, fecha límite y propuesta; backlog con prioridad y estimación; este plan. | media | Arquitectura y coordinación | 2026-11-16 | [Decisiones abiertas](../DECISIONS.md#decisiones-abiertas), [backlog.md](backlog.md) | hecho |

## Diferencias entre el informe y la implementación

| Observación | Acción | Prioridad | Responsable | Fecha objetivo | Evidencia | Estado |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| El informe cita `notebooks/00_exploracion_datos.ipynb`, pero el notebook se llamaba distinto. | Se renombró el notebook y se actualizaron los links. | baja | Arquitectura y coordinación | 2026-11-16 | [notebooks/README.md](../notebooks/README.md) | hecho |
| El informe planteaba Colab como entorno de ejecución. | El entorno de referencia es Docker con Cassandra local; Colab queda como alternativa contra AstraDB. | alta | Arquitectura y coordinación | 2026-11-16 | D-20, [README](../README.md#quickstart-con-docker) | hecho |
| Silver y Gold se iban a recalcular solo para las fechas afectadas. | Se recalculan completos en cada corrida, porque los umbrales dependen de todos los datos. | media | Calidad, Silver y gobierno | 2026-11-16 | D-15 | hecho |
| La carga a Cassandra iba a ser por fecha. | Carga completa por upsert, con gate de publicación. | media | Gold, serving y analítica | 2026-11-16 | D-09 | hecho |
| Una sola quarantine alimentada desde Bronze. | Quarantine por capa con `_rule_id`, `_quarantine_reason` y `_raw`. | alta | Calidad, Silver y gobierno | 2026-11-16 | D-16 | hecho |
| El gate entre Silver y Gold solo estaba descripto. | Gate de reconciliation con 7 checks sobre manifests y disco. | alta | Gold, serving y analítica | 2026-11-16 | D-01 | hecho |
| Todo cost negativo iba a quarantine. | Regla híbrida: quarantine debajo de -0.01, flag entre -0.01 y 0. | alta | Calidad, Silver y gobierno | 2026-11-16 | D-13 | hecho |
| El `value` texto numérico iba a llevar un flag. | Se castea sin flag; el flag queda como deseable. | baja | Calidad, Silver y gobierno | 2026-11-16 | D-14, [backlog](backlog.md) | hecho |
| Tipo de cambio de USD distinto de 1 (decisión abierta del informe). | Se usa el valor provisto y se marca con `FX_USD_NOT_ONE`. | media | Calidad, Silver y gobierno | 2026-11-16 | D-17 | hecho |
| Métricas como columnas o `map` (decisión abierta del informe). | Columnas fijas. | media | Calidad, Silver y gobierno | 2026-11-16 | D-18 | hecho |
| Umbral de anomalías a calibrar (decisión abierta del informe). | Se calibró sobre el dataset completo y se dejó en 3 × p99. | alta | Gold, serving y analítica | 2026-11-16 | D-04, [analitica.md](analitica.md) | hecho |
| En la Figura 1 la analítica aparecía después de Cassandra. | Corre dentro de `gold` sobre el mart diario y se publica como `cost_anomaly_mart`. | media | Arquitectura y coordinación | 2026-11-16 | D-04, [diagramas/arquitectura.md](diagramas/arquitectura.md) | hecho |
| `cost_anomaly_mart` usaba la columna `date`. | Se llama `usage_date`, como pide la convención de nombres. | baja | Gold, serving y analítica | 2026-11-16 | D-03 | hecho |
| La consulta 2 salía de `org_daily_usage_by_service`. | Mart propio `top_services_by_org_14d` para todas las fechas de corte. | alta | Gold, serving y analítica | 2026-11-16 | D-02, D-07 | hecho |
| MapReduce era la lógica del mart diario. | Se implementó como referencia; el pipeline usa DataFrames. | media | Gold, serving y analítica | 2026-11-16 | D-05 | hecho |
| Marts `revenue_by_org_month`, `tickets_by_org_date`, `genai_tokens_by_org_date` y consultas 3 a 5. | Pasan a la entrega final. | alta | Gold, serving y analítica | 2026-12-07 | [backlog](backlog.md) | backlog |
| Retención por zona (Tabla 8). | Documentada; nada se borra automáticamente. La retención automática queda como deseable. | baja | Arquitectura y coordinación | 2026-12-07 | [data-lake.md](data-lake.md#retención), D-25 | backlog |
