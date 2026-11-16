# Documentación

Documentación técnica del pipeline. Las decisiones están en [`DECISIONS.md`](../DECISIONS.md) y la puesta en marcha en el [README](../README.md).

| Documento | Qué responde |
| :--- | :--- |
| [diagramas/README.md](diagramas/README.md) | Índice de los diagramas. |
| [diagramas/arquitectura.md](diagramas/arquitectura.md) | Cómo es la arquitectura implementada, cómo se encadenan los cinco steps y qué cambió respecto de la Figura 1 del informe. |
| [matriz-requisitos.md](matriz-requisitos.md) | Qué componente resuelve cada ítem de la consigna y cada requisito del informe, con qué evidencia y en qué estado está. |
| [data-lake.md](data-lake.md) | Zonas, rutas, formato, partition de cada dataset, retención y convenciones de nombres. |
| [diccionario-datos.md](diccionario-datos.md) | Columnas, tipos y grain de cada dataset de Bronze, Silver, Gold y quarantine, de los manifests y de las tablas de Cassandra/AstraDB. |
| [calidad.md](calidad.md) | Qué reglas de calidad hay en cada capa, qué hace cada una y cuántas rows afecta. |
| [mapreduce.md](mapreduce.md) | El mart diario como job MapReduce (map → shuffle → reduce) y cómo se comprueba que da lo mismo que la versión con DataFrames. |
| [analitica.md](analitica.md) | Detección de anomalías de cost diario: método, resumen de la corrida y calibración del multiplier. |
| [serving.md](serving.md) | Tablas de Cassandra/AstraDB pensadas desde las consultas, carga con upsert y gate de publicación, cómo configurar Astra y correr las consultas, evidencia y límites. |
| [gobierno/README.md](gobierno/README.md) | Controles, metadatos, linaje, responsables y acceso (gobierno preliminar). |
| [plan-correcciones.md](plan-correcciones.md) | Qué se corrigió de la entrega 1 y dónde se apartó la implementación del informe. |
| [backlog.md](backlog.md) | Qué falta para la entrega final, qué es deseable y qué queda fuera de alcance. |

Documentos de la cursada:

- [Informe de la entrega 1](TPE%20Big%20Data%20-%20Grupo%208%202C%202026.pdf): documento de diseño original. Queda como registro histórico; lo vigente es lo que está en el repo.
- [Consigna](Consigna_Proyecto_Integrador_Big_Data_ITBA_2C_2026%20%282%29.pdf) del proyecto y de las tres entregas.
