<div align="center">

# Cloud Provider Analytics

### Trabajo Práctico Especial · 72.80 Big Data · ITBA

**ETL + Streaming + Serving en Cassandra para FinOps, Soporte y Producto**

![Entrega](https://img.shields.io/badge/Entrega-1%C2%AA%20%C2%B7%20Dise%C3%B1o%20y%20fundaci%C3%B3n-0B5A8A?style=for-the-badge)
![Cuatrimestre](https://img.shields.io/badge/2C-2026-1E3A5F?style=for-the-badge)
![Grupo](https://img.shields.io/badge/Grupo-8-F28C28?style=for-the-badge)

![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=flat-square&logo=apachespark&logoColor=white)
![Parquet](https://img.shields.io/badge/Parquet-50ABF1?style=flat-square&logo=apache&logoColor=white)
![Cassandra](https://img.shields.io/badge/Cassandra%20%2F%20AstraDB-1287B1?style=flat-square&logo=apachecassandra&logoColor=white)
![Google Colab](https://img.shields.io/badge/Google%20Colab-F9AB00?style=flat-square&logo=googlecolab&logoColor=white)
![Google Drive](https://img.shields.io/badge/Google%20Drive-4285F4?style=flat-square&logo=googledrive&logoColor=white)

</div>

---

> [!IMPORTANT]
> ### Todos los artefactos de la entrega están dentro del informe
>
> El **documento de diseño** [`TPE Big Data - Grupo 8 2C 2026.pdf`](./TPE%20Big%20Data%20-%20Grupo%208%202C%202026.pdf) contiene **todos** los artefactos pedidos en la consigna: diagrama de arquitectura v1, matriz requisito-componente, diseño del Data Lake, flujos batch/streaming, lógica MapReduce, supuestos, riesgos y plan de esfuerzo.
>
> El notebook **no** contiene esos artefactos: es solamente la **evidencia de lectura y exploración de datos** que respalda la sección 3 del informe.

> [!NOTE]
> ### El notebook se ejecutó desde nuestro entorno en Google Drive
>
> [`exploracion_datos.ipynb`](./exploracion_datos.ipynb) se corrió en **Google Colab**, montando el **Google Drive** del grupo, donde vive el dataset provisto:
>
> ```
> /content/drive/MyDrive/cloud_provider_challenge_dataset_v1/datalake/landing
> ```
>
> El dataset **no** está subido al repositorio. Las salidas que se ven en el notebook corresponden a esa ejecución.

---

## Integrantes

| Integrante | Legajo |
| :--- | :---: |
| Luca Maximiliano Rossi | 63730 |
| Patrick Torlaschi | 62273 |
| Nicolás Casella | 62311 |
| Santiago Nartallo | 62208 |
| Federico Viera | 62022 |

**Docente:** Prof. (Ad.) Diego Mosquera

---

## Dónde encontrar cada artefacto

Mapa entre el checklist de la primera evaluación (sección 9.1 de la consigna) y el lugar donde se responde.

| Ítem de la consigna | Dónde está |
| :--- | :--- |
| Interpretación del caso y objetivos medibles | Informe · **§1** Interpretación del problema |
| Análisis 5V | Informe · **§2** Justificación de Big Data |
| Inventario y perfil de fuentes | Informe · **§3** Inventario y perfil de las fuentes |
| Arquitectura v1 y patrón justificado | Informe · **§4** Arquitectura de alto nivel (Figura 1) |
| Matriz requisito-componente | Informe · **§5** Matriz requisito-componente |
| Diseño Landing / Bronze / Silver / Gold | Informe · **§6** Diseño del Data Lake |
| Flujos batch y streaming | Informe · **§7** Flujos de datos (Figura 2) |
| Lógica MapReduce o equivalente | Informe · **§8** Flujo batch con lógica MapReduce (Figura 3) |
| Supuestos, riesgos y mitigaciones | Informe · **§9** Supuestos, riesgos y decisiones abiertas |
| Estimación de esfuerzo, roles y recursos | Informe · **§10** Esfuerzo, roles y recursos |
| Evidencia mínima de lectura / exploración de datos |  [`exploracion_datos.ipynb`](./exploracion_datos.ipynb) |
| Repositorio accesible y versionado | Este repositorio |

---

## Exploración de datos

El notebook perfila las **ocho fuentes** de Landing sin modificarlas (solo lectura).

| Sección | Qué se analiza |
| :--- | :--- |
| 1 · Configuración | Montaje de Drive y ruta a `landing/` |
| 2 · Master Data | Filas, columnas, vacíos y duplicados por clave natural |
| 3 · Hallazgos de calidad | NPS, CSAT, logins, billing, monedas y huérfanos |
| 4 · Eventos de uso | Evolución de esquema v1 → v2, `value`/`unit`, costos y anomalías, orden de llegada |
| 5 · Integridad | Cruce con dimensiones y tamaño esperado de `org_daily_usage_by_service` |
| 6 · Conclusiones | Resumen de hallazgos que alimentan el diseño |

<details>
<summary><b>Hallazgos clave</b></summary>

<br/>

- **43.200 eventos** en **120 archivos** JSONL; cada archivo cubre los 60 días completos, por lo que el 99 % de los eventos llega desordenado → watermark amplio (61 días) y agregación en batch.
- El esquema cambia a **v2 el 18/07/2025**: `carbon_kg` y `genai_tokens` solo existen en v2 (25 % de eventos son v1).
- `unit` se puede imputar desde `metric` porque la relación es uno a uno.
- **211 eventos** con `cost_usd_increment < -0.01` → quarantine; **79 picos** > 3 × p99 del servicio → flag.
- `billing_monthly`: créditos nulos (57 %), organizaciones con más de una moneda y facturas USD con tipo de cambio ≠ 1.
- Master Data sin claves duplicadas ni huérfanos, pero con nulos y valores fuera de rango (NPS, CSAT, `last_login`).

El detalle completo está en el informe, §3.2 (Tabla 2).

</details>

### Cómo reproducirlo

1. Abrir [`exploracion_datos.ipynb`](./exploracion_datos.ipynb) en **Google Colab**.
2. Subir el dataset provisto a Google Drive en `MyDrive/cloud_provider_challenge_dataset_v1/datalake/landing/`.
3. Ejecutar todas las celdas (`Runtime → Run all`) y autorizar el montaje de Drive.

> [!TIP]
> Para correrlo fuera de Colab, omitir la celda de `drive.mount` y definir la variable de entorno `LANDING` apuntando a la carpeta `landing/` local.

---

## Estructura del repositorio

```
tpe-big-data/
├── README.md                              ← estás acá
├── TPE Big Data - Grupo 8 2C 2026.pdf     ← informe con TODOS los artefactos
└── exploracion_datos.ipynb                ← evidencia de exploración (ejecutado en Colab + Drive)
```

La estructura completa (`src/`, `config/`, `tests/`, `evidence/`, `DECISIONS.md`, etc.) se incorpora a partir de la segunda entrega, junto con el código del pipeline.

---

## Convenciones

| Elemento | Convención | Ejemplo |
| :--- | :--- | :--- |
| Rutas | `{zona}/{dataset}/{partición}=valor/` | `bronze/usage_events/event_date=2025-09-01/` |
| Checkpoints | `checkpoints/{job}/` | `checkpoints/usage_events_stream/` |
| Columnas | `snake_case` en minúscula | `cost_usd_increment` |
| Fechas / timestamps | sufijo `_date` / `_ts` | `usage_date`, `ingest_ts` |
| Montos normalizados | sufijo `_usd` | `revenue_usd` |
| Flags booleanos | prefijo `is_` | `is_cost_anomaly` |
| Columnas de quarantine | prefijo `_` | `_rule_id`, `_quarantine_reason` |
| Serving | keyspace `cloud_analytics`, tabla = nombre del mart Gold | `org_daily_usage_by_service` |

Detalle completo en el informe, §6.7.

---

## Recursos

Google Colab · Google Drive · Python / PySpark · AstraDB (plan gratuito) · GitHub — todos sin costo para el volumen de datos actual.

<div align="center">

<br/>

**Grupo 8 · 72.80 Big Data · Instituto Tecnológico de Buenos Aires · 2C 2026**

</div>
