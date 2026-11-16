# Notebooks

| Notebook | Para qué |
|---|---|
| [`00_exploracion_datos.ipynb`](./00_exploracion_datos.ipynb) | Lectura y exploración inicial de las fuentes de Landing (primera entrega). Evidencia que respalda la sección 3 del documento de diseño. [Abrir en Colab](https://colab.research.google.com/github/VieraFederico/tpe-big-data/blob/main/notebooks/00_exploracion_datos.ipynb) |
| [`colab_pipeline.ipynb`](./colab_pipeline.ipynb) | Corre los cinco steps del pipeline en Google Colab, sin Docker, publica en AstraDB y corre las consultas 1 y 2. [Abrir en Colab](https://colab.research.google.com/github/VieraFederico/tpe-big-data/blob/main/notebooks/colab_pipeline.ipynb) |

## `00_exploracion_datos.ipynb`

Es el notebook que cita el informe de la entrega 1. Se corrió en Google Colab montando el Google Drive del grupo, donde está el dataset provisto:

```
/content/drive/MyDrive/cloud_provider_challenge_dataset_v1/datalake/landing
```

Las salidas que se ven en el notebook corresponden a esa ejecución. Para reproducirlo:

1. Abrirlo en Google Colab.
2. Subir el dataset provisto por la cátedra a Google Drive en `MyDrive/cloud_provider_challenge_dataset_v1/datalake/landing/`.
3. Ejecutar todas las celdas (`Runtime → Run all`) y autorizar el montaje de Drive.

## `colab_pipeline.ipynb`

Es la alternativa a Docker del [Quickstart](../README.md#alternativa-google-colab). Clona el repo, instala `config/requirements.txt` y Java 17, y después:

1. Prueba la conexión a AstraDB.
2. Corre los cinco steps, uno por celda: `bronze_batch`, `bronze_stream`, `silver`, `gold` y `serving`.
3. Muestra los conteos de `_manifests/` por step y dataset (lo mismo que `conteos.csv` en la evidencia).
4. Corre las consultas 1 y 2 contra AstraDB con la org y las fechas que calcula `src/serving/evidence_params.py`, igual que `./infra/run.sh queries`.
5. Baja en un zip los logs y las consultas.

El notebook lee `ASTRA_DB_APPLICATION_TOKEN` y `ASTRA_DB_API_ENDPOINT` de **Colab Secrets**. No se escriben en el notebook ni en Drive. El secure connect bundle se descarga solo al disco de la VM, que se borra al cerrar la sesión; si se prefiere subir uno ya descargado, se pone su ruta en la celda de configuración y no hace falta el endpoint. El keyspace `cloud_analytics` tiene que existir en Astra antes de correrlo ([cómo crearlo](../docs/serving.md#crear-el-keyspace-en-astradb)).

Usa el dataset que ya está en el repo (`data/datalake/landing`), así que no hace falta montar Drive.

Para versionarlo con salidas: correrlo completo con `Runtime → Run all`, revisar que ninguna salida muestre el token y guardarlo con sus salidas (`File → Download → Download .ipynb`) en `notebooks/`.
