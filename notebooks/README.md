# Notebooks

| Notebook | Para qué |
|---|---|
| [`exploracion_datos.ipynb`](./exploracion_datos.ipynb) | Lectura y exploración inicial de las fuentes de Landing (primera entrega). Evidencia que respalda la sección 3 del documento de diseño. [Abrir en Colab](https://colab.research.google.com/github/VieraFederico/tpe-big-data/blob/main/notebooks/exploracion_datos.ipynb) |
| [`colab_pipeline.ipynb`](./colab_pipeline.ipynb) | Corre el pipeline en Google Colab, sin Docker, contra AstraDB. [Abrir en Colab](https://colab.research.google.com/github/VieraFederico/tpe-big-data/blob/main/notebooks/colab_pipeline.ipynb) |

## `exploracion_datos.ipynb`

Se corrió en Google Colab montando el Google Drive del grupo, donde está el dataset provisto:

```
/content/drive/MyDrive/cloud_provider_challenge_dataset_v1/datalake/landing
```

Las salidas que se ven en el notebook corresponden a esa ejecución. Para reproducirlo:

1. Abrirlo en Google Colab.
2. Subir el dataset provisto por la cátedra a Google Drive en `MyDrive/cloud_provider_challenge_dataset_v1/datalake/landing/`.
3. Ejecutar todas las celdas (`Runtime → Run all`) y autorizar el montaje de Drive.

## `colab_pipeline.ipynb`

El notebook lee `ASTRA_DB_APPLICATION_TOKEN` y `ASTRA_DB_API_ENDPOINT` de **Colab Secrets**. No se escriben en el notebook ni en Drive. El secure connect bundle se descarga solo al disco de la VM, que se borra al cerrar la sesión.

Usa el dataset que ya está en el repo (`data/datalake/landing`), así que no hace falta montar Drive.
