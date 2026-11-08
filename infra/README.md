# Infraestructura

Entorno Docker para correr el pipeline contra **Cassandra local** o **AstraDB**, con el mismo código.

| Archivo | Qué hace |
| :--- | :--- |
| `Dockerfile` | Imagen del pipeline: Python 3.11 + Java 17 (Spark) + dependencias de `config/requirements.txt`. |
| `docker-compose.yml` | Servicio `pipeline` y, con el perfil `local`, un Cassandra 4.1. |

## Configuración

```bash
cp config/.env.example config/.env
```

El destino del serving se elige con `SERVING_TARGET` en `config/.env`:

| `SERVING_TARGET` | Destino | Qué hace falta |
| :--- | :--- | :--- |
| `local` | Cassandra en Docker | Nada; el keyspace se crea solo. |
| `astra` | AstraDB | Ver [Requisitos para AstraDB](#requisitos-para-astradb). |

### Requisitos para AstraDB

Antes de correr con `SERVING_TARGET=astra`:

1. **Crear el keyspace `cloud_analytics` a mano** desde la UI de Astra: *Databases → (la base) → Data Explorer → Create Keyspace*. Astra no permite `CREATE KEYSPACE` por CQL, así que el pipeline no puede crearlo. Si falta, la conexión falla con `El keyspace 'cloud_analytics' no existe en astra`.
2. **Generar un token** (*Settings → Tokens*) y copiarlo en `ASTRA_DB_APPLICATION_TOKEN`.
3. **Copiar el API endpoint** de la base (`https://<db_id>-<region>.apps.astra.datastax.com`) en `ASTRA_DB_API_ENDPOINT`.
4. Poner `SERVING_TARGET=astra` en `config/.env`.

El secure connect bundle no hace falta descargarlo manualmente: se descarga solo a `secrets/` en la primera conexión.

## Ejecución

Desde la raíz del repo, con `infra/run.sh` (Linux, Mac, Git Bash) o `infra/run.ps1` (PowerShell). Los dos aceptan los mismos comandos:

```bash
./infra/run.sh list                              # pasos del pipeline y si están implementados
./infra/run.sh run                               # pipeline completo
./infra/run.sh run bronze_batch,bronze_stream    # solo algunos pasos
./infra/run.sh check                             # prueba la conexión a Cassandra/AstraDB
./infra/run.sh query src/serving/queries/q1.cql -p org_id=org_001   # consulta + evidencia
./infra/run.sh shell                             # terminal dentro del contenedor
./infra/run.sh clean                             # borra las salidas del lake (landing queda)
./infra/run.sh down                              # baja los contenedores
./infra/run.sh build                             # reconstruye la imagen
```

```powershell
.\infra\run.ps1 run bronze_batch
```

Si `SERVING_TARGET=local`, los comandos que usan Cassandra (`check`, `query`, `run` con `serving`) la levantan solos. Si no existe `config/.env`, se crea desde el ejemplo.

Si PowerShell bloquea el script por la política de ejecución: `powershell -ExecutionPolicy Bypass -File .\infra\run.ps1 <comando>`.

Equivalente sin scripts:

```bash
docker compose -f infra/docker-compose.yml run --rm pipeline python -m src.pipeline --steps bronze_batch
```

## Volúmenes

- `src/` se monta en el contenedor: los cambios de código no requieren rebuild.
- `data/` se monta en el contenedor: las salidas del lake (bronze, silver, gold...) quedan en el host.
- `secrets/` guarda el bundle de Astra; está ignorado por git.

## Limpieza

```bash
./infra/run.sh clean   # salidas del lake: bronze, silver, gold, quarantine, checkpoints
./infra/run.sh down    # contenedores

# Además borra los datos de Cassandra local
docker compose -f infra/docker-compose.yml --profile local down -v
```
