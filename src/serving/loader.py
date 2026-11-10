"""Escritura de DataFrames de Spark en Cassandra/AstraDB, en paralelo desde los workers."""
import logging
import os
import re
import time
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass
from itertools import chain, islice
from typing import Any

from pyspark.sql import DataFrame, Row

from src.config import Settings, load_settings
from src.serving.errors import WriteFailedError

log = logging.getLogger(__name__)

IDENTIFIER_RE = re.compile(r"^[a-z_][a-z0-9_]*$")
DEFAULT_WRITE_CONCURRENCY = 32


@dataclass(frozen=True)
class WriteResult:
    """Resultado de cargar un DataFrame en una tabla.

    Attributes:
        table: Tabla destino.
        rows_written: Filas escritas con éxito.
        rows_failed: Filas que Cassandra rechazó.
        seconds: Duración total de la carga.
    """

    table: str
    rows_written: int
    rows_failed: int
    seconds: float


@dataclass(frozen=True)
class _WriteSpec:
    """Lo que necesita cada worker de Spark para escribir su partición (viaja serializado)."""

    settings: Settings
    cql: str
    columns: tuple[str, ...]
    concurrency: int
    batch_rows: int


def write_dataframe(
    df: DataFrame,
    table: str,
    settings: Settings | None = None,
    *,
    concurrency: int | None = None,
    batch_rows: int = 1000,
    fail_on_error: bool = True,
) -> WriteResult:
    """Escribe un DataFrame de Spark en una tabla de Cassandra.

    Cada partición se escribe desde su worker con un INSERT preparado y ejecución
    concurrente. Los None se mandan como UNSET para no generar tombstones.

    Args:
        df: DataFrame cuyas columnas coinciden con las de la tabla.
        table: Tabla destino, dentro del keyspace de ``settings``.
        settings: Configuración; si no se pasa se lee del entorno.
        concurrency: Requests en vuelo por partición (default: CASSANDRA_WRITE_CONCURRENCY
            o DEFAULT_WRITE_CONCURRENCY).
        batch_rows: Filas que se mandan juntas en cada tanda concurrente.
        fail_on_error: Si es True, falla cuando alguna fila no se pudo escribir.

    Returns:
        Filas escritas y fallidas, y la duración de la carga.

    Raises:
        ValueError: Si la tabla o alguna columna no es un identificador CQL válido.
        WriteFailedError: Si hubo filas fallidas y ``fail_on_error`` es True.
    """
    resolved_settings = settings or load_settings()
    columns = tuple(df.columns)
    for name in (table, *columns):
        if not IDENTIFIER_RE.match(name):
            raise ValueError(f"Identificador inválido para CQL: {name!r} (usar snake_case)")

    spec = _WriteSpec(
        settings=resolved_settings,
        cql=f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({', '.join('?' * len(columns))})",
        columns=columns,
        concurrency=concurrency or int(os.getenv("CASSANDRA_WRITE_CONCURRENCY", DEFAULT_WRITE_CONCURRENCY)),
        batch_rows=batch_rows,
    )

    started = time.monotonic()
    log.info("Cargando %s (target=%s, concurrency=%d)",
             table, resolved_settings.serving_target, spec.concurrency)
    written, failed = (
        df.rdd.mapPartitions(lambda rows: _write_partition(spec, rows))
        .fold((0, 0), _add_counts)
    )
    result = WriteResult(table, written, failed, round(time.monotonic() - started, 2))
    log.info("Carga %s: %d filas escritas, %d fallidas en %.2fs", table, written, failed, result.seconds)

    if failed and fail_on_error:
        raise WriteFailedError(table, failed)
    return result


def foreach_batch_writer(
    table: str, settings: Settings | None = None, **write_options: Any
) -> Callable[[DataFrame, int], None]:
    """Devuelve una función para ``foreachBatch`` que escribe cada micro-lote en Cassandra.

    Ejemplo: ``df.writeStream.foreachBatch(foreach_batch_writer("my_table")).start()``.

    Args:
        table: Tabla destino.
        settings: Configuración; si no se pasa se lee del entorno.
        **write_options: Opciones que se pasan a ``write_dataframe``.
    """
    resolved_settings = settings or load_settings()

    def _write(batch_df: DataFrame, batch_id: int) -> None:
        result = write_dataframe(batch_df, table, resolved_settings, **write_options)
        log.info("Micro-lote %d -> %s: %d filas", batch_id, table, result.rows_written)

    return _write


def _add_counts(left: tuple[int, int], right: tuple[int, int]) -> tuple[int, int]:
    """Suma dos pares (escritas, fallidas)."""
    return left[0] + right[0], left[1] + right[1]


def _write_partition(spec: _WriteSpec, rows: Iterable[Row]) -> Iterator[tuple[int, int]]:
    """Escribe las filas de una partición y emite un único par (escritas, fallidas).

    Las particiones vacías no abren sesión. Solo se loguea el primer error, para no
    inundar los logs del worker si falla toda la partición.
    """
    from cassandra.concurrent import execute_concurrent_with_args
    from cassandra.query import UNSET_VALUE

    from src.serving.cassandra_client import get_shared_session

    row_iterator = iter(rows)
    first_row = next(row_iterator, None)
    if first_row is None:
        yield (0, 0)
        return

    session = get_shared_session(spec.settings)
    prepared = session.prepare(spec.cql)
    written = failed = 0
    pending_rows = chain([first_row], row_iterator)

    while row_chunk := list(islice(pending_rows, spec.batch_rows)):
        params = [
            tuple(UNSET_VALUE if row[column] is None else row[column] for column in spec.columns)
            for row in row_chunk
        ]
        results = execute_concurrent_with_args(
            session, prepared, params, concurrency=spec.concurrency, raise_on_first_error=False
        )
        for succeeded, outcome in results:
            if succeeded:
                written += 1
            else:
                if failed == 0:
                    log.error("Error escribiendo en Cassandra: %s", outcome)
                failed += 1

    yield (written, failed)
