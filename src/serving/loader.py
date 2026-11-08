import logging
import os
import re
import time
from dataclasses import dataclass
from itertools import islice
from typing import Callable, Iterable, Iterator

from src.config import Settings, load_settings

log = logging.getLogger(__name__)

IDENTIFIER_RE = re.compile(r"^[a-z_][a-z0-9_]*$")


@dataclass(frozen=True)
class WriteResult:
    table: str
    rows_written: int
    rows_failed: int
    seconds: float

# Info que viaja a los workers de spark
@dataclass(frozen=True)
class _WriteSpec:
    settings: Settings
    cql: str
    columns: tuple[str, ...]
    concurrency: int
    batch_rows: int

# Usar para escribir dataframes de spark en cassandra.
def write_dataframe(
    df,
    table: str,
    settings: Settings | None = None,
    *,
    concurrency: int | None = None,
    batch_rows: int = 1000,
    fail_on_error: bool = True,
) -> WriteResult:
    s = settings or load_settings()
    columns = tuple(df.columns)
    for name in (table, *columns):
        if not IDENTIFIER_RE.match(name):
            raise ValueError(f"Identificador inválido para CQL: {name!r} (usar snake_case)")

    spec = _WriteSpec(
        settings=s,
        cql=f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({', '.join('?' * len(columns))})",
        columns=columns,
        concurrency=concurrency or int(os.getenv("CASSANDRA_WRITE_CONCURRENCY", "32")),
        batch_rows=batch_rows,
    )

    started = time.monotonic()
    log.info("Cargando %s (target=%s, concurrency=%d)", table, s.serving_target, spec.concurrency)
    written, failed = (
        df.rdd.mapPartitions(lambda rows: _write_partition(spec, rows))
        .fold((0, 0), lambda a, b: (a[0] + b[0], a[1] + b[1]))
    )
    result = WriteResult(table, written, failed, round(time.monotonic() - started, 2))
    log.info("Carga %s: %d filas escritas, %d fallidas en %.2fs", table, written, failed, result.seconds)

    if failed and fail_on_error:
        raise RuntimeError(f"{failed} filas fallaron al escribir {table}; ver logs de los workers")
    return result

# Usar para escribir micro lotes de un dataframe de spark en cassandra. Sirve para streaming. Retorna una funcion que puede
# ser usada por foreachBatch. Por ejemplo: df.writeStream.foreachBatch(foreach_batch_writer("my_table")).start()
def foreach_batch_writer(table: str, settings: Settings | None = None, **kwargs) -> Callable:
    s = settings or load_settings()

    def _write(batch_df, batch_id: int) -> None:
        result = write_dataframe(batch_df, table, s, **kwargs)
        log.info("Micro-lote %d -> %s: %d filas", batch_id, table, result.rows_written)

    return _write


def _write_partition(spec: _WriteSpec, rows: Iterable) -> Iterator[tuple[int, int]]:
    from cassandra.concurrent import execute_concurrent_with_args
    from cassandra.query import UNSET_VALUE

    from src.serving.cassandra_client import get_shared_session

    rows = iter(rows)
    first = next(rows, None)
    if first is None:
        yield (0, 0)
        return

    session = get_shared_session(spec.settings)
    prepared = session.prepare(spec.cql)
    written = failed = 0
    pending = _chain_first(first, rows)

    while chunk := list(islice(pending, spec.batch_rows)):
        params = [
            tuple(UNSET_VALUE if row[c] is None else row[c] for c in spec.columns) for row in chunk
        ]
        results = execute_concurrent_with_args(
            session, prepared, params, concurrency=spec.concurrency, raise_on_first_error=False
        )
        for ok, outcome in results:
            if ok:
                written += 1
            else:
                if failed == 0:
                    log.error("Error escribiendo en Cassandra: %s", outcome)
                failed += 1

    yield (written, failed)


def _chain_first(first, rest: Iterator) -> Iterator:
    yield first
    yield from rest
