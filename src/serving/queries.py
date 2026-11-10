"""CLI para ejecutar una consulta CQL del serving y guardar su evidencia (.txt y .csv)."""
import argparse
import csv
import logging
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from cassandra.query import SimpleStatement

from src.config import REPO_ROOT, Settings, load_settings

log = logging.getLogger(__name__)

DEFAULT_EVIDENCE_DIR = REPO_ROOT / "evidence"


@dataclass
class QueryResult:
    """Resultado de una consulta CQL, listo para guardar como evidencia.

    Attributes:
        name: Nombre de la consulta (y de los archivos de evidencia).
        cql: Texto de la consulta ejecutada.
        params: Parámetros con los que se ejecutó.
        columns: Nombres de las columnas del resultado.
        rows: Filas recuperadas (como máximo ``max_rows``).
        total_rows: Total de filas que devolvió la consulta.
        seconds: Duración, incluyendo la paginación.
        executed_at: Momento de ejecución en ISO 8601 (UTC).
    """

    name: str
    cql: str
    params: Mapping[str, Any]
    columns: list[str]
    rows: list[tuple]
    total_rows: int
    seconds: float
    executed_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat(timespec="seconds"))


def run_query(
    session,
    cql: str,
    params: Mapping[str, Any] | None = None,
    *,
    name: str = "query",
    fetch_size: int = 500,
    max_rows: int = 1000,
) -> QueryResult:
    """Ejecuta una consulta CQL paginada y junta su resultado.

    Recorre todas las páginas para contar el total de filas, pero guarda solo las
    primeras ``max_rows``.

    Args:
        session: Sesión CQL abierta.
        cql: Consulta; se aceptan espacios y ``;`` al final.
        params: Parámetros con nombre de la consulta.
        name: Nombre de la consulta para logs y evidencia.
        fetch_size: Filas por página.
        max_rows: Máximo de filas a conservar en el resultado.

    Returns:
        El resultado con columnas, filas, total y duración.
    """
    params = dict(params or {})
    statement = SimpleStatement(cql.strip().rstrip(";"), fetch_size=fetch_size)

    started = time.monotonic()
    result_set = session.execute(statement, params or None)
    columns = list(result_set.column_names or [])
    rows, total_rows = [], 0
    for row in result_set:
        total_rows += 1
        if len(rows) < max_rows:
            rows.append(tuple(row))
    seconds = round(time.monotonic() - started, 3)

    log.info("Consulta %s: %d filas en %.3fs", name, total_rows, seconds)
    return QueryResult(name, cql.strip(), params, columns, rows, total_rows, seconds)


def save_evidence(
    result: QueryResult, settings: Settings, evidence_dir: Path = DEFAULT_EVIDENCE_DIR
) -> Path:
    """Guarda el resultado como evidencia: un .txt legible y un .csv con las filas.

    Returns:
        Ruta del archivo .txt.
    """
    evidence_dir.mkdir(parents=True, exist_ok=True)
    txt_path = evidence_dir / f"{result.name}.txt"
    csv_path = evidence_dir / f"{result.name}.csv"

    truncated = f" (se muestran {len(result.rows)})" if result.total_rows > len(result.rows) else ""
    report_lines = [
        f"Consulta:   {result.name}",
        f"Ejecutada:  {result.executed_at}",
        f"Destino:    {settings.serving_target} / keyspace {settings.keyspace}",
        f"Parámetros: {result.params or '-'}",
        f"Filas:      {result.total_rows}{truncated}",
        f"Tiempo:     {result.seconds}s",
        "",
        "CQL:",
        result.cql,
        "",
        "Resultado:",
        format_table(result.columns, result.rows),
    ]
    txt_path.write_text("\n".join(report_lines) + "\n", encoding="utf-8")

    with csv_path.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(result.columns)
        writer.writerows(result.rows)

    log.info("Evidencia guardada en %s y %s", txt_path, csv_path)
    return txt_path


def format_table(columns: list[str], rows: list[tuple], max_rows: int = 50) -> str:
    """Formatea filas como una tabla de texto con columnas alineadas.

    Args:
        columns: Nombres de las columnas (encabezado).
        rows: Filas a mostrar; los None se muestran vacíos.
        max_rows: Máximo de filas a incluir; si hay más, se indica al final.

    Returns:
        La tabla como texto, lista para imprimir o guardar.
    """
    if not columns:
        return "(sin columnas)"
    shown_rows = [["" if value is None else str(value) for value in row] for row in rows[:max_rows]]
    widths = [
        max([len(column_name), *(len(row[index]) for row in shown_rows)])
        for index, column_name in enumerate(columns)
    ]

    def format_line(values: list[str]) -> str:
        return " | ".join(value.ljust(width) for value, width in zip(values, widths))

    lines = [format_line(columns), "-+-".join("-" * width for width in widths)]
    lines.extend(format_line(row) for row in shown_rows)
    if len(rows) > max_rows:
        lines.append(f"... ({len(rows) - max_rows} filas más en el .csv)")
    if not rows:
        lines.append("(sin filas)")
    return "\n".join(lines)


PARAM_TYPES: dict[str, Callable[[str], Any]] = {"str": str, "int": int, "float": float}


def parse_param(item: str) -> tuple[str, Any]:
    """Parsea un parámetro de consulta con la forma CLAVE=VALOR o CLAVE:TIPO=VALOR.

    Por defecto el valor queda como texto: adivinar el tipo hacía que, por ejemplo,
    un id "123" se mandara como int y Cassandra lo rechazara contra una columna text.

    Args:
        item: Texto tal como llega en ``-p``, p. ej. ``org_id=org_001`` o ``limit:int=10``.

    Returns:
        Tupla (clave, valor convertido al tipo pedido).

    Raises:
        ValueError: Si falta el ``=``, la clave está vacía, el tipo no existe o el
            valor no se puede convertir.
    """
    key_spec, separator, raw_value = item.partition("=")
    key, _, type_name = key_spec.partition(":")
    if not separator or not key:
        raise ValueError(f"Parámetro inválido {item!r}: usar CLAVE=VALOR o CLAVE:TIPO=VALOR")

    type_name = type_name or "str"
    if type_name not in PARAM_TYPES:
        raise ValueError(f"Tipo {type_name!r} desconocido en {item!r}; usar uno de {list(PARAM_TYPES)}")
    try:
        return key, PARAM_TYPES[type_name](raw_value)
    except ValueError as error:
        raise ValueError(f"El valor {raw_value!r} de {key!r} no es {type_name}") from error


def main(argv: list[str] | None = None) -> int:
    """CLI: ejecuta el .cql indicado, guarda la evidencia e imprime la tabla."""
    parser = argparse.ArgumentParser(description="Ejecuta una consulta CQL y guarda la evidencia.")
    parser.add_argument("cql_file", type=Path)
    parser.add_argument(
        "-p",
        "--param",
        action="append",
        default=[],
        metavar="CLAVE[:TIPO]=VALOR",
        help=f"Parámetro de la consulta; TIPO es uno de {list(PARAM_TYPES)} (default: str)",
    )
    parser.add_argument("--name", help="Nombre de la evidencia (default: nombre del archivo)")
    parser.add_argument("--evidence-dir", type=Path, default=DEFAULT_EVIDENCE_DIR)
    parser.add_argument("--max-rows", type=int, default=1000)
    arguments = parser.parse_args(argv)

    params = {}
    for item in arguments.param:
        try:
            key, value = parse_param(item)
        except ValueError as error:
            parser.error(str(error))
        params[key] = value

    from src.serving.cassandra_client import session_scope

    settings = load_settings()
    with session_scope(settings) as session:
        result = run_query(
            session,
            arguments.cql_file.read_text(encoding="utf-8"),
            params,
            name=arguments.name or arguments.cql_file.stem,
            max_rows=arguments.max_rows,
        )
    save_evidence(result, settings, arguments.evidence_dir)
    print(format_table(result.columns, result.rows))
    return 0


if __name__ == "__main__":
    from src.common.logging_setup import setup_logging

    setup_logging("queries")
    sys.exit(main())
