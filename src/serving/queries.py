import argparse
import csv
import logging
import sys
import time
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
    name: str
    cql: str
    params: Mapping[str, Any]
    columns: list[str]
    rows: list[tuple]
    total_rows: int
    seconds: float
    executed_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat(timespec="seconds"))

# Ejectuta una consulta CQL y devuelve un QueryResult, de forma paginada.
def run_query(
    session,
    cql: str,
    params: Mapping[str, Any] | None = None,
    *,
    name: str = "query",
    fetch_size: int = 500,
    max_rows: int = 1000,
) -> QueryResult:
    params = dict(params or {})
    statement = SimpleStatement(cql.strip().rstrip(";"), fetch_size=fetch_size)

    started = time.monotonic()
    result_set = session.execute(statement, params or None)
    columns = list(result_set.column_names or [])
    rows, total = [], 0
    for row in result_set:
        total += 1
        if len(rows) < max_rows:
            rows.append(tuple(row))
    seconds = round(time.monotonic() - started, 3)

    log.info("Consulta %s: %d filas en %.3fs", name, total, seconds)
    return QueryResult(name, cql.strip(), params, columns, rows, total, seconds)


def save_evidence(
    result: QueryResult, settings: Settings, evidence_dir: Path = DEFAULT_EVIDENCE_DIR
) -> Path:
    evidence_dir.mkdir(parents=True, exist_ok=True)
    txt_path = evidence_dir / f"{result.name}.txt"
    csv_path = evidence_dir / f"{result.name}.csv"

    truncated = f" (se muestran {len(result.rows)})" if result.total_rows > len(result.rows) else ""
    header = [
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
    txt_path.write_text("\n".join(header) + "\n", encoding="utf-8")

    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(result.columns)
        writer.writerows(result.rows)

    log.info("Evidencia guardada en %s y %s", txt_path, csv_path)
    return txt_path


def format_table(columns: list[str], rows: list[tuple], max_rows: int = 50) -> str:
    if not columns:
        return "(sin columnas)"
    shown = [["" if v is None else str(v) for v in row] for row in rows[:max_rows]]
    widths = [max(len(c), *(len(r[i]) for r in shown)) if shown else len(c) for i, c in enumerate(columns)]
    line = lambda values: " | ".join(v.ljust(w) for v, w in zip(values, widths))
    out = [line(columns), "-+-".join("-" * w for w in widths), *(line(r) for r in shown)]
    if len(rows) > max_rows:
        out.append(f"... ({len(rows) - max_rows} filas más en el .csv)")
    if not rows:
        out.append("(sin filas)")
    return "\n".join(out)


def _parse_value(raw: str) -> Any:
    for cast in (int, float):
        try:
            return cast(raw)
        except ValueError:
            pass
    return raw


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Ejecuta una consulta CQL y guarda la evidencia.")
    parser.add_argument("cql_file", type=Path)
    parser.add_argument("-p", "--param", action="append", default=[], metavar="CLAVE=VALOR")
    parser.add_argument("--name", help="Nombre de la evidencia (default: nombre del archivo)")
    parser.add_argument("--evidence-dir", type=Path, default=DEFAULT_EVIDENCE_DIR)
    parser.add_argument("--max-rows", type=int, default=1000)
    args = parser.parse_args(argv)

    params = {}
    for item in args.param:
        key, sep, value = item.partition("=")
        if not sep:
            parser.error(f"Parámetro inválido {item!r}: usar CLAVE=VALOR")
        params[key] = _parse_value(value)

    from src.serving.cassandra_client import session_scope

    settings = load_settings()
    with session_scope(settings) as session:
        result = run_query(
            session,
            args.cql_file.read_text(encoding="utf-8"),
            params,
            name=args.name or args.cql_file.stem,
            max_rows=args.max_rows,
        )
    save_evidence(result, settings, args.evidence_dir)
    print(format_table(result.columns, result.rows))
    return 0


if __name__ == "__main__":
    from src.common.logging_setup import setup_logging

    setup_logging("queries")
    sys.exit(main())
