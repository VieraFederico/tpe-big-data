"""Chequeos de la documentación versionada: links, READMEs y secciones obligatorias."""

import csv
import fnmatch
import json
import os
import re
import shutil
import subprocess
from collections.abc import Iterable, Iterator
from functools import cache
from pathlib import Path
from urllib.parse import unquote

import pytest

from src.evidence.collect import SCHEMAS_FILE
from src.evidence.inventory import DELIVERY_DIR
from src.pipeline import STEPS
from src.quality.report import QUALITY_DIR, RULE_COUNTS_FILE, SAMPLES_DIR
from src.serving.cassandra_writer import SCHEMA_DIR

# En Docker la documentación no está en la imagen: compose monta el repo en REPO_PATH.
REPO = Path(os.getenv("REPO_PATH") or Path(__file__).resolve().parent.parent)

# Evidencia de la corrida de entrega; los chequeos que la usan se saltean hasta que exista.
DELIVERY = REPO / "evidence" / DELIVERY_DIR

# Un link en una línea con esta marca apunta a evidencia que todavía no se generó.
PENDING = "pendiente de corrida"

README_SECTIONS = [
    "Qué es",
    "Estado de la entrega 2",
    "Quickstart con Docker",
    "Alternativa: Google Colab",
    "Mapa del repositorio",
    "Comandos",
    "Tests",
    "Problemas comunes",
    "Integrantes",
]
DECISION_FIELDS = [
    "Fecha",
    "Contexto",
    "Decisión",
    "Alternativas descartadas",
    "Consecuencias",
    "Estado",
]
GOVERNANCE_SECTIONS = ["Controles", "Metadatos", "Linaje", "Responsables", "Acceso y secretos"]
BACKLOG_SECTIONS = [
    "Obligatorio para la entrega final (2026-12-07)",
    "Deseable",
    "Fuera de alcance",
]
FEEDBACK_DIMENSIONS = [
    "Comprensión y justificación",
    "Arquitectura",
    "Data lake",
    "Datos",
    "Procesamiento batch",
    "Ingeniería y documentación",
    "Defensa / respuesta al feedback",
]
PRIORITIES = {"alta", "media", "baja"}
NOTEBOOK = "00_exploracion_datos.ipynb"
# Los maestros de Silver se documentan en texto: las columnas de Bronze sin ingest_date, más estas.
SILVER_MASTER_COLUMNS = {"dq_flags", "ingest_ts", "source_file", "run_id"}

_LINK_RE = re.compile(r"!?\[[^\]]*\]\(([^)\s]+)[^)]*\)")
_FENCE_RE = re.compile(r"^\s*(```|~~~)")
_INLINE_CODE_RE = re.compile(r"`[^`]*`")
_URL_SCHEME_RE = re.compile(r"^[a-z][a-z0-9+.-]*:", re.IGNORECASE)
_HEADING_RE = re.compile(r"^## (.+?)\s*$", re.MULTILINE)
_ENV_VAR_RE = re.compile(r"^#?\s*([A-Z][A-Z0-9_]*)=", re.MULTILINE)
_SEPARATOR_RE = re.compile(r"^\|[\s:|-]+\|$")
_SUBSECTION_RE = re.compile(r"^#{2,3} (.+?)\s*$(.*?)(?=^#{2,3} |\Z)", re.MULTILINE | re.DOTALL)
_CODE_SPAN_RE = re.compile(r"`([^`]+)`")
_DDL_COLUMN_RE = re.compile(r"^\s+(\w+) [\w<>, ]+,$", re.MULTILINE)


def versioned_files(root: Path) -> list[Path]:
    """Archivos que git versiona o versionaría; sin git, recorre el repo salteando lo ignorado."""
    if shutil.which("git") and (root / ".git").exists():
        listed = subprocess.run(
            ["git", "ls-files", "--cached", "--others", "--exclude-standard", "-z"],
            cwd=root,
            capture_output=True,
            text=True,
            check=False,
        )
        if listed.returncode == 0:
            names = [name for name in listed.stdout.split("\0") if name]
            return sorted(root / name for name in names if (root / name).is_file())
    return sorted(_walk_without_ignored(root))


def _ignore_patterns(root: Path) -> list[str]:
    patterns = []
    for ignore_file in (root / ".gitignore", root / ".git" / "info" / "exclude"):
        if ignore_file.is_file():
            for line in ignore_file.read_text(encoding="utf-8").splitlines():
                if line.strip() and not line.startswith("#"):
                    patterns.append(line.strip())
    return patterns


def _is_ignored(relative: str, is_dir: bool, patterns: list[str]) -> bool:
    """Aplica los patrones como git: gana el último que coincide y ``!`` lo niega."""
    ignored = False
    for pattern in patterns:
        negated = pattern.startswith("!")
        body = pattern.removeprefix("!")
        if body.endswith("/") and not is_dir:
            continue
        body = body.strip("/")
        target = relative if "/" in body else relative.rsplit("/", 1)[-1]
        if fnmatch.fnmatchcase(target, body):
            ignored = not negated
    return ignored


def _walk_without_ignored(root: Path) -> Iterator[Path]:
    patterns = _ignore_patterns(root)
    for directory, subdirs, files in os.walk(root):
        relative_dir = Path(directory).relative_to(root).as_posix()
        prefix = "" if relative_dir == "." else f"{relative_dir}/"
        subdirs[:] = [
            name
            for name in subdirs
            if name != ".git" and not _is_ignored(prefix + name, True, patterns)
        ]
        for name in files:
            if not _is_ignored(prefix + name, False, patterns):
                yield Path(directory) / name


@cache
def _repo_files() -> tuple[Path, ...]:
    return tuple(versioned_files(REPO))


def _markdown_files() -> list[Path]:
    return [path for path in _repo_files() if path.suffix == ".md"]


def _read(relative: str) -> str:
    return (REPO / relative).read_text(encoding="utf-8")


def _lines_outside_code(text: str) -> Iterator[tuple[int, str]]:
    """Líneas fuera de los bloques de código, sin el código inline."""
    in_fence = False
    for number, line in enumerate(text.splitlines(), start=1):
        if _FENCE_RE.match(line):
            in_fence = not in_fence
        elif not in_fence:
            yield number, _INLINE_CODE_RE.sub("", line)


def broken_links(markdown_files: Iterable[Path], root: Path) -> list[str]:
    """Links relativos que no apuntan a nada, como ``archivo:línea -> link``."""
    broken = []
    for md_file in markdown_files:
        for number, line in _lines_outside_code(md_file.read_text(encoding="utf-8")):
            for target in _LINK_RE.findall(line):
                path = unquote(target.split("#", 1)[0])
                if not path or _URL_SCHEME_RE.match(path) or (md_file.parent / path).exists():
                    continue
                if PENDING not in line:
                    broken.append(f"{md_file.relative_to(root).as_posix()}:{number} -> {target}")
    return broken


def empty_readmes(files: Iterable[Path], root: Path) -> list[str]:
    return [
        path.relative_to(root).as_posix()
        for path in files
        if path.name == "README.md" and not path.read_text(encoding="utf-8").strip()
    ]


def _section(text: str, heading: str) -> str:
    """Texto entre ``## heading`` y el ``##`` siguiente."""
    match = re.search(
        rf"^## {re.escape(heading)}\s*$(.*?)(?=^## |\Z)", text, re.MULTILINE | re.DOTALL
    )
    assert match, f"falta la sección {heading!r}"
    return match[1]


def _table_rows(text: str) -> list[dict[str, str]]:
    """Rows de las tablas Markdown del texto, con las celdas por nombre de columna."""
    rows: list[dict[str, str]] = []
    header: list[str] = []
    previous = ""
    for line in (line.strip() for line in text.splitlines()):
        if not line.startswith("|"):
            header, previous = [], ""
            continue
        cells = [cell.strip() for cell in line.strip("|").split("|")]
        if _SEPARATOR_RE.match(line):
            header = [cell.strip() for cell in previous.strip("|").split("|")]
        elif header:
            rows.append(dict(zip(header, cells, strict=False)))
        previous = line
    return rows


def test_los_links_relativos_de_la_documentacion_existen():
    assert broken_links(_markdown_files(), REPO) == []


def test_un_link_roto_hace_fallar_el_chequeo(tmp_path):
    doc = tmp_path / "doc.md"
    doc.write_text("[existe](doc.md), [roto](falta.md) y [web](https://x.org)\n", encoding="utf-8")

    assert broken_links([doc], tmp_path) == ["doc.md:1 -> falta.md"]


def test_los_links_en_codigo_o_pendientes_de_corrida_no_se_chequean(tmp_path):
    doc = tmp_path / "doc.md"
    doc.write_text(
        f"```\n[a](falta.md)\n```\n`[b](falta.md)`\n[c](falta.csv) ({PENDING})\n",
        encoding="utf-8",
    )

    assert broken_links([doc], tmp_path) == []


def test_ningun_readme_versionado_esta_vacio():
    assert empty_readmes(_repo_files(), REPO) == []


def test_un_readme_vacio_hace_fallar_el_chequeo(tmp_path):
    (tmp_path / "README.md").write_text(" \n", encoding="utf-8")

    assert empty_readmes(versioned_files(tmp_path), tmp_path) == ["README.md"]


def test_sin_git_se_saltean_los_archivos_ignorados(tmp_path):
    (tmp_path / ".gitignore").write_text("salida/\n*.log\n!guardar.log\n", encoding="utf-8")
    (tmp_path / ".git" / "info").mkdir(parents=True)
    (tmp_path / ".git" / "info" / "exclude").write_text("local/\n", encoding="utf-8")
    for name in ("a.md", "sub/b.md", "salida/c.md", "local/d.md", "e.log", "guardar.log"):
        (tmp_path / name).parent.mkdir(parents=True, exist_ok=True)
        (tmp_path / name).write_text("x", encoding="utf-8")

    found = {path.relative_to(tmp_path).as_posix() for path in versioned_files(tmp_path)}

    assert found == {".gitignore", "a.md", "sub/b.md", "guardar.log"}


def test_el_readme_tiene_las_secciones_en_orden():
    assert _HEADING_RE.findall(_read("README.md")) == README_SECTIONS


def test_cada_item_del_estado_de_la_entrega_tiene_donde_verse():
    rows = _table_rows(_section(_read("README.md"), "Estado de la entrega 2"))

    assert len(rows) >= 13
    assert [
        row["Ítem de la consigna"]
        for row in rows
        if not _LINK_RE.search(row["Dónde se ve"]) and PENDING not in row["Dónde se ve"]
    ] == []


def test_el_indice_de_docs_lista_todos_los_documentos():
    docs = REPO / "docs"
    documents = {
        path.relative_to(docs).as_posix()
        for path in _markdown_files()
        if path.is_relative_to(docs) and path != docs / "README.md"
    }
    linked = {
        unquote(target.split("#", 1)[0]) for target in _LINK_RE.findall(_read("docs/README.md"))
    }

    assert sorted(documents - linked) == []


def test_config_readme_documenta_cada_variable_del_ejemplo():
    variables = set(_ENV_VAR_RE.findall(_read("config/.env.example")))
    readme = _read("config/README.md")

    assert variables
    assert sorted(name for name in variables if f"`{name}`" not in readme) == []


def test_cada_decision_tiene_los_siete_campos():
    entries = [
        entry
        for entry in re.split(r"^## ", _read("DECISIONS.md"), flags=re.MULTILINE)
        if re.match(r"D-\d+: ", entry)
    ]

    assert len(entries) >= 20
    for entry in entries:
        title = entry.splitlines()[0]
        assert [field for field in DECISION_FIELDS if f"- **{field}**: " not in entry] == [], title
        status = re.search(r"- \*\*Estado\*\*: (.+)", entry)
        assert status and re.match(r"(vigente|abierta|reemplazada por D-\d+)$", status[1]), title


def test_las_decisiones_abiertas_tienen_prioridad_fecha_y_propuesta():
    rows = _table_rows(_section(_read("DECISIONS.md"), "Decisiones abiertas"))

    assert rows
    for row in rows:
        assert row["Prioridad"] in PRIORITIES, row["Id"]
        assert re.match(r"\d{4}-\d{2}-\d{2}$", row["Fecha límite"]), row["Id"]
        assert row["Opción propuesta"], row["Id"]


def test_el_diagrama_muestra_la_arquitectura_y_el_flujo_de_steps_en_mermaid():
    text = _read("docs/diagramas/arquitectura.md")
    blocks = re.findall(r"```mermaid\n(.*?)```", text, re.DOTALL)
    mermaid = "\n".join(blocks)

    assert len(blocks) == 2
    for expected in ("quarantine/bronze", "quarantine/silver", "_manifests", "Gate", *STEPS):
        assert expected in mermaid, expected
    assert re.search(r"^- ", _section(text, "Cambios respecto de la Figura 1 del informe"), re.M)


def test_la_matriz_no_tiene_rows_sin_componente_evidencia_o_estado():
    rows = _table_rows(_read("docs/matriz-requisitos.md"))

    assert len(rows) >= 13 + 14
    assert [row for row in rows if not all(row.values())] == []
    assert [row for row in rows if not re.match(r"(hecho|parcial|backlog)\b", row["Estado"])] == []


def test_el_gobierno_tiene_cinco_secciones_y_el_linaje_de_cada_tabla():
    text = _read("docs/gobierno/README.md")
    lineage = _table_rows(_section(text, "Linaje"))

    assert _HEADING_RE.findall(text) == GOVERNANCE_SECTIONS
    assert {row["Tabla de Cassandra"].strip("`") for row in lineage} == {
        ddl.stem for ddl in SCHEMA_DIR.glob("*.cql")
    }
    assert all(row["Landing"] for row in lineage)


def test_el_plan_de_correcciones_tiene_una_row_por_dimension_de_la_devolucion():
    rows = _table_rows(_section(_read("docs/plan-correcciones.md"), "Devolución de la entrega 1"))

    assert [row["Dimensión"] for row in rows] == FEEDBACK_DIMENSIONS
    assert [row for row in rows if not all(row.values())] == []


def test_el_backlog_tiene_tres_secciones_con_prioridad_estimacion_y_origen():
    text = _read("docs/backlog.md")

    assert _HEADING_RE.findall(text) == BACKLOG_SECTIONS
    for heading in BACKLOG_SECTIONS:
        rows = _table_rows(_section(text, heading))
        assert rows, heading
        for row in rows:
            assert row["Prioridad"] in PRIORITIES, row["Ítem"]
            assert row["Estimación"].isdigit(), row["Ítem"]
            assert row["Origen"], row["Ítem"]


def test_el_notebook_de_exploracion_tiene_el_nombre_que_cita_el_informe():
    names = {path.relative_to(REPO).as_posix() for path in _repo_files()}
    old_name = re.compile(r"(?<!00_)exploracion_datos\.ipynb")
    texts = [path for path in _repo_files() if path.suffix in {".md", ".py", ".ipynb"}]

    assert f"notebooks/{NOTEBOOK}" in names
    assert f"notebooks/{NOTEBOOK.removeprefix('00_')}" not in names
    assert [
        path.relative_to(REPO).as_posix()
        for path in texts
        if old_name.search(path.read_text(encoding="utf-8"))
    ] == []


def _subsections(text: str) -> dict[str, str]:
    """Texto de cada sección ``##`` o ``###``, por título sin backticks."""
    return {title.strip("`"): body for title, body in _SUBSECTION_RE.findall(text)}


def _column_names(text: str) -> set[str]:
    """Columnas nombradas en la celda ``Columna`` de las tablas del texto."""
    cells = " ".join(row.get("Columna", "") for row in _table_rows(text))
    return set(_CODE_SPAN_RE.findall(cells))


def _documented_columns(dataset: str, sections: dict[str, str]) -> set[str]:
    """Columnas que el diccionario documenta para ``zona/dataset``."""
    zone, _, name = dataset.partition("/")
    if dataset in sections:
        return _column_names(sections[dataset])
    if zone == "silver" and f"bronze/{name}" in sections:
        added = [
            row["Columna"]
            for row in _table_rows(sections["Maestros de Silver"])
            if row["Dataset"] == f"`{dataset}`"
        ]
        source = _column_names(sections[f"bronze/{name}"]) - {"ingest_date"}
        return source | SILVER_MASTER_COLUMNS | set(_CODE_SPAN_RE.findall(" ".join(added)))
    if zone == "quarantine":
        # Lo rechazado conserva las columnas de entrada, que salen del dataset de Bronze.
        quarantine = sections["Quarantine"]
        columns = _column_names(quarantine) | _documented_columns(
            f"bronze/{name.partition('/')[2]}", sections
        )
        for row in _table_rows(quarantine):
            pattern = re.sub(r"<[^>]+>", "*", row.get("Dataset", "").strip("`"))
            if pattern and fnmatch.fnmatchcase(name, pattern.removeprefix("quarantine/")):
                for span in _CODE_SPAN_RE.findall(row["Columnas de entrada"]):
                    columns |= _documented_columns(span, sections) if "/" in span else {span}
        return columns
    return set()


def undocumented_columns(dictionary: str, schemas_file: Path) -> list[str]:
    """Columnas de ``schemas.json`` que el diccionario no documenta, como ``dataset.columna``."""
    sections = _subsections(dictionary)
    schemas: dict[str, list[dict[str, str]]] = json.loads(schemas_file.read_text(encoding="utf-8"))
    return [
        f"{dataset}.{column['columna']}"
        for dataset, columns in sorted(schemas.items())
        for column in columns
        if column["columna"] not in _documented_columns(dataset, sections)
    ]


def quality_mismatches(catalog: Path, rule_counts: Path) -> list[str]:
    """Reglas de ``rule_counts.csv`` que el catálogo no muestra con las mismas rows y su muestra."""
    documented = {}
    for row in _table_rows(catalog.read_text(encoding="utf-8")):
        if "Muestra" in row:
            # La tabla de eventos no tiene columna Dataset: todas sus reglas son de usage_events.
            dataset = row.get("Dataset", "usage_events").strip("`")
            documented[(dataset, row["`rule_id`"].strip("`"))] = row
    problems = []
    with rule_counts.open(newline="", encoding="utf-8") as csv_file:
        for rule in csv.DictReader(csv_file):
            name = f"{rule['dataset']}/{rule['rule_id']}"
            row = documented.get((rule["dataset"], rule["rule_id"]))
            if row is None:
                problems.append(f"{name}: no está en el catálogo")
                continue
            rows = rule["rows_affected"]
            if row["Rows"] != rows:
                problems.append(f"{name}: {row['Rows']} rows en el catálogo, {rows} en la corrida")
            sample = rule_counts.parent / SAMPLES_DIR / f"{rule['dataset']}__{rule['rule_id']}.csv"
            links = {
                (catalog.parent / unquote(link)).resolve()
                for link in _LINK_RE.findall(row["Muestra"])
            }
            if int(rows) > 0 and (sample.resolve() not in links or not sample.is_file()):
                problems.append(f"{name}: falta el link a su muestra")
    return problems


def test_el_diccionario_documenta_cada_columna_de_cassandra():
    sections = _subsections(_section(_read("docs/diccionario-datos.md"), "Cassandra/AstraDB"))

    for ddl in sorted(SCHEMA_DIR.glob("*.cql")):
        columns = _DDL_COLUMN_RE.findall(ddl.read_text(encoding="utf-8"))
        assert columns, ddl.name
        assert sorted(set(columns) - _column_names(sections[ddl.stem])) == [], ddl.stem


def test_el_diccionario_documenta_cada_columna_del_lake():
    schemas_file = DELIVERY / SCHEMAS_FILE
    if not schemas_file.is_file():
        pytest.skip(f"Todavía no hay {SCHEMAS_FILE}: lo genera ./infra/run.sh evidence")

    assert undocumented_columns(_read("docs/diccionario-datos.md"), schemas_file) == []


def test_el_catalogo_de_calidad_coincide_con_la_corrida_de_entrega():
    rule_counts = DELIVERY / QUALITY_DIR / RULE_COUNTS_FILE
    if not rule_counts.is_file():
        pytest.skip(f"Todavía no hay {RULE_COUNTS_FILE}: lo genera ./infra/run.sh evidence")

    assert quality_mismatches(REPO / "docs" / "calidad.md", rule_counts) == []


DICTIONARY_SAMPLE = """\
### `bronze/users`

| Columna | Tipo | Descripción |
| :--- | :--- | :--- |
| `user_id` | string | Usuario. |
| `ingest_ts`, `ingest_date` | timestamp, date | Columnas técnicas. |

## Silver

### Maestros de Silver

| Dataset | Columna | Tipo | Descripción |
| :--- | :--- | :--- | :--- |
| `silver/users` | `tags` | array<string> | Nueva. |

## Quarantine

| Columna | Tipo | Descripción |
| :--- | :--- | :--- |
| `_rule_id` | string | Regla. |

| Dataset | Grain | Columnas de entrada |
| :--- | :--- | :--- |
| `quarantine/bronze/<maestro>` | Una row. | Las de la fuente y `_corrupt_record`. |
"""


@pytest.mark.parametrize(
    ("dataset", "column", "missing"),
    [
        ("bronze/users", "ingest_date", []),
        ("silver/users", "tags", []),
        ("silver/users", "dq_flags", []),
        ("silver/users", "ingest_date", ["silver/users.ingest_date"]),
        ("quarantine/bronze/users", "_corrupt_record", []),
        ("quarantine/bronze/users", "_rule_id", []),
        ("bronze/users", "email", ["bronze/users.email"]),
        ("gold/otro", "x", ["gold/otro.x"]),
    ],
)
def test_el_diccionario_se_compara_con_los_schemas_del_lake(tmp_path, dataset, column, missing):
    schemas_file = tmp_path / SCHEMAS_FILE
    schemas = {dataset: [{"columna": column, "tipo": "string"}]}
    schemas_file.write_text(json.dumps(schemas), encoding="utf-8")

    assert undocumented_columns(DICTIONARY_SAMPLE, schemas_file) == missing


def _quality_files(tmp_path: Path, catalog_rows: str, sample: bool = True) -> tuple[Path, Path]:
    rule_counts = tmp_path / "evidencia" / RULE_COUNTS_FILE
    (rule_counts.parent / SAMPLES_DIR).mkdir(parents=True)
    rule_counts.write_text(
        "layer,dataset,rule_id,action,rows_affected\n"
        "silver,usage_events,VALUE_NULL,flag,3\n"
        "silver,users,LOGIN_BEFORE_CREATED,flag,0\n",
        encoding="utf-8",
    )
    if sample:
        (rule_counts.parent / SAMPLES_DIR / "usage_events__VALUE_NULL.csv").write_text("x\n")
    catalog = tmp_path / "docs" / "calidad.md"
    catalog.parent.mkdir()
    catalog.write_text(catalog_rows, encoding="utf-8")
    return catalog, rule_counts


EVENT_RULES = """\
| `rule_id` | Condición | Acción | Rows | Muestra |
| :--- | :--- | :--- | ---: | :--- |
| `VALUE_NULL` | `value` null | flag | {rows} | [muestra](../evidencia/samples/usage_events__VALUE_NULL.csv) |

| `rule_id` | Dataset | Condición | Acción | Rows | Muestra |
| :--- | :--- | :--- | :--- | ---: | :--- |
| `LOGIN_BEFORE_CREATED` | `users` | login viejo | flag | 0 | |
"""


def test_el_catalogo_de_calidad_con_las_rows_y_muestras_de_la_corrida_pasa(tmp_path):
    catalog, rule_counts = _quality_files(tmp_path, EVENT_RULES.format(rows=3))

    assert quality_mismatches(catalog, rule_counts) == []


def test_el_catalogo_de_calidad_con_otras_rows_o_sin_muestra_falla(tmp_path):
    catalog, rule_counts = _quality_files(tmp_path, EVENT_RULES.format(rows=5), sample=False)
    catalog.write_text(catalog.read_text(encoding="utf-8").replace("`LOGIN", "`OTRA"))

    assert quality_mismatches(catalog, rule_counts) == [
        "usage_events/VALUE_NULL: 5 rows en el catálogo, 3 en la corrida",
        "usage_events/VALUE_NULL: falta el link a su muestra",
        "users/LOGIN_BEFORE_CREATED: no está en el catálogo",
    ]
