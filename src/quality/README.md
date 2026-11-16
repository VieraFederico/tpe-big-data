# Calidad

| Módulo | Qué hace |
| :--- | :--- |
| [`catalog.py`](catalog.py) | Todas las reglas (quarantine) y flags de Bronze y Silver, con su condición y su motivo. |
| [`rules.py`](rules.py) | `split_by_rules`: separa válidas y quarantine, con `_rule_id`, `_quarantine_reason`, `_raw` y `_quarantine_ts`. |
| [`flags.py`](flags.py) | Arma `dq_flags` (ordenado, sin repetidos, vacío si no hay ninguno). |
| [`report.py`](report.py) | `rule_counts.csv` y las muestras de cada regla en `evidence/quality/<run_id>/`. |

Catálogo explicado en [`docs/calidad.md`](../../docs/calidad.md).
