# Tests

Tests con `pytest` sobre una SparkSession local. Cada test usa un lake temporario propio (`tmp_path`), así que no tocan `data/datalake`.

```bash
./infra/run.sh test                 # todos (en Docker)
./infra/run.sh test -k stream -v    # filtrar; los args van directo a pytest
python -m pytest tests              # sin Docker (Colab o entorno con pyspark)
```

[`test_docs.py`](test_docs.py) revisa la documentación: que los links relativos de los `.md` versionados apunten a algo, que ningún `README.md` esté vacío, las secciones del README principal, que `docs/README.md` liste todos los documentos de `docs/` y que `config/README.md` explique todas las variables de `config/.env.example`. Un link en una línea marcada como "pendiente de corrida" puede no existir todavía: es evidencia que genera `./infra/run.sh evidence`. En Docker lee el repo desde `/repo` (ver [`infra/README.md`](../infra/README.md#volúmenes)).
