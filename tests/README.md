# Tests

Tests con `pytest` sobre una SparkSession local. Cada test usa un lake temporario propio (`tmp_path`), así que no tocan `data/datalake`.

```bash
./infra/run.sh test                 # todos (en Docker)
./infra/run.sh test -k stream -v    # filtrar; los args van directo a pytest
python -m pytest tests              # sin Docker (Colab o entorno con pyspark)
```
