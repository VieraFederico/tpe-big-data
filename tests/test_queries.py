import pytest

from src.serving.queries import format_table, parse_param


def test_format_table_alinea_columnas_y_muestra_none_vacio():
    assert format_table(["a", "bb"], [(1, None)]) == "a | bb\n--+---\n1 |   "


def test_format_table_sin_filas():
    assert format_table(["id"], []) == "id\n--\n(sin filas)"


def test_format_table_trunca_y_avisa():
    table = format_table(["id"], [("x",), ("y",)], max_rows=1)

    assert table.splitlines()[-2:] == ["x ", "... (1 filas más en el .csv)"]


def test_format_table_sin_columnas():
    assert format_table([], []) == "(sin columnas)"


@pytest.mark.parametrize(
    ("item", "expected"),
    [
        ("org_id=org_001", ("org_id", "org_001")),
        ("org_id=123", ("org_id", "123")),
        ("comment=nan", ("comment", "nan")),
        ("limit:int=10", ("limit", 10)),
        ("threshold:float=1.5", ("threshold", 1.5)),
        ("note:str=a=b", ("note", "a=b")),
        ("empty=", ("empty", "")),
    ],
)
def test_parse_param(item, expected):
    assert parse_param(item) == expected


@pytest.mark.parametrize(
    ("item", "message"),
    [
        ("sin_igual", "CLAVE=VALOR"),
        ("=valor", "CLAVE=VALOR"),
        ("limit:bool=1", "desconocido"),
        ("limit:int=diez", "no es int"),
    ],
)
def test_parse_param_invalido(item, message):
    with pytest.raises(ValueError, match=message):
        parse_param(item)
