import pytest

from src.serving.queries import parse_param


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
