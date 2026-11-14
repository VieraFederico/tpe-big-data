"""Excepciones del serving (Cassandra / AstraDB)."""


class ServingError(Exception):
    """Base de los errores del serving; permite capturarlos todos juntos."""


class AstraApiError(ServingError):
    """La DevOps API de Astra respondió con un error HTTP.

    Attributes:
        status_code: Código HTTP de la respuesta.
    """

    def __init__(self, status_code: int, message: str) -> None:
        super().__init__(message)
        self.status_code = status_code


class KeyspaceNotFoundError(ServingError):
    """El keyspace configurado no existe en el destino.

    Attributes:
        keyspace: Nombre del keyspace buscado.
    """

    def __init__(self, keyspace: str, message: str) -> None:
        super().__init__(message)
        self.keyspace = keyspace


class WriteFailedError(ServingError):
    """Cassandra rechazó filas durante una carga.

    Attributes:
        table: Tabla destino.
        rows_failed: Cantidad de filas que no se pudieron escribir.
    """

    def __init__(self, table: str, rows_failed: int) -> None:
        super().__init__(
            f"{rows_failed} filas fallaron al escribir {table}; ver logs de los workers"
        )
        self.table = table
        self.rows_failed = rows_failed


class GoldNotPublishableError(ServingError):
    """Gold no se publica porque su manifiesto falta o no terminó en ``ok``.

    Attributes:
        status: Estado leído de ``_manifests/gold.json``, o ``missing`` si no existe.
    """

    def __init__(self, status: str) -> None:
        super().__init__(
            f"No se publica Gold: el estado de _manifests/gold.json es {status!r} "
            "(tiene que ser 'ok'); correr el paso gold y revisar la conciliación"
        )
        self.status = status
