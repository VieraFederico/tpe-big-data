"""Inventario de los datasets que hay en el lake, para contarlos y describirlos en la evidencia."""

from dataclasses import dataclass
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from src.common.lake import dataset_path, read_dataset
from src.config import Settings, Zone

# Carpeta de la evidencia de la entrega, dentro de EVIDENCE_PATH.
DELIVERY_DIR = "entrega-2"
# Zonas con un dataset por carpeta.
DATA_ZONES: tuple[Zone, ...] = (Zone.BRONZE, Zone.SILVER, Zone.GOLD)
# La cuarentena se separa por capa: quarantine/<capa>/<dataset>/.
QUARANTINE_LAYERS: tuple[Zone, ...] = (Zone.BRONZE, Zone.SILVER)


@dataclass(frozen=True, order=True)
class LakeDataset:
    """Un dataset del lake.

    Attributes:
        zone: Zona donde vive (``bronze``, ``silver``, ``gold`` o ``quarantine``).
        dataset: Nombre dentro de la zona; en cuarentena incluye la capa, p. ej. ``bronze/users``.
    """

    zone: Zone
    dataset: str

    @property
    def name(self) -> str:
        """Ruta relativa a la raíz del lake, p. ej. ``quarantine/bronze/users``."""
        return f"{self.zone}/{self.dataset}"


def lake_datasets(settings: Settings) -> list[LakeDataset]:
    """Lista los datasets de Bronze, Silver, Gold y las dos cuarentenas, ordenados por nombre."""
    found: list[LakeDataset] = []
    for zone in DATA_ZONES:
        found.extend(
            LakeDataset(zone, folder.name)
            for folder in _subfolders(Path(settings.zone_path(zone)))
        )
    for layer in QUARANTINE_LAYERS:
        layer_folder = Path(settings.zone_path(Zone.QUARANTINE)) / layer
        found.extend(
            LakeDataset(Zone.QUARANTINE, f"{layer}/{folder.name}")
            for folder in _subfolders(layer_folder)
        )
    return sorted(found, key=lambda item: item.name)


def read_if_present(
    spark: SparkSession, settings: Settings, item: LakeDataset
) -> DataFrame | None:
    """Lee el dataset, o devuelve None si no tiene archivos parquet (p. ej. cuarentena vacía)."""
    path = Path(dataset_path(settings, item.zone, item.dataset))
    if not path.is_dir() or next(path.rglob("*.parquet"), None) is None:
        return None
    return read_dataset(spark, settings, item.zone, item.dataset)


def _subfolders(folder: Path) -> list[Path]:
    """Subcarpetas visibles de ``folder``; vacío si no existe."""
    if not folder.is_dir():
        return []
    return sorted(
        child
        for child in folder.iterdir()
        if child.is_dir() and not child.name.startswith(("_", "."))
    )
