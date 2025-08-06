"""
readers.py -- Funciones para la lectura de archivos. Usado para la limpieza posterior.
Proyecto Chaos_Signals ·  Python 3.12
"""

from pathlib import Path
from typing import List
import pandas as pd

# -----------------------------------------------------------------------------------
# Configuración global  ─────────────────────────────────────────────────────────────
# -----------------------------------------------------------------------------------

RAW_DIR = Path(__file__).resolve().parents[2] / "data" / "raw" # no se si parents 2 o 1


# -----------------------------------------------------------------------------------
# Funciones públicas    ─────────────────────────────────────────────────────────────
# -----------------------------------------------------------------------------------

def read_raw(source: str) -> List[pd.DataFrame]:
    """
    Lee todos los .parquet de data/raw/{source} y devuelve una lista de DataFrames independientes.
    """
    files = list_raw_files(source)
    if not files:
        raise ValueError(f"No se encontraron ficheros parquet para la fuente {source}")
    return [pd.read_parquet(f) for f in files]


def list_raw_files(source: str) -> List[Path]:
    """
    Devuelve todos los .parquet de la carpeta raw/{source}.
    """
    folder = RAW_DIR / source
    if not folder.exists():
        raise ValueError(f"Fuente desconocida: {source}")
    return list(folder.glob('*.parquet'))
