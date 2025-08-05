"""
data_cleaning.py -- Funciones de limpieza de datos raw para el proyecto de Chaos_Signals.
Proyecto Chaos-IV_Signals · Python 3.12
"""


# -----------------------------------------------------------------------------------
# Configuración global  ─────────────────────────────────────────────────────────────
# -----------------------------------------------------------------------------------
from pathlib import Path
import pandas as pd

from readers import list_raw_files
from cleaners import clean_yahoo, clean_ibkr

PROCESSED_DIR = Path(__file__).resolve().parents[2] / "data" / "processed"
PROCESSED_DIR.mkdir(exist_ok=True)

# Mappeo de funciones
CLEANERS = {
    'yahoo': clean_yahoo,
    'ibkr': clean_ibkr
}


# -----------------------------------------------------------------------------------
# Funciones públicas  ───────────────────────────────────────────────────────────────
# -----------------------------------------------------------------------------------

def process_source(source: str):  
    """
    Limpia todos los archivos de una fuente dada
    """
    if source not in CLEANERS:
        raise ValueError(f"Source no configurada: {source}")
    
    cleaner = CLEANERS[source]
    for path in list_raw_files(source):

# -----------------------------------------------------------------------------------
# Ejecución   ───────────────────────────────────────────────────────────────────────
# -----------------------------------------------------------------------------------

if __name__ == "__main__":
    for src in CLEANERS:
