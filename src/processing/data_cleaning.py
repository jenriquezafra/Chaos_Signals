"""
data_cleaning.py -- Funciones de limpieza de datos raw para el proyecto de Chaos_Signals.
Proyecto Chaos-IV_Signals · Python 3.12
"""


# -----------------------------------------------------------------------------------
# Configuración global  ─────────────────────────────────────────────────────────────
# -----------------------------------------------------------------------------------
from pathlib import Path
import pandas as pd

from validation import all_checks
from readers import list_raw_files
from cleaners import clean_yahoo, clean_ibkr

PROCESSED_DIR = Path(__file__).resolve().parents[2] / "data" / "processed"
PROCESSED_DIR.mkdir(exist_ok=True)

# Mappeo de funciones
CLEANERS = {
    'yahoo': clean_yahoo,
    'ibkr': clean_ibkr
}

# para la validación
schema_cols = ['close', 'high', 'low', 'open', 'volume', 'daily_return', 'log_return', 'range', 'is_gap']
nulls_cols = ['close', 'high', 'low', 'open', 'volume']
dup_subset = schema_cols
pos_cols = ['close', 'high', 'low', 'open', 'volume', 'range']
dates_freq = 'B'


# -----------------------------------------------------------------------------------
# Funciones públicas  ───────────────────────────────────────────────────────────────
# -----------------------------------------------------------------------------------

def process_source(source: str) -> None:
    """
    Limpia todos los archivos de una fuente dada.
    """
    if source not in CLEANERS:
        raise ValueError(f"Source no configurada: {source}")
    
    # obtenemos las rutas de los raw
    paths = list_raw_files(source)
    cleaner = CLEANERS[source]
    for path in paths:
        # leemos el df
        df = pd.read_parquet(path)

        # informamos por consola
        out_path = PROCESSED_DIR / path.name
        print(f"Procesando {out_path.name} de {source}")

        # aplicamos la limpieza específica
        df_clean = cleaner(df)

        # validamos los valores
        all_checks(
            df, schema_cols, nulls_cols, dup_subset, pos_cols, dates_freq)
    
        # guardamos el resultado 
        df_clean.to_parquet(out_path)




# -----------------------------------------------------------------------------------
# Ejecución   ───────────────────────────────────────────────────────────────────────
# -----------------------------------------------------------------------------------

if __name__ == "__main__":
    for src in CLEANERS:
        process_source(src)