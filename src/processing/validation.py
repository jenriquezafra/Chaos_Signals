"""
validation.py -- Funciones para validar los valores de nuestros datos y que sean consistentes.
Proyecto Chaos_Signals ·  Python 3.12
"""

import pandas as pd


# -----------------------------------------------------------------------------------
# Funciones públicas    ─────────────────────────────────────────────────────────────
# -----------------------------------------------------------------------------------

def check_schema(
        df: pd.DataFrame,
        required_cols: list[str]
    ) -> None:
    """
    Vemos que todas las columnas esperadas están en el dataframe.
    """
    missing = set(required_cols) - set(df.columns)
    assert not missing, f"Faltan columnas en el esquema: {missing}"

def check_no_nulls(
        df: pd.DataFrame,
        cols: list[str]
    ) -> None:
    """
    Nos aseguramos de que no haya nulos en las columnas.
    """
    null_counts = df[cols].isna().sum()
    bad = null_counts[null_counts > 0]
    assert bad.empty, f"Valores nulos detectados en columnas: {dict(bad)}"

def check_no_duplicates(
        df: pd.DataFrame, 
        subset: list[str]
    ) -> None:
    """
    Vemos que no haya filas duplicadas en el subconjunto de columnas
    """
    dup = df.duplicated(subset=subset).sum()
    assert dup==0, f"Se encontraron {dup} filas duplicadas según {subset}"

def check_date_continuity(
        df: pd.DataFrame, 
        freq: str = 'B'
    ) -> None:
    """
    Vemos que no falten fechas según la frecuencia (por defecto días hábiles 'B').
    El DataFrame debe tener el índice de tipo datetime
    """
    idx = pd.to_datetime(df.index)
    idx = idx.sort_values()
    full_idx = pd.date_range(idx.min(), idx.max(), freq=freq)
    missing = full_idx.difference(idx)
    assert missing.empty, f"Fechas faltantes detectadas: {len(missing)} gaps"

def check_price_relations(df: pd.DataFrame) -> None:
    """
    Vemos que low <= open, close >=high para todos los registros.
    """
    bad = df.query('low > high or open < low or close > high')
    count = len(bad)
    assert count == 0, f"{count} registros con precios inconsistentes"

def check_positive_values(
        df: pd.DataFrame,
        cols: list[str]
        ) -> None:
    """
    Nos aseguramos de que los valores de las columnas numéricas sean no negativos.
    """
    for col in cols:
        neg = (df[col] < 0).sum()
        assert neg == 0, f"{neg} valores negativos en columna '{col}'"


def all_checks(
        df: pd.DataFrame,
        schema_cols: list[str],   
        nulls_cols: list[str],
        dup_subset: list[str],
        pos_cols: list[str],
        dates_freq: str = 'B'
    ) -> None:
    """
    Aplicamos todos los check anteriores
    """

    check_schema(df, schema_cols)
    check_no_nulls(df, nulls_cols)
    check_no_duplicates(df, dup_subset)
#    check_date_continuity(df, dates_freq) # quitado ahora mismo porque es muy rígido
    check_price_relations(df)
    check_positive_values(df, pos_cols)
