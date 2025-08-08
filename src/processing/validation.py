"""
validation.py -- Funciones para validar los valores de nuestros datos y que sean consistentes.
Proyecto Chaos_Signals ·  Python 3.12
"""

import pandas as pd
#import pandas_market_calendars as mcal


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
    El DataFrame debe tener el índice de tipo datetime. 
    WIP: hay que añadir calendario para Sotck/Forex/Índices respectivamente.
    """
    idx = pd.to_datetime(df.index)
    idx = idx.sort_values()
    full_idx = pd.date_range(idx.min(), idx.max(), freq=freq)
    missing = full_idx.difference(idx)
    assert missing.empty, f"Fechas faltantes detectadas: {len(missing)} gaps"

def check_price_relations(
        df: pd.DataFrame,
        cols=('open', 'high', 'low', 'close'),
        eps = 5e-3
        ) -> None:
    """
    Vemos que low <= open, close >=high para todos los registros.
    """
    o, h, l, c = (df[c].astype(float) for c in cols)

    bad = (
        (l -h > eps) |                      # low <= high
        (o - h > eps) | (l - o > eps) |     # low <= open <= high
        (c - h > eps) | (l - o > eps)       # low <= close <= high
    )
    count = int(bad.sum())
    if count:
        sample = df.index[bad][:10].tolist()
        raise AssertionError(f"{count} registros con OHLC inconsistentes (eps = {eps}). Algunos: {sample}")

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
#    check_date_continuity(df, dates_freq) # está en proceso
    check_price_relations(df)
    check_positive_values(df, pos_cols)


# -----------------------------------------------------------------------------------
# Funciones adicionales    ─────────────────────────────────────────────────────────────
# -----------------------------------------------------------------------------------

def ohlc_violation_report(
        df: pd.DataFrame,
        cols=('open', 'high', 'low', 'close'),
        eps=1e-5,
        n=10
    ): 
    o, h, l, c = (df[c].astype(float) for c in cols)
    v = pd.DataFrame(index=df.index)
    v['A_low>high'] = l > h + eps
    v['B_open>high'] = o > h + eps
    v['C_open<low'] = o < l - eps
    v['D_close>high'] = c > h + eps
    v['E_close<low'] = c < l - eps
    v['any'] = v[['A_low>high','B_open>high','C_open<low','D_close>high','E_close<low']].any(axis=1)
    bad = v[v['any']].copy()

    # añadimos deltas para ver la magnitud
    bad['d_l_h'] = df['low'] - df['high']
    bad['d_o_h'] = df['open'] - df['high']
    bad['d_o_l'] = df['open'] - df['high']
    bad['d_c_h'] = df['close'] - df['high']
    bad['d_c_l'] = df['close'] - df['low']

    print("Resumen por tipo:\n", bad[['A_low>high','B_open>high','C_open<low','D_close>high','E_close<low']].sum())
    print("\nTop ejemplos:")
    return bad.head(n)