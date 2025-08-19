"""
cleaners.py -- Funciones para la limpieza de datos. Usado para la limpieza posterior.
Proyecto Chaos_Signals ·  Python 3.12
"""

import pandas as pd
import numpy as np

# -----------------------------------------------------------------------------------
# Configuración global  ─────────────────────────────────────────────────────────────
# -----------------------------------------------------------------------------------



# -----------------------------------------------------------------------------------
# Funciones públicas    ─────────────────────────────────────────────────────────────
# -----------------------------------------------------------------------------------

def clean_yahoo(df: pd.DataFrame) -> pd.DataFrame:
    """
    Limpieza de datos de Yahoo
    """    
    # limpiamos el header
    df.columns = df.columns.get_level_values(0)
    df.columns.name = None                              # quitamos el nombre pegado al index (Price)
    df.index = pd.to_datetime(df.index, dayfirst=True)  # parseamos la fecha
    df.index.name = "date"                              # renombramos
    prices = df['close']
    # creamos nuevas columnas
    gap_threshold = 0.01                                # ajustar
    df['daily_return'] = prices.pct_change()
    df['log_return'] = np.log(prices / prices.shift(1)).dropna()
    df['range'] = df['high'] - df['low']
    df['is_gap'] = (df['open']-df['close'].shift(1)).abs() > gap_threshold

    # tratamos los NaNs
    if df.isna().sum().sum() > 0:
        df = df.dropna()

    return df


def clean_ibkr(df: pd.DataFrame) -> pd.DataFrame:
    """
    Limpieza de datos de IBKR
    """
    pass # WI