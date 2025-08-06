"""
yahoo.py -- Funciones de extracción y caché de datos spot y opciones de Yahoo Finance.
Proyecto Chaos-IV_Signals ·  Python 3.12
"""

from __future__ import annotations
from pathlib import Path
from typing import Literal
import time
import logging
import pyarrow.feather as feather
import pandas as pd
import yfinance as yf   
from datetime import datetime, timedelta

# -----------------------------------------------------------------------------------
# Configuración global  ─────────────────────────────────────────────────────────────
# -----------------------------------------------------------------------------------
CACHE_SPOT_DIR = Path("data/raw/yahoo").resolve()
CACHE_SPOT_DIR.mkdir(parents=True, exist_ok=True)

RATE_LIMIT_DELAY = 0.6  # yahoo solo deja 2 peticiones/s 
DEFAULT_TZ = "Europe/Madrid"
logger = logging.getLogger(__name__)

START_DATE = (datetime.now() - timedelta(days=365*3)).strftime("%Y-%m-%d") # últimos 3 años
END_DATE = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d") # ayer

TKRS = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'NVDA', 'AMD', 'META',
        'GC=F', 'SPY', 'QQQ', 'EURUSD=X']

# -----------------------------------------------------------------------------------
# Funciones públicas    ─────────────────────────────────────────────────────────────
# -----------------------------------------------------------------------------------
def get_spot(
        ticker: str,
        *, 
        interval: Literal["1d", "60m", "30m", "15m", "5m", "1m"] = "1d",
        auto_adjust: bool = True,
        prepost: bool = True,
        cache: bool = True
) -> pd.DataFrame:
    """
    Descarga OHLCV desde Yahoo Finance 

    Si `cache = True`, intenta leer / escribir Parquet en 
    data/raw/yahoo/spot/{ticker}_{interval}.parquet
    """
    start = START_DATE
    end = END_DATE
    path = _spot_cache_path(ticker, interval)
    if cache and path.exists():
        logger.debug("Cargando OHLCV de caché: %s", path)
        return _read_parquet(path)
    
    # 1) llamada remota
    df = yf.download(
        ticker,
        start=start,
        end=end, 
        interval=interval,
        auto_adjust=auto_adjust,
        prepost=prepost,
        progress=False
    )

    if df.index.tz is None:
        df = df.tz_localize(DEFAULT_TZ, nonexistent="shift_forward")
    else:
        df = df.tz_convert(DEFAULT_TZ)

    # 2) normalización de columnas y tipos
    df.rename(columns=str.lower, inplace=True)
    df.index.name = "timestamp"

    # 3) guarda caché
    if cache:
        _write_parquet(df, path)
        print(f"Guardado {path.name}")
        
    time.sleep(RATE_LIMIT_DELAY)
    return df
    

def update_cache(
        ticker: str,
        *,
        spot_intervals: list[str] = ["1d", "60m"]
) -> None:
    """
    Actualiza en bloque la cache local:
    - recorre `spot_intervals``
    """
    for iv in spot_intervals:
        get_spot(ticker, interval=iv, cache=True)



# -----------------------------------------------------------------------------------
# Helpers privados      ─────────────────────────────────────────────────────────────
# -----------------------------------------------------------------------------------

OTHERS_TICKERS = {"GC=F", "QQQ", "SPY"}

def _spot_cache_path(tkr: str, iv: str) -> Path:
    tag = "latest"
    base_dir = CACHE_SPOT_DIR
    base_dir.mkdir(parents=True, exist_ok=True)
    return base_dir / f"{tkr}_{iv}_{tag}.parquet"

def _read_parquet(p: Path) -> pd.DataFrame:
    return pd.read_parquet(p)

def _write_parquet(df: pd. DataFrame, p: Path):
    df.to_parquet(p, compression="zstd")



# -----------------------------------------------------------------------------------
# Ejecución   ───────────────────────────────────────────────────────────────────────
# -----------------------------------------------------------------------------------

if __name__ == "__main__":
    for tkr in TKRS:
        update_cache(tkr, spot_intervals=["1d"])