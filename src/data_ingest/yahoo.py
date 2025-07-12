"""
yahoo.py -- Funciones de extracción y caché de datos spot y opciones de Yahoo Finance.
Proyecto Chaos-IV_Signals ·  Python 3.12
"""

from __future__ import annotations
from pathlib import Path
from typing import Literal, overload
import time
import logging
import pyarrow.feather as feather
import pandas as pd
import yfinance as yf   

# -----------------------------------------------------------------------------------
# Configuración global  ─────────────────────────────────────────────────────────────
# -----------------------------------------------------------------------------------
DATA_DIR = Path("data/raw/yahoo").resolve()
CACHE_SPOT_DIR = DATA_DIR / "spot"
CACHE_OPT_DIR = DATA_DIR / "options"
CACHE_SPOT_DIR.mkdir(parents=True, exist_ok=True)
CACHE_OPT_DIR.mkdir(parents=True, exist_ok=True)

RATE_LIMIT_DELAY = 0.6  # yahoo solo deja 2 peticiones/s 
DEFAULT_TZ = "Europe/Madrid"
logger = logging.getLogger(__name__)



# -----------------------------------------------------------------------------------
# Funciones públicas    ─────────────────────────────────────────────────────────────
# -----------------------------------------------------------------------------------
def get_spot(
        ticker: str,
        *, 
        start: str | None = None,
        end: str | None = None, 
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
    path = _spot_cache_path(ticker, interval, start, end)
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
        
    time.sleep(RATE_LIMIT_DELAY)
    return df
    

@overload
def get_options(
    ticker: str,
    *,
    expiration: str | None = None,
    side: Literal["calls"],
    cache: bool = True,
) -> pd.DataFrame: ...

@overload
def get_options(
    ticker: str,
    *,
    expiration: str | None = None,
    side: Literal["puts"],
    cache: bool = True,
) -> pd.DataFrame: ...

@overload
def get_options(
    ticker: str,
    *,
    expiration: str | None = None,
    side: Literal["both"] = "both",
    cache: bool = True,
) -> tuple[pd.DataFrame, pd.DataFrame]: ...

def get_options(
        ticker: str, 
        *,
        expiration: str | None = None, 
        side: Literal["calls", "puts", "both"] = "both",
        cache: bool = True,
):
    """
    Devuelve la(s) cadena(s) de opciones EOD.
    
    - `expiration`: YYYY-MM-DD o None (<<próximo vencimiento>>)
    -`side`: calls, puts, o both -> Devuelve DataFrame(s)
    """
    path = _opt_cache_path(ticker, expiration or "nearest")
    if cache and path.exists():
        logger.debug("Cargando cadena de opciones de caché: %s", path)
        return _read_opt_cache(path, side)
    
    tk = yf.Ticker(ticker)
    if expiration is None: 
        expiration = tk.options[0]  # próximo vencimiento

    chain = tk.option_chain(expiration)
    calls, puts = _clean_chain(chain.calls), _clean_chain(chain.puts)

    if cache:
        _write_opt_cache(path, calls, puts)

    time.sleep(RATE_LIMIT_DELAY)
    if side == "calls":
        return calls
    if side == "puts":
        return puts
    return calls, puts


def update_cache(
        ticker: str,
        *,
        spot_intervals: list[str] = ["1d", "60m"],
        options_latest: bool = True
) -> None:
    """
    Actualiza en bloque la cache local:
    - recorre `spot_intervals``
    - descarga la última cadena de opciones si `options_latest`
    """
    for iv in spot_intervals:
        get_spot(ticker, interval=iv, cache=True)

    if options_latest:
        get_options(ticker, side="both", cache = True)



# -----------------------------------------------------------------------------------
# Helpers privados      ─────────────────────────────────────────────────────────────
# -----------------------------------------------------------------------------------
def _spot_cache_path(tkr: str, iv: str, s:str | None, e:str | None)  -> Path:
    tag = f"{s}_{e}" if s or e else "full"
    return CACHE_SPOT_DIR / f"{tkr}_{iv}_{tag}.parquet"

def _opt_cache_path(tkr: str, exp: str) -> Path: 
    return CACHE_OPT_DIR / f"{tkr}_{exp}.feather"

def _clean_chain(df: pd.DataFrame) -> pd.DataFrame:
    wanted = [
        "contractSymbol", "strike", "lastPrice", 
        "bid", "ask", "volume", "openInterest"
    ]
    return (
        df[wanted]
        .rename(columns=str.lower)
        .assign(expiration=lambda d: d.contractsymbol.str[-8:]) # CAMBIARLO (ejemplo)
        .set_index("contractsymbol")
    )

def _read_parquet(p: Path) -> pd.DataFrame:
    return pd.read_parquet(p)

def _write_parquet(df: pd. DataFrame, p: Path):
    df.to_parquet(p, compression="zstd")

def _write_opt_cache(p: Path, calls: pd.DataFrame, puts: pd.DataFrame):
#    import feather
    feather.write_feather(pd.concat({"calls": calls, "puts": puts}), p)

def _read_opt_cache(p: Path, side: str):
#    import feather, pandas as pd
    data = feather.read_feather(p)
    if side == "both":
        return data.loc["calls"], data.loc["puts"]
    return data.loc[side]


# -----------------------------------------------------------------------------------
# CLI para uso rápido   ─────────────────────────────────────────────────────────────
# -----------------------------------------------------------------------------------
if __name__ == "__main__":
    import argparse, sys

    parser = argparse.ArgumentParser(description="Descarga datos Yahoo Finance")
    parser.add_argument("--ticker", required=True)
    parser.add_argument("--interval", default="1d")
    parser.add_argument("--spot", action="store_true")
    parser.add_argument("--options", action="store_true")
    args = parser.parse_args()

    if args.spot:
        df = get_spot(args.ticker, interval=args.interval, cache=True)
        print(df.tail())
    if args.options:
        calls, puts = get_options(args.ticker, side="both", cache=True)
        print(calls.head()); print(puts.head())
    if not (args.spot or args.options):
        parser.print_help(sys.stderr)