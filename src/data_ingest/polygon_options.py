"""
polygon.py -- Funciones de extracción y guardado de datos de Polygon.io
Proyecto Chaos-IV_Signals ·  Python 3.12

Este script hará:
1. Obtener EOD snapshots de cadenas de opciones para cada underlying.
2. Extraer campos importantes (IV, greeks, bid/ask, OI, volumes, underlying price).
3. Mantener la snapshot como un parquet particionado por fecja y ticker.
4. Guardar los OHLCV diarios para cada underlying y los principales índices de volatilidad (VIX, VIX3M, VXMT) en parquet.
"""

from __future__ import annotations

import os
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import List

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from polygon import RESTClient
from dateutil.relativedelta import relativedelta


# ────────────────────────────────────────────────────────────────────────────────
# Configuration
# ────────────────────────────────────────────────────────────────────────────────
DEFAULT_SYMBOLS = ["AAPL", "NVDA", "AMD", "MSFT", "GOOGL", "TSLA", "QQQ", "SPY"] 
API_KEY = os.getenv("POLYGON_API_KEY") or "YOUR_API_KEY_HERE"
STORAGE_PATH = Path("data/raw/polygon").resolve()
FROM_DATE = "2019-01-01"
TO_DATE = "2025-06-01"


# ────────────────────────────────────────────────────────────────────────────────
# Utility helpers
# ────────────────────────────────────────────────────────────────────────────────

def _today_iso() -> str: 
    return date.today().isoformat()

def _ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


# ────────────────────────────────────────────────────────────────────────────────
# Core fetch logic
# ────────────────────────────────────────────────────────────────────────────────

OPTION_FIELDS = {
    # contract identification
    "details":[
        "ticker",
        "expiration_date",
        "strike_price",
        "contract_type",
    ],
    # market data & greeks
    "implied_volatility": [],
    "greeks": ["delta", "gamma", "theta", "vega"],
    "open_interest": [],
    "break_even_price": [],
    "day": ["o", "h", "l", "c", "v"],
    "last_quote": ["bid", "ask", "bid_size", "ask_size", "timestamp"],
    "last_trade": ["price", "size", "timestamp"],
    # underlying data
    "underlying_asset": ["price", "ticker"],
}


class PolygonOptionFetcher:
    """
    Encapsula el snapshot de obtención y persistencia
    """
    def __init__(
            self,
            api_key: str,
            storage_path: Path = STORAGE_PATH, 
            rate_limit_per_sec: float=4.5, # CAMBIAR
    ) -> None:
        self.client = RESTClient(api_key=api_key)
        self.storage_path = storage_path
        self.rate_limit_per_sec = rate_limit_per_sec
        self._next_allowed = time.monotonic()


    # ────────────────────────────────────────────────────────────────────────────────
    # Public
    # ────────────────────────────────────────────────────────────────────────────────

    # def fetch_and_store_snapshot(
    #         self,
    #         symbol: str,
    #         span_pct: float = 0.20,
    #         max_dte: int = 90,
    #         limit: int = 250,
    # ) -> Path:
    #     """
    #     Obtenemos una cadena de opciones entera para *symbol* la guardamos en parquet.
    #     Devuelve la ruta del archivo parquet guardado.
    #     """
    #     today = date.today()
    #     params = {
    #         "expiration_date.lte": (today + timedelta(days=max_dte)).isoformat(),
    #         "contract_type": "all", # calls + puts)
    #         "limit": limit,
    #     }

    #     snapshot_iter = self._rate_limited(
    #         lambda: self.client.list_snapshot_options_chain(symbol, params=params)
    #     )

    #     # DEBUG: print how manu contracts API returned befores filtering
    #     raw = list(snapshot_iter)
    #     print(f"{symbol}: raw contracts returned -> {len(raw)}")
    #     snapshot_iter = raw 

    #     rows: List[dict] = []
    #     for item in snapshot_iter:
    #         # polygon devuelve precio de underlying por item, asi que lo podemos
    #         # usar para filtrar por moneyness
    #         spot = item["underlying_asset"].get("price")
    #         strike = item["details"]["strike_price"]

    #         if spot and spot >0:
    #             if abs(strike-spot) / spot > span_pct:
    #                 continue # fuera de rango de moneyness
    #         flat = self._flatten_option_snapshot(item)
    #         rows.append(flat)

    #     if not rows:
    #         raise RuntimeError(f"No option data gathered for {symbol} on {today}.")
        
    #     df = pd.DataFrame(rows)
    #     out_dir = self.storage_path / "options"
    #     _ensure_dir(out_dir)
    #     out_file = out_dir / f"{symbol}_{today.isoformat()}_snapshot.parquet"
    #     table = pa.Table.from_pandas(df, preserve_index=False)
    #     pq.write_table(table, out_file, compression="zstd")
    #     return out_file
    
    def fetch_and_store_underlying_bars(
            self,
            symbol: str,
            from_: str | None = None,
            to: str | None = None,
    ) -> Path:
        """
        Obtener daily OHLCV bars para *symbol* entre *from_* y *to* (inclusive).
        """
        if from_ is None:
            from_ = _today_iso()
        if to is None:
            to = _today_iso()

        bars_iter = self._rate_limited(
            lambda: self.client.list_aggs(symbol, 1, "day", from_, to)
        )

        df = pd.DataFrame(bars_iter)
        if df.empty:
            raise RuntimeError(f"No bars for {symbol} from {from_} to {to}.")
        
        out_dir = self.storage_path / "stocks" 
        _ensure_dir(out_dir)
        out_file = out_dir / f"{symbol}_{from_}_{to}.parquet"
        pq.write_table(pa.Table.from_pandas(df, preserve_index=False), out_file, compression="zstd")
        return out_file
    

    # ──────────────────────────────────────────────────────────────────────
    # Internal helpers
    # ──────────────────────────────────────────────────────────────────────

    def _rate_limited(self, func):
        """
        Forzamos un simple token-bucket rate limit alrededor de *func()* calls.
        """
        wait = self._next_allowed - time.monotonic()
        if wait > 0:
            time.sleep(wait)
        result = func()
        self._next_allowed = time.monotonic() + 1.0 / self.rate_limit_per_sec
        return result
    
    def _flatten_option_snapshot(self, snap: dict) -> dict:
        flat: dict = {"ts": datetime.utcnow().isoformat(timespec="seconds")}
        for key, subfields in OPTION_FIELDS.items():
            data = snap.get(key, {})
            if not subfields:
                flat[key] = data
            else:
                for sub in subfields:
                    flat[f"{key}.{sub}"] = data.get(sub)
        return flat

    # ------------------------------------------------------------------
    # NEW: get all option symbols that traded between two dates
    # ------------------------------------------------------------------
    def _contract_symbols_between(self, underlying: str, start: str, end: str) -> list[str]:
        """
        Return a sorted list of unique Polygon option symbols for *underlying*
        that existed between *start* and *end* (YYYY-MM-DD).

        Uses `list_options_contracts`, which returns a generator, and queries
        one snapshot per month to stay inside free‑tier rate limits.
        """
        symbols: set[str] = set()
        cursor = datetime.fromisoformat(start).date()
        end_date = datetime.fromisoformat(end).date()

        while cursor <= end_date:
            gen = self.client.list_options_contracts(
                underlying_ticker=underlying,
                as_of=cursor.isoformat(),
                limit=1000
            )
            symbols.update(
                c.ticker for c in gen if getattr(c, "ticker", None)
            )
            cursor += relativedelta(months=1)
            time.sleep(1 / self.rate_limit_per_sec)  # pacing

        return sorted(symbols)

    def fetch_and_store_contract_bars(
            self,
            underlying: str,
            start: str,
            end: str,
            span_pct: float = 0.20,
            max_dte: int = 90,
    ) -> Path:
        """
        Download daily bars for every option contract that existed
        between *start* and *end* (uses aggregates endpoint).
        Stores a single Parquet with all rows.
        """
        symbols = self._contract_symbols_between(underlying, start, end)
        rows: list[dict] = []
        for sym in symbols:
            # quick filter by strike distance & DTE using symbol parts
            try:
                u, yymmdd, cpflag, strike = sym[:sym.find(
                    yymmdd := sym[len(underlying):-8])], sym[len(underlying):len(underlying)+6], sym[len(underlying)+6], sym[-8:]
            except Exception:
                continue  # skip malformed
            expiry = datetime.strptime(yymmdd, "%y%m%d").date()
            dte = (expiry - datetime.fromisoformat(start).date()).days
            if dte < 0 or dte > max_dte:
                continue
            # fetch bars
            bars = self.client.list_aggs(f"O:{sym}", 1, "day", start, end, limit=50000)
            for b in bars:
                b["option_symbol"] = sym
                rows.append(b)
            time.sleep(1 / self.rate_limit_per_sec)
        if not rows:
            raise RuntimeError(f"No option bars for {underlying}")
        df = pd.DataFrame(rows)
        out_dir = self.storage_path / "options"
        _ensure_dir(out_dir)
        out_file = out_dir / f"{underlying}_{start}_{end}_bars.parquet"
        pq.write_table(pa.Table.from_pandas(df, preserve_index=False), out_file, compression="zstd")
        return out_file

# ────────────────────────────────────────────────────────────────────────────────
# CLI entry‑point
# ───────────────────────────────────────────────────────────────────────────────


def main() -> None:
    symbols = DEFAULT_SYMBOLS
    max_dte = 90
    span = 0.20
    rate = 4.5
    api_key = API_KEY

    if api_key == "YOUR_API_KEY_HERE":
        sys.exit("ERROR: Define you Polygon.io key in API_KEY or set POLYGON_API_KEY env var.")
    
    fetcher = PolygonOptionFetcher(api_key=api_key, rate_limit_per_sec=rate)

    for sym in symbols:
        try:
            print(f"→ Fetching {sym} option daily bars …", flush=True)
            opt_path = fetcher.fetch_and_store_contract_bars(
                sym,
                start=FROM_DATE,
                end=TO_DATE,
                span_pct=span,
                max_dte=max_dte
            )
            print(f"   saved option bars → {opt_path}")

            print(f"→ Fetching {sym} underlying bars …", flush=True)
            bars_path = fetcher.fetch_and_store_underlying_bars(sym, from_=FROM_DATE, to=TO_DATE)
            print(f"   saved bars → {bars_path}\n")

        except Exception as e:
            print(f"✖ Error processing {sym}: {e}")


if __name__ == "__main__":
    main()