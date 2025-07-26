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

import argparse
import json
import os
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Iterable, List, Sequence

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from polygon import RESTClient, exceptions



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

OPTION_FILDS = {
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
            storage_path: Path = Path("data"), # CAMBIAR
            rate_limit_per_sec: float=4.5, # CAMBIAR
    ) -> None:
        self.client = RESTClient(api_key=api_key)
        self.storage_path = storage_path
        self.rate_limit_per_sec = rate_limit_per_sec
        self._next_allowed = time.monotonic()


    # ────────────────────────────────────────────────────────────────────────────────
    # Public
    # ────────────────────────────────────────────────────────────────────────────────

    def fetch_and_store_snapshot(
            self,
            symbol: str,
            span_pct: float = 0.20,
            max_dte: int = 90,
            limit: int = 250,
    ) -> Path:
        """
        Obtenemos una cadena de opciones entera para *symbol* la guardamos en parquet.
        Devuelve la ruta del archivo parquet guardado.
        """
        today = date.today()
        params = {
        "expiration_date.lte": (today + timedelta(days=max_dte)).isoformat(),
        "contract_type": "all", # calls + puts)
        "limit": limit,
        }

        snapshot_iter = self._rate_limited(
            lambda: self.client.list_snapshot_options_chain(symbol, params=params)
        )

        rows: List[dict] = []
        for item in snapshot_iter:
            # polygon devuelve precio de underlying por item, asi que lo podemos
            # usar para filtrar por moneyness
            spot = item["underlying_asset"]["price"]
            strike = item["details"]["strike_price"]
            if abs(strike-spot) / spot > span_pct:
                continue # skip out-of-band options
            flat = self._flatten_option_snapshot(item)
            rows.append(flat)

        if not rows:
            raise RuntimeError(f"No option data gathered for {symbol} on {today}.")
        
        df = pd.DataFrame(rows)
        out_dir = (
            self.storage_path / "options" / symbols / f"date={today.isoformat()}"
        )
        _ensure_dir(out_dir)
        out_file = out_dir / "snapshot.parquet"
        table = pa.Table.from_pandas(df, preserve_index=False)
        pq.write_table(table, out_file, compression="zstd")
        return out_file
    
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
            lambda: self.client.list_agss(symbol, 1, "day", from_, to)
        )

        df = pd.DataFrame(bars_iter)
        if df.empty:
            raise RuntimeError(f"No bars for {symbol} from {from_} to {to}.")
        
        out_dir = self.storage_path / "stocks" / symbol 
        _ensure_dir(out_dir)
        out_file = out_dir / f"{from_}_{to}.parquet"
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
        self._next_alllowed = time.monotonic() + 1.0 / self.rate_limit_per_sec
        return result
    
    def _flatter_option_snapshot(self, snap: dict) -> dict:
        flat: dict = {"ts": datetime.utcnow().isoformat(timespec="secods")}
        for key, subfields in OPTION_FIELDS.items():
            data = snap.get(key, {})
            if not subfields:
                flat[key] = data
            else:
                for sub in subfields:
                    flat[f"{key}.{sub}"] = data.get(sub)
        return flat

# ────────────────────────────────────────────────────────────────────────────────
# CLI entry‑point
# ───────────────────────────────────────────────────────────────────────────────
 
def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Nightly Polygon options snapshot fetcher")
    p.add_argument("--symbols", nargs="+", required=True, help="Underlying tickers e.g. AAPL MSFT SPY")
    p.add_argument("--max-dte", type=int, default=90, help="Max days‑to‑expiry to keep")
    p.add_argument("--span", type=float, default=0.20, help="Moneyness span (+/‑pct from ATM)")
    p.add_argument("--storage-path", type=Path, default=Path("data"))
    p.add_argument("--rate", type=float, default=4.5, help="Max requests per second")
    p.add_argument("--api-key", default=os.getenv("POLYGON_API_KEY"), help="Polygon.io API key (env POLYGON_API_KEY)")
    return p.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    args = _parse_args(argv)
    if not args.api_key:
        sys.exit("ERROR: provide --api-key or set POLYGON_API_KEY env var")

    fetcher = PolygonOptionFetcher(api_key=args.api_key, storage_path=args.storage_path, rate_limit_per_sec=args.rate)

    for sym in args.symbols:
        try:
            print(f"→ Fetching {sym} options snapshot …", flush=True)
            snap_path = fetcher.fetch_and_store_snapshot(sym, span_pct=args.span, max_dte=args.max_dte)
            print(f"   saved snapshot → {snap_path}")

            print(f"→ Fetching {sym} underlying bars …", flush=True)
            bars_path = fetcher.fetch_and_store_underlying_bars(sym)
            print(f"   saved bars → {bars_path}\n")
        except exceptions.NoResultsError:
            print(f"✖ No data for {sym} (skipped)")
        except Exception as e:
            print(f"✖ Error processing {sym}: {e}")


if __name__ == "__main__":
    main()



