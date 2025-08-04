"""
sync_data.py -- Script para obtener todos los datos del mercado a la vez.
Proyecto Chaos-IV_Signals ·  Python 3.12
"""

from __future__ import annotations
from pathlib import Path

from yahoo import update_cache
# ir poniendo el resto de funciones aquí


TICKERS = ['SPY', 'QQQ', 'AAPL', 'NVDA', 'TSLA']

def _refresh_yahoo():
    for tkr in TICKERS:
        update_cache(tkr, spot_intervals=["1d"])        
    print("Datos de Yahoo Finance actualizados.")



def main():
    _refresh_yahoo()
    # poner el resto
    print("Todos los datos actualizados.")


if __name__ == "__main__":
    main()

    