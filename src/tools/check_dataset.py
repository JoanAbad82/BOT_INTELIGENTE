# ==========================================
# ========== FILE: src/tools/check_dataset.py
# ==========================================
from __future__ import annotations

import sys
import argparse
import pandas as pd

EXPECTED_COLS = {"open", "high", "low", "close", "volume"}


def _ensure_utc_index(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliza el índice datetime a UTC (tz-aware) y lo ordena ASC,
    sin eliminar duplicados (para poder reportarlos correctamente).
    - Si el índice no es DatetimeIndex, intenta parsearlo.
    - Si viene naive, se localiza a UTC; si trae tz, se convierte a UTC.
    """
    if not isinstance(df.index, pd.DatetimeIndex):
        df.index = pd.to_datetime(df.index, utc=True, errors="coerce")

    if df.index.tz is None:
        df.index = df.index.tz_localize("UTC")
    else:
        df.index = df.index.tz_convert("UTC")

    return df.sort_index()  # 👈 no deduplicamos aquí


def _load_df(path: str) -> pd.DataFrame:
    """
    Carga el CSV con 'datetime' como índice y devuelve el DataFrame
    con índice normalizado a UTC. La validación de columnas se hace en main().
    """
    df = pd.read_csv(path, parse_dates=["datetime"], index_col="datetime")
    return _ensure_utc_index(df)


def _validate_ohlc_sanity(df: pd.DataFrame) -> None:
    """
    Coherencia mínima:
      - low <= min(open, close, high)
      - high >= max(open, close, low)
      - volume >= 0
    """
    lows_ok = (df["low"] <= df[["open", "close", "high"]].min(axis=1))
    highs_ok = (df["high"] >= df[["open", "close", "low"]].max(axis=1))
    vols_ok = (df["volume"] >= 0)
    if not (lows_ok.all() and highs_ok.all() and vols_ok.all()):
        raise ValueError("Sanidad OHLC/volumen fallida (low/high/volume incoherentes).")


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Valida un dataset OHLCV a una frecuencia fija (duplicados/huecos/completitud) y, opcionalmente, sanidad OHLC/volumen."
    )
    ap.add_argument("csv_path", help="Ruta al CSV con columna 'datetime' (índice o columna).")
    ap.add_argument("--freq", default="15min", help="Frecuencia esperada (p. ej. '15min', '1H').")
    ap.add_argument(
        "--sanity-ohlc",
        action="store_true",
        help="Valida coherencia OHLC/volumen (low/high/volume).",
    )
    args = ap.parse_args()

    # Carga y normalización
    df = _load_df(args.csv_path)

    # --- Validación de columnas esperadas (alineado con fill_gaps)
    missing = EXPECTED_COLS.difference(set(df.columns))
    if missing:
        print(f"ERROR: columnas faltantes en origen: {sorted(missing)}")
        sys.exit(1)

    # Construye el grid completo esperado y calcula huecos/duplicados
    full = pd.date_range(df.index.min(), df.index.max(), freq=args.freq, tz="UTC")
    expected, present = len(full), len(df)
    dups = int(df.index.duplicated().sum())  # se mide tras ordenar, sin deduplicar
    gaps = full.difference(df.index)
    pct = 100.0 * present / expected if expected else 0.0

    # Sanidad OHLC/volumen (opcional)
    if args.sanity_ohlc:
        try:
            _validate_ohlc_sanity(df)
        except ValueError as e:
            print(f"ERROR: {e}")
            sys.exit(1)

    # Reporte y exit code
    if dups > 0 or len(gaps) > 0:
        first = gaps[0] if len(gaps) else None
        last = gaps[-1] if len(gaps) else None
        print(
            f"ERROR: duplicados={dups}, huecos={len(gaps)}, "
            f"completitud={present}/{expected}={pct:.2f}%"
        )
        if first is not None:
            print(f"Rango huecos aprox: {first} → {last}")
        sys.exit(1)

    print(f"OK: dataset limpio. Completitud={present}/{expected}={pct:.2f}%")
    sys.exit(0)


if __name__ == "__main__":
    main()
