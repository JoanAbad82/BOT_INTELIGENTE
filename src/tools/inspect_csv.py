# ==========================================
# ========== FILE: src/tools/inspect_csv.py
# ==========================================
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd


def _ensure_utc_index(df: pd.DataFrame) -> pd.DataFrame:
    """Garantiza índice datetime en UTC, ordenado y sin duplicados."""
    if not isinstance(df.index, pd.DatetimeIndex):
        df.index = pd.to_datetime(df.index, utc=True, errors="coerce")

    if df.index.tz is None:
        df.index = df.index.tz_localize("UTC")
    else:
        df.index = df.index.tz_convert("UTC")

    # Orden y de-duplicado conservando la última
    df = df[~df.index.duplicated(keep="last")].sort_index()
    return df


def main() -> None:
    if len(sys.argv) < 2:
        print("Uso: python -m src.tools.inspect_csv <ruta_csv>")
        sys.exit(1)

    path = Path(sys.argv[1])
    if not path.exists():
        print(f"Error: no existe el archivo: {path}")
        sys.exit(1)

    # Carga
    df = pd.read_csv(path, parse_dates=["datetime"], index_col="datetime")
    df = _ensure_utc_index(df)

    # Métricas básicas
    print(f"Ruta: {path}")
    print(f"Filas: {len(df):,}")
    print(f"Rango: {df.index.min()} → {df.index.max()}  (freq esperada: 15min)")

    # Duplicados y huecos
    dup = int(df.index.duplicated().sum())
    print(f"Índices duplicados (post-normalización): {dup}")

    full_range = pd.date_range(df.index.min(), df.index.max(), freq="15min", tz="UTC")
    gaps = full_range.difference(df.index)
    print(f"Huecos en la serie (n): {len(gaps)}")
    if len(gaps) > 0:
        print(f"Primeros 10 huecos: {list(gaps[:10])}")

    # Alineación exacta al grid de 15min (desde epoch)
    epoch = pd.Timestamp(0, tz="UTC")
    tf = pd.Timedelta("15min")
    tf_ms = int(tf.total_seconds() * 1000)
    epoch_ms = int(epoch.timestamp() * 1000)
    aligned = all(
        ((int(ts.timestamp() * 1000) - epoch_ms) % tf_ms) == 0 for ts in df.index
    )
    print(f"Alineación exacta al grid 15min: {'OK' if aligned else 'NO'}")

    # Paso constante (cuando no hay gaps)
    if len(gaps) == 0 and len(df) > 1:
        deltas = (df.index[1:] - df.index[:-1]).unique()
        print(f"Paso temporal único: {list(deltas)}")

    # Métrica de completitud
    expected = len(full_range)
    present = len(df)
    pct = 100.0 * present / expected if expected else 0.0
    print(f"Completitud: {present}/{expected} = {pct:.2f}%")


if __name__ == "__main__":
    main()
