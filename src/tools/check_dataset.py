# ==========================================
# ========== FILE: src/tools/check_dataset.py
# ==========================================
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import List, Tuple

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


def _load_df(path: Path) -> pd.DataFrame:
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
    lows_ok = df["low"] <= df[["open", "close", "high"]].min(axis=1)
    highs_ok = df["high"] >= df[["open", "close", "low"]].max(axis=1)
    vols_ok = df["volume"] >= 0
    if not (lows_ok.all() and highs_ok.all() and vols_ok.all()):
        raise ValueError("Sanidad OHLC/volumen fallida (low/high/volume incoherentes).")


def _suggest_ohlcv_candidates(base_dir: Path, limit: int = 8) -> Tuple[List[Path], List[Path]]:
    """
    Devuelve 2 listas (RAW, FILLED) con hasta `limit` candidatos cada una
    dentro de `base_dir`, ordenados por fecha de modificación descendente.
    """
    if not base_dir.exists():
        return ([], [])

    all_csv = sorted(base_dir.glob("*.csv"), key=lambda p: p.stat().st_mtime, reverse=True)
    raw = [p for p in all_csv if not p.stem.endswith("_filled")][:limit]
    filled = [p for p in all_csv if p.stem.endswith("_filled")][:limit]
    return (raw, filled)


def _print_missing_with_suggestions(requested: Path, base_dir: Path) -> None:
    print(f"ERROR: no existe el archivo: {requested}")
    # Sugerencias útiles en data/ohlcv
    raw, filled = _suggest_ohlcv_candidates(base_dir)
    if raw or filled:
        print("\nSugerencias en data/ohlcv (más recientes primero):")
        if raw:
            print("  RAW (sin _filled):")
            for p in raw:
                ts = pd.to_datetime(p.stat().st_mtime, unit="s", utc=True)
                print(f"   - {p}    (mod: {ts.isoformat()})")
        if filled:
            print("  FILLED (_filled):")
            for p in filled:
                ts = pd.to_datetime(p.stat().st_mtime, unit="s", utc=True)
                print(f"   - {p}    (mod: {ts.isoformat()})")
    else:
        print("No se encontraron CSV en data/ohlcv/.")
    print("\nPista: Si indicabas sólo el stem, prueba añadiendo la extensión .csv")
    sys.exit(2)


def main() -> None:
    ap = argparse.ArgumentParser(
        description=(
            "Valida un dataset OHLCV a una frecuencia fija (duplicados/huecos/completitud) y, "
            "opcionalmente, sanidad OHLC/volumen."
        )
    )
    ap.add_argument("csv_path", help="Ruta al CSV con columna 'datetime' (índice o columna).")
    ap.add_argument("--freq", default="15min", help="Frecuencia esperada (p. ej. '15min', '1H').")
    ap.add_argument(
        "--sanity-ohlc",
        action="store_true",
        help="Valida coherencia OHLC/volumen (low/high/volume).",
    )
    args = ap.parse_args()

    # --- Normalización de freq: admite '15T' -> '15min' (evita FutureWarning)
    import re

    freq = args.freq
    m = re.fullmatch(r"(\d+)T", str(freq).strip(), flags=re.IGNORECASE)
    if m:
        freq = f"{m.group(1)}min"

    # --- Resolución de ruta y guardarraíles de UX (sugerencias si no existe)
    in_path = Path(args.csv_path)
    if not in_path.suffix:
        # Si vino sin extensión, prueba con .csv
        candidate = in_path.with_suffix(".csv")
        if candidate.exists():
            in_path = candidate
    if not in_path.exists():
        _print_missing_with_suggestions(in_path, base_dir=Path("data/ohlcv"))

    # Carga y normalización
    df = _load_df(in_path)

    # --- Validación de columnas esperadas (alineado con fill_gaps)
    missing = EXPECTED_COLS.difference(set(df.columns))
    if missing:
        print(f"ERROR: columnas faltantes en origen: {sorted(missing)}")
        sys.exit(1)

    # Construye el grid completo esperado y calcula huecos/duplicados
    full = pd.date_range(df.index.min(), df.index.max(), freq=freq, tz="UTC")
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
