# ==========================================
# ========== FILE: src/tools/fill_gaps.py
# ==========================================
from __future__ import annotations

import argparse
from pathlib import Path
from datetime import timezone
from typing import List, Tuple

import pandas as pd
from loguru import logger

from src.data.ohlcv_downloader import FetchConfig, download_ohlcv
from src.utils.logging import setup_logging
from src.config.settings import settings

# ⚠️ OJO: Pandas != CCXT
DEFAULT_FREQ_PANDAS = "15min"  # ✅ sin FutureWarnings (antes "15T")
DEFAULT_TF_CCXT = "15m"  # CCXT usa sufijo 'm'


# ---------------------------
# Utilidades de índice/fechas
# ---------------------------
def _ensure_utc_index(df: pd.DataFrame) -> pd.DataFrame:
    """
    Asegura que el índice datetime:
      - es DatetimeIndex
      - está en UTC (tz-aware)
      - está ordenado ascendente y sin duplicados (manteniendo la última ocurrencia)
    """
    if not isinstance(df.index, pd.DatetimeIndex):
        df.index = pd.to_datetime(df.index, utc=True, errors="coerce")

    # Si viene naive, localizamos a UTC; si viene con tz, convertimos a UTC.
    if df.index.tz is None:
        df.index = df.index.tz_localize("UTC")
    else:
        df.index = df.index.tz_convert("UTC")

    # Orden + deduplicado conservando la última ocurrencia
    df = df[~df.index.duplicated(keep="last")].sort_index()
    return df


def group_gaps(
    gaps: pd.DatetimeIndex, freq: str
) -> List[Tuple[pd.Timestamp, pd.Timestamp]]:
    """Agrupa huecos contiguos en rangos [start, end] según el paso `freq`."""
    if len(gaps) == 0:
        return []

    ranges: List[Tuple[pd.Timestamp, pd.Timestamp]] = []
    start = gaps[0]
    prev = gaps[0]
    step = pd.Timedelta(freq)  # p. ej. '15min'

    for ts in gaps[1:]:
        if ts - prev == step:
            prev = ts
            continue
        ranges.append((start, prev))
        start = ts
        prev = ts

    ranges.append((start, prev))
    return ranges


# ---------------------------
# CLI
# ---------------------------
def _parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="Rellena huecos en un CSV OHLCV alineado al grid, descargando parches desde Binance (LIVE)."
    )
    ap.add_argument(
        "--csv",
        required=True,
        help="Ruta al CSV fuente (con columna 'datetime' e índice/columna parseable a fecha).",
    )
    ap.add_argument(
        "--freq",
        default=DEFAULT_FREQ_PANDAS,
        help=f"Frecuencia esperada del dataset (Pandas offset). Por defecto: {DEFAULT_FREQ_PANDAS}.",
    )
    ap.add_argument(
        "--symbol",
        default=settings.default_symbol,
        help=f"Símbolo BASE/USDC para descargar parches. Por defecto: {settings.default_symbol}.",
    )
    ap.add_argument(
        "--timeframe",
        default=DEFAULT_TF_CCXT,
        help=f"Timeframe CCXT para los parches (1m,5m,15m,1h,...). Por defecto: {DEFAULT_TF_CCXT}.",
    )
    ap.add_argument(
        "--margin",
        default="75min",
        help="Margen alrededor de cada hueco para descargar contexto (Pandas offset). Por defecto: 75min.",
    )
    ap.add_argument(
        "--outdir",
        default=None,
        help="Directorio de salida. Si no se indica, se usa el del CSV fuente.",
    )
    ap.add_argument(
        "--suffix",
        default="_filled",
        help="Sufijo del archivo de salida. Por defecto: _filled.",
    )
    ap.add_argument(
        "--no-reload-markets",
        action="store_true",
        help="No forzar reload de mercados al descargar parches.",
    )
    return ap.parse_args()


# ---------------------------
# Lógica principal
# ---------------------------
def main() -> None:
    setup_logging("BOT_INTELIGENTE")
    args = _parse_args()

    src_csv = Path(args.csv)
    if not src_csv.exists():
        logger.error(f"No existe el archivo de origen: {src_csv}")
        raise SystemExit(2)

    # Carga robusta y normalización del índice a UTC
    df = pd.read_csv(src_csv, parse_dates=["datetime"], index_col="datetime")
    df = _ensure_utc_index(df)

    # === Validaciones de entrada (contrato de columnas y tz) ===
    expected_cols = {"open", "high", "low", "close", "volume"}
    missing = expected_cols.difference(set(df.columns))
    if missing:
        logger.error(f"Columnas faltantes en origen: {sorted(missing)}")
        raise SystemExit(2)
    if df.index.tz is None or str(df.index.tz) != "UTC":
        logger.error("El índice no está en UTC tras la normalización.")
        raise SystemExit(2)

    # Construye el rango completo esperado a la frecuencia indicada (Pandas)
    freq = args.freq
    full = pd.date_range(df.index.min(), df.index.max(), freq=freq, tz="UTC")
    gaps = full.difference(df.index)

    logger.info(f"Huecos detectados: {len(gaps)}")
    # Determina ruta de salida
    outdir = Path(args.outdir) if args.outdir else src_csv.parent
    outdir.mkdir(parents=True, exist_ok=True)
    out_path = outdir / (src_csv.stem + args.suffix + src_csv.suffix)

    if len(gaps) == 0:
        logger.info("Nada que rellenar.")
        # Exportamos una versión canónica/ordenada por consistencia
        numeric_cols = ["open", "high", "low", "close", "volume"]
        df[numeric_cols] = (
            df[numeric_cols].apply(pd.to_numeric, errors="coerce").astype("float64")
        )
        df.to_csv(out_path, index=True)
        logger.info(f"Guardado dataset (sin cambios) en: {out_path}")
        print(out_path)
        return

    ranges = group_gaps(gaps, freq)
    logger.info(f"Tramos contiguos: {len(ranges)}")

    filled_parts = []
    margin = pd.Timedelta(args.margin)  # p. ej. '75min'

    for i, (g_start, g_end) in enumerate(ranges, 1):
        # Añadimos margen para evitar bordes incompletos en fetch
        since = (g_start - margin).to_pydatetime().replace(tzinfo=timezone.utc)
        until = (g_end + margin).to_pydatetime().replace(tzinfo=timezone.utc)
        logger.info(
            f"[{i}/{len(ranges)}] Rellenando {g_start} → {g_end} (con margen {args.margin})"
        )

        cfg = FetchConfig(
            symbol=args.symbol,
            timeframe=args.timeframe,
            since=since,
            until=until,
            # El downloader gestionará su propia carpeta RAW/FINAL; no imponemos outdir aquí
            # para no mezclar parches con el dataset principal salvo necesidad.
        )
        part_path = download_ohlcv(cfg)
        part = pd.read_csv(part_path, parse_dates=["datetime"], index_col="datetime")
        part = _ensure_utc_index(part)

        # Validación rápida de columnas en cada parche
        miss_part = expected_cols.difference(set(part.columns))
        if miss_part:
            logger.warning(
                f"Parche con columnas faltantes {sorted(miss_part)} en {part_path}; se omite."
            )
            continue

        filled_parts.append(part)

    if not filled_parts:
        logger.warning("No se generaron parches válidos. No hay cambios que aplicar.")
        df.to_csv(out_path, index=True)
        logger.info(f"Guardado dataset (original) en: {out_path}")
        print(out_path)
        return

    # Fusiona, desduplica y ordena
    patch = pd.concat([df] + filled_parts, axis=0)
    patch = _ensure_utc_index(patch)

    # Fuerza tipado float64 en columnas numéricas
    numeric_cols = ["open", "high", "low", "close", "volume"]
    patch[numeric_cols] = (
        patch[numeric_cols].apply(pd.to_numeric, errors="coerce").astype("float64")
    )

    # Re-chequeo de huecos sobre el nuevo rango completo
    new_full = pd.date_range(patch.index.min(), patch.index.max(), freq=freq, tz="UTC")
    remaining = new_full.difference(patch.index)
    logger.info(f"Huecos restantes tras parcheo: {len(remaining)}")
    if len(remaining) > 0:
        logger.warning(
            f"Persisten {len(remaining)} huecos tras el parcheo. Considera reintentar o ampliar margen."
        )

    patch.to_csv(out_path, index=True)
    logger.info(f"Guardado dataset rellenado: {out_path}")
    print(out_path)


if __name__ == "__main__":
    main()
