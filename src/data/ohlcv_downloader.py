# SPDX-License-Identifier: MIT
# Copyright (c) 2025 Joan Abad and contributors
# ============================================
# ========== FILE: src/data/ohlcv_downloader.py
# ============================================

from __future__ import annotations

import math
import os
import re
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import ccxt
import pandas as pd
from ccxt.base.errors import ExchangeError, NetworkError, RequestTimeout
from loguru import logger

from src.config.settings import settings

__all__ = [
    "FetchConfig",
    "download_ohlcv",  # -> Path
    "read_ohlcv_csv",  # -> pd.DataFrame
    "download_ohlcv_and_read",  # -> tuple[Path, pd.DataFrame]
]


# =======================
# ==== Utilidades
# =======================
def _to_millis(dt: datetime) -> int:
    """Convierte un datetime (naive → asume UTC) a milisegundos UNIX."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return int(dt.timestamp() * 1000)


def _iso_utc(ms: int) -> str:
    """Devuelve una representación ISO-8601 UTC para ms UNIX."""
    return datetime.utcfromtimestamp(ms / 1000).replace(tzinfo=UTC).isoformat()


def _ensure_utc_index(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliza DataFrame final: índice datetime UTC, orden ascendente y tipos float64.
    Espera columnas: ['open','high','low','close','volume'] + 'datetime' (o 'timestamp').
    """
    if "timestamp" in df.columns and "datetime" not in df.columns:
        df["datetime"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    if "datetime" not in df.columns:
        raise ValueError("Falta columna 'datetime' para indexar el DataFrame final.")

    df = (
        df.drop_duplicates(subset=["datetime"])
        .sort_values("datetime")
        .reset_index(drop=True)
        .set_index("datetime")
    )
    cols = ["open", "high", "low", "close", "volume"]
    return df[cols].astype("float64")


def _validate_ohlc_sanity(df: pd.DataFrame) -> None:
    """
    Reglas mínimas de sanidad por vela:
      - low <= min(open, close, high)
      - high >= max(open, close, low)
      - volume >= 0
    """
    lows_ok = df["low"] <= df[["open", "close", "high"]].min(axis=1)
    highs_ok = df["high"] >= df[["open", "close", "low"]].max(axis=1)
    vols_ok = df["volume"] >= 0
    if not (lows_ok.all() and highs_ok.all() and vols_ok.all()):
        raise RuntimeError("Sanidad OHLC/volumen fallida (low/high/volume incoherentes).")


# =======================
# ==== Config y Exchange
# =======================
@dataclass
class FetchConfig:
    symbol: str = settings.default_symbol
    timeframe: str = "15m"
    since: datetime | None = None
    until: datetime | None = None
    outdir: Path = Path(__file__).resolve().parents[2] / "data" / "ohlcv"
    limit_per_call: int = 1000
    reload_markets: bool = True


class MarketExchange:
    """Exchange SOLO para DATOS (LIVE). Forzamos spot y rate limit."""

    def __init__(self) -> None:
        self.ex = ccxt.binance(
            {
                "enableRateLimit": True,
                "timeout": int(settings.timeout_ms),
                "options": {"defaultType": "spot"},
            }
        )
        try:
            self.ex.set_sandbox_mode(False)
        except Exception:
            # Algunos conectores no exponen sandbox o pueden fallar silenciosamente
            pass

    def load(self, reload: bool = True) -> None:
        self.ex.load_markets(reload=reload)

    @property
    def symbols(self) -> list[str]:
        return list(self.ex.symbols)

    def has_symbol(self, s: str) -> bool:
        return s in self.ex.markets

    def fetch_ohlcv(self, symbol: str, timeframe: str, since_ms: int | None, limit: int):
        return self.ex.fetch_ohlcv(symbol, timeframe=timeframe, since=since_ms, limit=limit)


# =======================
# ==== Robustez red
# =======================
MAX_RETRIES = 5
CHUNK_SIZE = 10_000  # volcado incremental


def _with_retries(fn: Callable[..., Any], *args, **kwargs):
    delay = 1.0
    for i in range(MAX_RETRIES):
        try:
            return fn(*args, **kwargs)
        except (NetworkError, RequestTimeout, ExchangeError) as e:
            if i == MAX_RETRIES - 1:
                raise
            logger.warning(
                f"{type(e).__name__} en fetch; reintento {i+1}/{MAX_RETRIES-1} en {delay:.1f}s…"
            )
            time.sleep(delay)
            delay *= 2.0  # backoff exponencial


# =======================
# ==== Descarga OHLCV
# =======================
def download_ohlcv(cfg: FetchConfig) -> Path:
    """
    Descarga OHLCV en formato RAW (timestamp, open, high, low, close, volume) y crea un
    CSV FINAL normalizado (índice datetime UTC), con:
      - Validación de símbolo BASE/USDC y existencia (nunca USDT).
      - Timestamps monótonos y alineados al grid del timeframe.
      - Volcado RAW incremental a *.raw.csv (resistente a Ctrl+C).
      - Escritura final ATÓMICA a *.csv (y limpieza del RAW).

    Returns
    -------
    Path
        Ruta del CSV **final** normalizado.

    Nota importante: la librería **no** configura logging. El *entrypoint* (CLI) debe hacerlo.
    """
    # No llamar a setup_logging(...) aquí: la librería solo usa logging.
    logger.info(f"Descargando OHLCV | symbol={cfg.symbol} tf={cfg.timeframe}")

    mkt = MarketExchange()
    mkt.load(reload=cfg.reload_markets)

    # Validación símbolo USDC + formato BASE/USDC (sin USDT)
    if not re.fullmatch(r"[A-Z0-9\-]+/USDC", cfg.symbol):
        raise ValueError(f"Formato de símbolo inválido o no USDC: {cfg.symbol!r}")
    if not mkt.has_symbol(cfg.symbol):
        base = cfg.symbol.split("/")[0]
        candidates = [s for s in mkt.symbols if s.startswith(base + "/") and s.endswith("/USDC")]
        raise ValueError(
            f"El símbolo {cfg.symbol} no existe en Binance LIVE. "
            f"Candidatos USDC: {candidates[:15]}"
        )

    # Tamaño de vela en ms
    try:
        tf_ms = int(pd.Timedelta(cfg.timeframe).total_seconds() * 1000)
    except Exception as e:  # noqa: BLE001
        raise ValueError(
            f"Timeframe inválido: {cfg.timeframe!r}. Ejemplos: '1m','15m','1h','1d'."
        ) from e
    if tf_ms <= 0:
        raise ValueError(f"Timeframe con duración no positiva: {cfg.timeframe!r}")

    # Rango temporal
    now = datetime.now(UTC).replace(microsecond=0)
    start_dt = cfg.since or (now - pd.Timedelta(days=30))
    end_dt = cfg.until or now
    if start_dt >= end_dt:
        raise ValueError(
            f"Rango inválido: since={start_dt.isoformat()} >= until={end_dt.isoformat()}"
        )

    start_ms = _to_millis(start_dt)
    end_ms = _to_millis(end_dt)

    # Alineación suave al grid
    if start_ms % tf_ms != 0:
        aligned = start_ms + (tf_ms - (start_ms % tf_ms))
        logger.warning(
            f"since no alineado a {cfg.timeframe} (tf_ms={tf_ms}); "
            f"{_iso_utc(start_ms)} → {_iso_utc(aligned)}."
        )
        start_ms = aligned
    if end_ms % tf_ms != 0:
        aligned = end_ms - (end_ms % tf_ms)
        logger.warning(
            f"until no alineado a {cfg.timeframe} (tf_ms={tf_ms}); "
            f"{_iso_utc(end_ms)} → {_iso_utc(aligned)}."
        )
        end_ms = aligned

    # Rutas de salida
    outdir = Path(cfg.outdir)  # robusto si llega str
    outdir.mkdir(parents=True, exist_ok=True)
    fname = (
        f"{cfg.symbol.replace('/','')}_{cfg.timeframe}_"
        f"{datetime.fromtimestamp(start_ms/1000, tz=UTC).date()}_"
        f"{datetime.fromtimestamp(end_ms/1000, tz=UTC).date()}.csv"
    )
    fpath = outdir / fname
    raw_path = fpath.with_suffix(".raw.csv")

    # Si el final existe, recreamos RAW desde cero (evita mezclar esquemas)
    if fpath.exists():
        logger.info(f"Final existente; se recreará RAW y se reemplazará atómicamente: {fpath}")
        try:
            if raw_path.exists():
                raw_path.unlink()
        except OSError:
            logger.warning(f"No se pudo eliminar RAW previo: {raw_path}")

    rows: list[list[float]] = []
    cursor = start_ms
    calls = 0
    written_any = raw_path.exists() and raw_path.stat().st_size > 0

    try:
        while cursor < end_ms:
            remaining = max(0, math.ceil((end_ms - cursor) / tf_ms))
            take = max(1, min(cfg.limit_per_call, remaining))

            batch = _with_retries(
                mkt.fetch_ohlcv, cfg.symbol, cfg.timeframe, since_ms=cursor, limit=take
            )
            calls += 1
            if not batch:
                logger.warning("fetch_ohlcv vacío; avanzamos una vela para evitar bucle.")
                cursor += tf_ms
                continue

            # Validaciones de integridad por batch
            ts = [r[0] for r in batch]
            if any(b <= a for a, b in zip(ts, ts[1:])):
                raise RuntimeError("Timestamps no monótonos en batch OHLCV.")
            if any((t % tf_ms) != 0 for t in ts):
                raise RuntimeError(
                    f"Timestamps no alineados al grid de {cfg.timeframe} (tf_ms={tf_ms})."
                )

            rows.extend(batch)
            cursor = int(ts[-1] + tf_ms)

            # Volcado incremental a RAW
            if len(rows) >= CHUNK_SIZE:
                pd.DataFrame(
                    rows,
                    columns=["timestamp", "open", "high", "low", "close", "volume"],
                ).to_csv(raw_path, mode="a", header=not written_any, index=False)
                written_any = True
                rows.clear()

            if calls % 10 == 0:
                logger.info(f"Progreso: {(cursor - start_ms) // tf_ms} velas… {_iso_utc(cursor)}")

        # Cola pendiente → RAW
        if rows:
            pd.DataFrame(
                rows, columns=["timestamp", "open", "high", "low", "close", "volume"]
            ).to_csv(raw_path, mode="a", header=not written_any, index=False)
            written_any = True
            rows.clear()

    except KeyboardInterrupt:
        if rows:
            pd.DataFrame(
                rows, columns=["timestamp", "open", "high", "low", "close", "volume"]
            ).to_csv(raw_path, mode="a", header=not written_any, index=False)
        logger.warning("Descarga interrumpida. Parcial (RAW) escrito.")
        raise

    if not written_any or not raw_path.exists():
        raise RuntimeError("No se descargaron velas. Revisa símbolo/timeframe/rango.")

    # ===== Normalización final: leer RAW, ordenar, validar y escribir FINAL atómicamente =====
    raw = pd.read_csv(raw_path)
    if "timestamp" not in raw.columns:
        raise RuntimeError(f"RAW en formato inesperado (sin 'timestamp'): {raw_path}")

    raw["datetime"] = pd.to_datetime(raw["timestamp"], unit="ms", utc=True)
    raw = raw.drop_duplicates(subset=["datetime"]).sort_values("datetime").reset_index(drop=True)

    final_df = _ensure_utc_index(raw)
    _validate_ohlc_sanity(final_df)  # Sanidad antes de persistir

    tmp_path = fpath.with_suffix(".csv.tmp")
    final_df.to_csv(tmp_path, index=True)  # índice = datetime (columna 'datetime' en CSV)
    os.replace(tmp_path, fpath)

    # Limpieza del RAW
    try:
        os.remove(raw_path)
    except OSError:
        logger.warning(f"No se pudo eliminar RAW temporal: {raw_path}")

    logger.info(f"Guardado {len(final_df):,} velas normalizadas en {fpath}")
    return fpath


# =======================
# ==== Helpers de lectura
# =======================
def read_ohlcv_csv(path: str | Path) -> pd.DataFrame:
    """
    Lee el CSV final normalizado (con índice 'datetime' UTC) y devuelve un DataFrame tipado.

    Parameters
    ----------
    path : str | Path
        Ruta al CSV final generado por `download_ohlcv`.

    Returns
    -------
    pd.DataFrame
        DataFrame con índice DatetimeIndex (UTC) y columnas float64:
        ['open','high','low','close','volume'].
    """
    df = pd.read_csv(path, parse_dates=["datetime"])
    if "datetime" not in df.columns:
        raise RuntimeError("CSV sin columna 'datetime'. ¿Es el CSV final normalizado?")
    df["datetime"] = pd.to_datetime(df["datetime"], utc=True)
    df = df.set_index("datetime").sort_index()
    cols = ["open", "high", "low", "close", "volume"]
    return df[cols].astype("float64")


def download_ohlcv_and_read(cfg: FetchConfig) -> tuple[Path, pd.DataFrame]:
    """
    Conveniencia para el CLI:
    - Descarga y normaliza OHLCV (igual que `download_ohlcv`).
    - Devuelve también el DataFrame ya cargado desde el CSV final.

    Returns
    -------
    (Path, pd.DataFrame)
        (ruta_csv_final, df_normalizado)
    """
    csv_path = download_ohlcv(cfg)
    df = read_ohlcv_csv(csv_path)
    return csv_path, df
