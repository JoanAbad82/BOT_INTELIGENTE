# ============================================
# ========== FILE: src/data/ohlcv_downloader.py
# ============================================

from __future__ import annotations

import math
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional, Callable, Any
import re

import ccxt
from ccxt.base.errors import NetworkError, ExchangeError, RequestTimeout
import pandas as pd
from loguru import logger

from src.config.settings import settings
from src.utils.logging import setup_logging


# =======================
# ==== Utilidades
# =======================
def _to_millis(dt: datetime) -> int:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def _iso_utc(ms: int) -> str:
    return datetime.utcfromtimestamp(ms / 1000).replace(tzinfo=timezone.utc).isoformat()


def _ensure_utc_index(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliza DataFrame final: índice datetime UTC, orden y tipos float64.
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


# --- NEW: sanity check OHLC/volume ---
def _validate_ohlc_sanity(df: pd.DataFrame) -> None:
    """
    Reglas mínimas:
      - low <= min(open, close, high)
      - high >= max(open, close, low)
      - volume >= 0
    """
    # Nota: df ya está en float64 y con columnas garantizadas
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
    since: Optional[datetime] = None
    until: Optional[datetime] = None
    outdir: Path = Path(__file__).resolve().parents[2] / "data" / "ohlcv"
    limit_per_call: int = 1000
    reload_markets: bool = True


class MarketExchange:
    """Exchange solo para DATOS (LIVE). Forzamos spot y rate limit."""
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
            pass

    def load(self, reload: bool = True) -> None:
        self.ex.load_markets(reload=reload)

    @property
    def symbols(self) -> List[str]:
        return list(self.ex.symbols)

    def has_symbol(self, s: str) -> bool:
        return s in self.ex.markets

    def fetch_ohlcv(self, symbol: str, timeframe: str, since_ms: Optional[int], limit: int):
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
    Descarga OHLCV en formato RAW (timestamp,...), y crea un CSV FINAL normalizado:
      - Validación de símbolo BASE/USDC y existencia.
      - Timestamps monótonos y alineados al grid.
      - Volcado RAW incremental a *.raw.csv
      - Escritura final ATÓMICA a *.csv
      - Ctrl+C seguro (parcial queda en RAW)
    Devuelve Path del CSV FINAL.
    """
    setup_logging("BOT_INTELIGENTE")
    logger.info(f"Descargando OHLCV | symbol={cfg.symbol} tf={cfg.timeframe}")

    mkt = MarketExchange()
    mkt.load(reload=cfg.reload_markets)

    # Validación símbolo USDC + formato
    if not re.fullmatch(r"[A-Z0-9\-]+/USDC", cfg.symbol):
        raise ValueError(f"Formato de símbolo inválido o no USDC: {cfg.symbol!r}")
    if not mkt.has_symbol(cfg.symbol):
        base = cfg.symbol.split("/")[0]
        candidates = [s for s in mkt.symbols if s.startswith(base + "/") and s.endswith("/USDC")]
        raise ValueError(
            f"El símbolo {cfg.symbol} no existe en Binance LIVE. Candidatos USDC: {candidates[:15]}"
        )

    # Tamaño de vela en ms
    try:
        tf_ms = int(pd.Timedelta(cfg.timeframe).total_seconds() * 1000)
    except Exception as e:
        raise ValueError(
            f"Timeframe inválido: {cfg.timeframe!r}. Ejemplos: '1m','15m','1h','1d'."
        ) from e
    if tf_ms <= 0:
        raise ValueError(f"Timeframe con duración no positiva: {cfg.timeframe!r}")

    # Rango temporal
    now = datetime.now(timezone.utc).replace(microsecond=0)
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
            f"since no alineado a {cfg.timeframe} (tf_ms={tf_ms}); {_iso_utc(start_ms)} → {_iso_utc(aligned)}."
        )
        start_ms = aligned
    if end_ms % tf_ms != 0:
        aligned = end_ms - (end_ms % tf_ms)
        logger.warning(
            f"until no alineado a {cfg.timeframe} (tf_ms={tf_ms}); {_iso_utc(end_ms)} → {_iso_utc(aligned)}."
        )
        end_ms = aligned

    # Rutas de salida
    outdir = cfg.outdir
    outdir.mkdir(parents=True, exist_ok=True)
    fname = (
        f"{cfg.symbol.replace('/','')}_{cfg.timeframe}_"
        f"{datetime.fromtimestamp(start_ms/1000, tz=timezone.utc).date()}_"
        f"{datetime.fromtimestamp(end_ms/1000, tz=timezone.utc).date()}.csv"
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

    rows: List[List[float]] = []
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

            # Validaciones de integridad
            ts = [r[0] for r in batch]
            if any(b <= a for a, b in zip(ts, ts[1:])):
                raise RuntimeError("Timestamps no monótonos en batch OHLCV.")
            if any((t % tf_ms) != 0 for t in ts):
                raise RuntimeError(f"Timestamps no alineados al grid de {cfg.timeframe} (tf_ms={tf_ms}).")

            rows.extend(batch)
            cursor = int(ts[-1] + tf_ms)

            # Volcado incremental a RAW
            if len(rows) >= CHUNK_SIZE:
                pd.DataFrame(rows, columns=["timestamp","open","high","low","close","volume"]).to_csv(
                    raw_path, mode="a", header=not written_any, index=False
                )
                written_any = True
                rows.clear()

            if calls % 10 == 0:
                logger.info(f"Progreso: {(cursor - start_ms) // tf_ms} velas… {_iso_utc(cursor)}")

        # Cola pendiente → RAW
        if rows:
            pd.DataFrame(rows, columns=["timestamp","open","high","low","close","volume"]).to_csv(
                raw_path, mode="a", header=not written_any, index=False
            )
            written_any = True
            rows.clear()

    except KeyboardInterrupt:
        if rows:
            pd.DataFrame(rows, columns=["timestamp","open","high","low","close","volume"]).to_csv(
                raw_path, mode="a", header=not written_any, index=False
            )
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
    _validate_ohlc_sanity(final_df)  # 👈 NEW: sanidad antes de persistir

    tmp_path = fpath.with_suffix(".csv.tmp")
    final_df.to_csv(tmp_path, index=True)
    os.replace(tmp_path, fpath)

    # Limpieza del RAW
    try:
        os.remove(raw_path)
    except OSError:
        logger.warning(f"No se pudo eliminar RAW temporal: {raw_path}")

    logger.info(f"Guardado {len(final_df):,} velas normalizadas en {fpath}")
    return fpath
