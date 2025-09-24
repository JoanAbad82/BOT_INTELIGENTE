# SPDX-License-Identifier: MIT
# Copyright (c) 2025 Joan Abad and contributors
# ==========================================
# ========== FILE: src/tools/fetch_ohlcv_cli.py
# ==========================================
from __future__ import annotations

import argparse
import sys
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
from loguru import logger

from src.data.ohlcv_downloader import FetchConfig, download_ohlcv
from src.utils.logging import setup_logging


class ExitCode:
    OK = 0
    INVALID_INPUT = 2
    IO_ERROR = 4
    UNEXPECTED = 5


# ---------------------------
# Utilidades de parsing fechas
# ---------------------------
def parse_iso8601_dt(value: str | None) -> datetime | None:
    """
    Admite:
      - '2025-09-01T00:00:00Z'
      - '2025-09-01T00:00:00+02:00'
      - '2025-09-01' (asume 00:00:00Z)
    Devuelve datetime tz-aware en UTC.
    """
    if value is None or str(value).strip() == "":
        return None
    s = value.strip()
    try:
        # Normalizar 'Z' a +00:00 para fromisoformat
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            # Fecha sin tz -> asumir UTC
            dt = dt.replace(tzinfo=UTC)
        dt = dt.astimezone(UTC)
        return dt
    except Exception as e:  # noqa: BLE001
        raise argparse.ArgumentTypeError(f"Fecha inválida: {value!r} ({e})") from e


def validate_symbol_base_usdc(symbol: str) -> str:
    """
    En BOT_INTELIGENTE el estándar es BASE/USDC.
    - Acepta letras/números en BASE.
    - Rechaza USDT explícitamente.
    - Rechaza BASE='USDC' (no tendría sentido).
    """
    s = symbol.strip().upper()
    if "/" not in s:
        raise argparse.ArgumentTypeError("El símbolo debe ser del tipo BASE/USDC (con '/').")
    base, quote = s.split("/", 1)
    if quote != "USDC":
        raise argparse.ArgumentTypeError(
            "La cotización (quote) debe ser USDC (estándar del proyecto)."
        )
    if base in {"USDC", "USDT"}:
        raise argparse.ArgumentTypeError(
            "La base no puede ser USDC y no se admite USDT en el proyecto."
        )
    if not base.isalnum():
        raise argparse.ArgumentTypeError(
            "La base debe ser alfanumérica (sin espacios ni símbolos)."
        )
    return f"{base}/USDC"


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="fetch_ohlcv_cli",
        description=(
            "Descarga OHLCV desde el exchange vía CCXT y guarda " "CSV normalizado (UTC, 15m)."
        ),
    )
    p.add_argument(
        "--exchange",
        default="binance",
        help="ID de exchange CCXT (por defecto: binance).",
    )
    p.add_argument(
        "--symbol",
        required=True,
        type=validate_symbol_base_usdc,
        help="Símbolo BASE/USDC (obligatorio). Ej: XRP/USDC.",
    )
    p.add_argument(
        "--timeframe",
        default="15m",
        help="Timeframe CCXT (por defecto: 15m).",
    )
    p.add_argument(
        "--since",
        type=parse_iso8601_dt,
        help="Inicio ISO-8601 (ej: 2025-07-15T00:00:00Z o 2025-07-15+02:00).",
    )
    p.add_argument(
        "--until",
        type=parse_iso8601_dt,
        help="Fin ISO-8601 (exclusivo). Si se omite, usa 'ahora'.",
    )
    p.add_argument(
        "--limit-per-call",
        type=int,
        default=1000,
        help="Límite por llamada CCXT (por defecto: 1000).",
    )
    p.add_argument(
        "--outdir",
        type=Path,
        default=Path("data/ohlcv"),
        help="Directorio de salida para CSV (por defecto: data/ohlcv).",
    )
    p.add_argument(
        "--filename-suffix",
        default="",
        help="Sufijo opcional para el CSV (ej: _filled, _raw).",
    )
    p.add_argument(
        "--no-reload-markets",
        action="store_true",
        help="No llamar a exchange.load_markets() (por defecto se carga).",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Muestra la configuración y no descarga.",
    )
    p.add_argument(
        "--verbose",
        action="store_true",
        help="Logs detallados.",
    )
    return p


# -------------
# Programa main
# -------------
def main(argv: list[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    setup_logging(verbose=args.verbose)

    try:
        outdir: Path = args.outdir
        outdir.mkdir(parents=True, exist_ok=True)

        since_utc: datetime | None = args.since
        until_utc: datetime | None = args.until or datetime.now(UTC)

        if since_utc and until_utc and since_utc >= until_utc:
            logger.error("Rango temporal inválido: --since debe ser < --until.")
            return ExitCode.INVALID_INPUT

        # Construimos el FetchConfig compatible con la librería actual
        cfg = FetchConfig(
            symbol=args.symbol,
            timeframe=args.timeframe,
            since=since_utc,
            until=until_utc,
            limit_per_call=int(args.limit_per_call),
            outdir=outdir,  # Path esperado por el dataclass
            reload_markets=not args.no_reload_markets,
        )

        if args.dry_run:
            logger.info("DRY RUN - Configuración de descarga: {}", asdict(cfg))
            return ExitCode.OK

        logger.info(
            "Descargando OHLCV: exchange={}, symbol={}, timeframe={}, since={}, until={}, "
            "limit_per_call={}, outdir={}, reload_markets={}",
            args.exchange,
            args.symbol,
            args.timeframe,
            since_utc,
            until_utc,
            int(args.limit_per_call),
            str(outdir),
            not args.no_reload_markets,
        )

        # ====== Compatibilidad de retorno: Path o (Path, DataFrame) ======
        res = download_ohlcv(cfg)

        # Soporta Path o (Path, DataFrame)
        if isinstance(res, tuple):
            csv_path, df = res
        else:
            csv_path = res
            # Cargamos el DF para los post-checks de pureza
            df = pd.read_csv(csv_path, parse_dates=["datetime"], index_col="datetime")
            if not isinstance(df.index, pd.DatetimeIndex):
                df.index = pd.to_datetime(df.index, utc=True, errors="coerce")
            if df.index.tz is None:
                df.index = df.index.tz_localize("UTC")
            else:
                df.index = df.index.tz_convert("UTC")
        # ===============================================================

        # ----------------------------
        # Validaciones post-descarga
        # ----------------------------
        if df is None or df.empty:
            logger.error("No se obtuvieron velas (DataFrame vacío).")
            return ExitCode.IO_ERROR

        if not isinstance(df.index, pd.DatetimeIndex) or df.index.tz is None:
            logger.error("El índice del DataFrame debe ser tz-aware en UTC.")
            return ExitCode.UNEXPECTED

        if not df.index.is_monotonic_increasing:
            logger.error("El índice temporal no está ordenado ascendentemente.")
            return ExitCode.UNEXPECTED

        if df.index.has_duplicates:
            logger.error("Se detectaron timestamps duplicados en el dataset.")
            return ExitCode.UNEXPECTED

        logger.success("CSV generado: {}", csv_path)
        return ExitCode.OK

    except argparse.ArgumentTypeError as e:
        logger.error("Argumentos inválidos: {}", e)
        return ExitCode.INVALID_INPUT
    except OSError as e:
        logger.exception("Error de E/S: {}", e)
        return ExitCode.IO_ERROR
    except Exception:
        logger.exception("Error inesperado.")
        return ExitCode.UNEXPECTED


if __name__ == "__main__":
    sys.exit(main())
