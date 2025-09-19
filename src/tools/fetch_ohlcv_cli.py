# ==========================================
# ========== FILE: src/tools/fetch_ohlcv_cli.py
# ==========================================
from __future__ import annotations

import re
import sys
import argparse
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from loguru import logger
from ccxt.base.errors import NetworkError, ExchangeError, RequestTimeout

from src.data.ohlcv_downloader import FetchConfig, download_ohlcv
from src.utils.logging import setup_logging
from src.config.settings import settings


# ---------------------------
# Utilidades de parsing
# ---------------------------
def _parse_iso_utc(s: Optional[str]) -> Optional[datetime]:
    """
    Acepta:
      - 2025-09-01
      - 2025-09-01T00:00
      - 2025-09-01T00:00:00
      - 2025-09-01T00:00:00Z
      - 2025-09-01T00:00:00+00:00
    Devuelve datetime tz-aware en UTC o None si s es None/''.
    """
    if s is None:
        return None
    s = s.strip()
    if not s:
        return None
    # Normaliza sufijo Z -> +00:00
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except Exception as e:
        raise argparse.ArgumentTypeError(f"Fecha inválida: {s!r} ({e})")
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt


def _validate_symbol_usdc(sym: str) -> str:
    """
    Fuerza formato BASE/USDC (mayúsculas, letras/números/guiones).
    Ej.: XRP/USDC, BTC/USDC, ETH-POW/USDC
    """
    s = sym.strip().upper()
    if not re.fullmatch(r"[A-Z0-9\-]+/USDC", s):
        raise argparse.ArgumentTypeError(
            f"Símbolo inválido o no USDC: {sym!r}. Formato requerido: BASE/USDC (p. ej., XRP/USDC)."
        )
    return s


# ---------------------------
# CLI
# ---------------------------
def main() -> None:
    setup_logging("BOT_INTELIGENTE")

    ap = argparse.ArgumentParser(
        description=(
            "Descarga OHLCV desde Binance (LIVE) con integridad (monotonía + grid), "
            "reintentos y escritura final atómica."
        )
    )
    ap.add_argument(
        "--symbol",
        default=None,
        help="Símbolo (por defecto: settings.default_symbol). Requiere formato BASE/USDC.",
    )
    ap.add_argument(
        "--timeframe",
        default="15m",
        help="Timeframe CCXT (1m, 5m, 15m, 1h, 4h, 1d). Por defecto: 15m.",
    )
    ap.add_argument(
        "--since",
        type=_parse_iso_utc,
        default=None,
        help="Inicio ISO (p. ej., 2025-09-01T00:00:00Z). Si no se pasa: ~30 días atrás.",
    )
    ap.add_argument(
        "--until",
        type=_parse_iso_utc,
        default=None,
        help="Fin ISO (p. ej., 2025-09-05T00:00:00Z). Si no se pasa: ahora.",
    )
    ap.add_argument(
        "--outdir",
        default=str(Path(__file__).resolve().parents[2] / "data" / "ohlcv"),
        help="Directorio de salida (por defecto: data/ohlcv).",
    )
    ap.add_argument(
        "--limit-per-call",
        type=int,
        default=1000,
        help="Límite por llamada a CCXT (máximo habitual en Binance=1000).",
    )
    ap.add_argument(
        "--no-reload-markets",
        action="store_true",
        help="No forzar reload de mercados (por defecto se recargan).",
    )

    args = ap.parse_args()

    # Símbolo (validado BASE/USDC)
    symbol = args.symbol or settings.default_symbol
    try:
        symbol = _validate_symbol_usdc(symbol)
    except argparse.ArgumentTypeError as e:
        print(f"[ARGS] {e}")
        sys.exit(2)

    # Construcción de configuración
    cfg = FetchConfig(
        symbol=symbol,
        timeframe=args.timeframe,
        since=args.since,
        until=args.until,
        outdir=Path(args.outdir),
        limit_per_call=int(args.limit_per_call),
        reload_markets=not args.no_reload_markets,
    )

    # Ejecución con manejo de errores y exit codes coherentes
    try:
        path = download_ohlcv(cfg)
        logger.info(f"Archivo generado: {path}")
        print(path)  # salida amigable para pipelines
        sys.exit(0)

    except (NetworkError, RequestTimeout) as e:
        print(f"[NET] Problema de red/timeout: {e}")
        sys.exit(11)
    except ExchangeError as e:
        # Mejoramos el mensaje si el error es por símbolo inexistente (passthrough de downloader)
        msg = str(e)
        if "no existe en Binance LIVE" in msg and "Candidatos USDC:" in msg:
            print(f"[EXCHANGE] {msg}")
        else:
            print(f"[EXCHANGE] Error del exchange: {msg}")
        sys.exit(12)
    except KeyboardInterrupt:
        print("\n[INTERRUPTED] Descarga cancelada por el usuario.")
        sys.exit(130)
    except Exception as e:
        print(f"[UNEXPECTED] {e.__class__.__name__}: {e}")
        sys.exit(99)


if __name__ == "__main__":
    main()
