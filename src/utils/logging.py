# SPDX-License-Identifier: MIT
# Copyright (c) 2025 Joan Abad and contributors
# ==========================================
# ========== FILE: src/utils/logging.py
# ==========================================
from __future__ import annotations

import sys
from pathlib import Path

from loguru import logger

DEFAULT_FORMAT = (
    "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
    "<level>{level: <8}</level> | "
    "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
    "<level>{message}</level>"
)


def setup_logging(
    verbose: bool = False,
    level: str | None = None,
    *,
    app_name: str = "BOT_INTELIGENTE",
    log_to_file: bool = True,
    log_dir: Path | None = None,
):
    """
    Configura Loguru de forma consistente para consola y (opcionalmente) fichero.

    Retrocompatible:
      - setup_logging() -> nivel INFO a consola + fichero DEBUG rotado
      - setup_logging(verbose=True) -> nivel DEBUG a consola
      - setup_logging(level="WARNING") -> nivel explícito a consola

    Parámetros:
      verbose     : si True, la consola usa nivel DEBUG (ignora 'level' si se pasa)
      level       : nivel explícito para consola (e.g., "WARNING", "INFO", "DEBUG")
      app_name    : nombre base del fichero de log (app_name.lower() + ".log")
      log_to_file : si True, añade sink a fichero con rotación
      log_dir     : directorio donde escribir logs; por defecto: <repo>/data/logs

    Devuelve:
      logger (loguru.logger)
    """
    # Limpia sinks previos por si se llama más de una vez
    logger.remove()

    # Resuelve nivel de consola
    resolved_level = level or ("DEBUG" if verbose else "INFO")

    # Sink a consola (stderr) con formato uniforme
    logger.add(
        sys.stderr,
        level=resolved_level,
        backtrace=False,  # evita trazas profundas (más ruido) en prod
        diagnose=False,  # mensajes concisos
        format=DEFAULT_FORMAT,
        enqueue=False,  # consola no necesita cola
    )

    # Sink a fichero con rotación (si procede)
    if log_to_file:
        # <repo root>/data/logs  (parents[2] sube desde src/utils/logging.py)
        base_log_dir = (
            log_dir
            if log_dir is not None
            else Path(__file__).resolve().parents[2] / "data" / "logs"
        )
        base_log_dir.mkdir(parents=True, exist_ok=True)
        log_file = base_log_dir / f"{app_name.lower()}.log"

        logger.add(
            log_file,
            level="DEBUG",  # capturamos todo en fichero
            rotation="10 MB",
            retention="14 days",
            backtrace=False,
            diagnose=False,
            format=DEFAULT_FORMAT,
            enqueue=True,  # escritura thread-safe
        )

    return logger
