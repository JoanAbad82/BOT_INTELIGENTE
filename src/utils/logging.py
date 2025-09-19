from __future__ import annotations

import sys
from pathlib import Path

from loguru import logger


def setup_logging(app_name: str = "BOT_INTELIGENTE") -> None:
    log_dir = Path(__file__).resolve().parents[2] / "data" / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"{app_name.lower()}.log"

    logger.remove()  # limpia handlers por si se llama 2 veces
    logger.add(sys.stdout, level="INFO", enqueue=True)
    logger.add(log_file, level="DEBUG", rotation="10 MB", retention="14 days", enqueue=True)
