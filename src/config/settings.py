# ==================================
# ========== FILE: src/config/settings.py
# ==================================
from __future__ import annotations

import os
import re
from dataclasses import dataclass
from dotenv import load_dotenv

# Carga el .env en la raíz del proyecto
load_dotenv()


@dataclass
class Settings:
    """
    Configuración del proyecto BOT_INTELIGENTE.
    Regla clave: el símbolo por defecto DEBE ser BASE/USDC (quote fijo en USDC).
    """

    # Claves de API (testnet)
    binance_testnet_api_key: str = os.getenv("BINANCE_TESTNET_API_KEY", "")
    binance_testnet_api_secret: str = os.getenv("BINANCE_TESTNET_API_SECRET", "")

    # Símbolo por defecto (en este proyecto SIEMPRE cotizamos en USDC)
    default_symbol: str = os.getenv("DEFAULT_SYMBOL", "XRP/USDC").strip().upper()

    # Opciones de conexión
    enable_rate_limit: bool = (
        os.getenv("ENABLE_RATE_LIMIT", "true").strip().lower() == "true"
    )
    timeout_ms: int = int(os.getenv("TIMEOUT_MS", "20000"))
    sandbox_mode: bool = os.getenv("SANDBOX_MODE", "true").strip().lower() == "true"

    def __post_init__(self) -> None:
        """
        Valida estrictamente que default_symbol cumpla BASE/USDC (quote fijo USDC)
        y normaliza el valor a MAYÚSCULAS.
        """
        sym = self.default_symbol.strip().upper()
        if not re.fullmatch(r"[A-Z0-9\-]+/USDC", sym):
            raise ValueError(
                f"default_symbol inválido (formato BASE/USDC requerido y quote USDC): {self.default_symbol}"
            )
        # Normalización a mayúsculas por consistencia
        object.__setattr__(self, "default_symbol", sym)


settings = Settings()
