# -*- coding: utf-8 -*-
# ==================================
# ========== FILE: src/main.py
# ==================================
from __future__ import annotations

import os
import sys
import time
from typing import List

import ccxt
from ccxt.base.errors import (
    AuthenticationError,
    ExchangeError,
    NetworkError,
    RequestTimeout,
)
from dotenv import load_dotenv


# -------------------------------
# Utilidades de entorno
# -------------------------------
def _env_bool(key: str, default: bool = True) -> bool:
    """
    Lee un booleano de variables de entorno con valores tipo 'true/false/1/0'.
    """
    raw = os.getenv(key, str(default)).strip().lower()
    return raw in {"1", "true", "yes", "y", "on"}


# -------------------------------
# Construcción del exchange
# -------------------------------
def _build_exchange() -> ccxt.binance:
    """
    Construye un cliente CCXT para Binance Spot en TESTNET con rate-limit y timeout
    configurables por variables de entorno. Fuerza sandbox y URLs de testnet si existen.
    """
    enable_rate_limit = _env_bool("ENABLE_RATE_LIMIT", True)
    timeout_ms = int(os.getenv("TIMEOUT_MS", "20000"))

    ex = ccxt.binance(
        {
            "apiKey": os.getenv("BINANCE_TESTNET_API_KEY", ""),
            "secret": os.getenv("BINANCE_TESTNET_API_SECRET", ""),
            "enableRateLimit": enable_rate_limit,
            "timeout": timeout_ms,
            "options": {"defaultType": "spot"},
        }
    )

    # TESTNET / Sandbox
    try:
        ex.set_sandbox_mode(True)
    except Exception:
        # Si CCXT cambia internamente la estructura, no fallamos por esto.
        pass

    # Refuerza URLs de testnet si CCXT las expone en `urls["test"]`
    try:
        if "test" in ex.urls and ex.urls["test"]:
            ex.urls["api"] = ex.urls["test"]
    except Exception:
        # Ignorar cambios internos en CCXT.
        pass

    return ex


# -------------------------------
# Validación de símbolo USDC
# -------------------------------
def _validate_symbol_usdc(ex: ccxt.binance, symbol: str) -> bool:
    """
    Verifica que `symbol` exista en los mercados cargados del exchange.
    Si no existe, sugiere candidatos con misma base y *quote* USDC.

    Devuelve:
        True  -> el símbolo existe en `ex.markets`
        False -> no existe; imprime advertencias y posibles candidatos
    """
    if symbol in ex.markets:
        return True

    base = symbol.split("/")[0] if "/" in symbol else symbol
    candidates: List[str] = [
        s for s in ex.symbols if s.startswith(base + "/") and s.endswith("/USDC")
    ]

    print(f"[WARN] Símbolo '{symbol}' no existe en TESTNET.")
    if candidates:
        print(f"       Candidatos con misma base y quote USDC: {candidates[:20]}")
    else:
        print("       No se encontraron candidatos USDC con la misma base.")
    return False


# -------------------------------
# Programa principal
# -------------------------------
def main() -> None:
    """
    Script de verificación de conectividad a Binance Spot TESTNET (USDC como quote).

    Códigos de salida:
      0   OK
      2   Símbolo inválido/no disponible
      10  Error de autenticación
      11  Error de red / timeout
      12  Error del exchange
      130 Interrupción por teclado (Ctrl+C)
      99  Error inesperado
    """
    load_dotenv()
    symbol = os.getenv("DEFAULT_SYMBOL", "XRP/USDC")

    try:
        # 1) Construcción del exchange (no hace llamadas de red)
        ex = _build_exchange()

        # 2) Ping/time: latencia y deriva de reloj
        server_ms = ex.fetch_time()
        local_ms = int(time.time() * 1000)
        drift_ms = int(server_ms - local_ms)
        print(f"[OK] fetch_time | server={server_ms} local={local_ms} drift={drift_ms} ms")

        # 3) Carga de mercados
        ex.load_markets(reload=True)
        print(f"[OK] load_markets | total={len(ex.markets)}")

        # 4) Validación del símbolo (USDC obligatorio en este proyecto)
        if not _validate_symbol_usdc(ex, symbol):
            sys.exit(2)

        # 5) Ticker
        t = ex.fetch_ticker(symbol)
        print(
            f"[OK] ticker {symbol} | "
            f"bid={t.get('bid')} ask={t.get('ask')} last={t.get('last')} @ {t.get('datetime')}"
        )
        print("\n✅ Conexión a Binance Spot TESTNET verificada correctamente.")

    except AuthenticationError as e:
        print(f"[AUTH] Error de autenticación: {e}")
        sys.exit(10)
    except (NetworkError, RequestTimeout) as e:
        print(f"[NET] Problema de red/timeout: {e}")
        sys.exit(11)
    except ExchangeError as e:
        print(f"[EXCHANGE] Error del exchange: {e}")
        sys.exit(12)
    except KeyboardInterrupt:
        print("\n[INTERRUPTED] Ejecución cancelada por el usuario.")
        sys.exit(130)
    except Exception as e:
        # Última barrera: documentamos y emitimos clase + msg (no silencia nada)
        err = f"{e.__class__.__name__}: {e}"
        print(f"[UNEXPECTED] Error no controlado: {err}")
        # Opcional: añadir hint de soporte/log si procede
        sys.exit(99)


if __name__ == "__main__":
    main()
