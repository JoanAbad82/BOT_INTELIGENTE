import importlib

import pytest


def test_default_symbol_is_usdc():
    from src.config.settings import settings

    assert settings.default_symbol.endswith("/USDC")


@pytest.mark.parametrize(
    "module",
    [
        "src.config.settings",
        "src.tools.fill_gaps",
        "src.data.ohlcv_downloader",
    ],
)
def test_modules_import(module):
    # Smoke test: que los módulos carguen sin lanzar excepciones
    importlib.import_module(module)
