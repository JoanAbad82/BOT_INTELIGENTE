# BOT_INTELIGENTE

Proyecto de bot de trading (**XRP/USDC**, 15m) con CCXT, pandas y Loguru.
> Política del repo: **USDC-only** (no se usa USDT).

![CI](https://github.com/JoanAbad82/BOT_INTELIGENTE/actions/workflows/ci.yml/badge.svg)
![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)

---

## Requisitos
- Python 3.11–3.12
- Poetry
- Git (pre-commit hooks)

```bash
poetry install
poetry run pre-commit install
```

---

## Validación de datasets y manifiestos

> Nota: `check_dataset.py` acepta el CSV por **posición** (sin `--csv`).

```bash
poetry run python -m src.tools.check_dataset \
  data/ohlcv/XRPUSDC_15m_2025-07-15_2025-09-18_filled.csv \
  --freq 15min \
  --sanity-ohlc \
  --manifest-out reports/manifests/XRPUSDC_15m_2025-07-15_2025-09-18_filled.manifest.json
```

| Código | Significado                                   |
| -----: | --------------------------------------------- |
|      0 | OK                                            |
|      2 | Entrada inválida (ruta/CSV/argumentos)        |
|      3 | Violación de contrato (gaps/dups/cols/dtypes) |
|      4 | Error de E/S (lectura/escritura)              |
|      5 | Error inesperado / excepción no controlada    |

---

## Dev tools

### Ejecutar linters/tests
```bash
poetry run ruff check .
poetry run black --check .
poetry run mypy .
poetry run pytest -q
poetry run pre-commit run --all-files
```

### Actualizar hooks de pre-commit (Windows)
```powershell
# Desde la raíz del repo
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.\scripts\update-hooks.ps1
```

> Hace: autoupdate de hooks en modo UTF-8, limpia entornos y ejecuta pre-commit.
> Alternativa manual:
> ```powershell
> $env:PYTHONUTF8 = "1"
> $env:PYTHONIOENCODING = "utf-8"
> poetry run python -X utf8 -m pre_commit.main autoupdate
> poetry run pre-commit clean
> poetry run pre-commit run --all-files
> ```

### Política de finales de línea (EOL)
Este repo usa **.gitattributes** para normalizar EOL:
- `*.py` → **LF**
- `*.ps1`/`*.bat` → **CRLF**
- Binarios (`.png`, `.zip`, `.pdf`, `.parquet`…) → **binary**

Si ves avisos de CRLF/LF, ejecuta:
```bash
git add --renormalize .
git commit -m "chore(repo): normalize line endings per .gitattributes"
```

---

## Licencia
- Licencia: **MIT** (ver `LICENSE` en la raíz).
- Cada archivo `.py` incluye cabecera SPDX:
  ```python
  # SPDX-License-Identifier: MIT
  # Copyright (c) 2025 Joan Abad and contributors
  ```

---
