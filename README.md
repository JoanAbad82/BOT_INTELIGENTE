# BOT_INTELIGENTE

Proyecto de bot de trading (XRP/USDC, 15m) con CCXT, pandas y Loguru.

## Validación de datasets y manifiestos

### Uso correcto
> Nota: `check_dataset.py` acepta el CSV por **posición** (sin `--csv`).

```bash
poetry run python -m src.tools.check_dataset data/ohlcv/XRPUSDC_15m_2025-07-15_2025-09-18_filled.csv   --freq 15min   --sanity-ohlc   --manifest-out reports/manifests/XRPUSDC_15m_2025-07-15_2025-09-18_filled.manifest.json
```

| Código | Significado                                   |
| -----: | --------------------------------------------- |
|      0 | OK                                            |
|      2 | Entrada inválida (ruta/CSV/argumentos)        |
|      3 | Violación de contrato (gaps/dups/cols/dtypes) |
|      4 | Error de E/S (lectura/escritura)              |
|      5 | Error inesperado / excepción no controlada    |
