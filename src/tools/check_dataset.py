# SPDX-License-Identifier: MIT
# Copyright (c) 2025 Joan Abad and contributors
# ==========================================
# ========== FILE: src/tools/check_dataset.py
# ==========================================
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path
from typing import Any

import pandas as pd

EXPECTED_COLS = {"open", "high", "low", "close", "volume"}
DEFAULT_FREQ_PANDAS = "15min"  # evitar FutureWarnings ("T" -> "min")


class ExitCode:
    OK = 0
    BAD_INPUT = 2
    CONTRACT_VIOLATION = 3
    IO_ERROR = 4
    UNKNOWN = 5


def _ensure_utc_index(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliza el índice datetime a UTC (tz-aware) y lo ordena ASC,
    sin eliminar duplicados (para poder reportarlos correctamente).
    """
    if not isinstance(df.index, pd.DatetimeIndex):
        df.index = pd.to_datetime(df.index, utc=True, errors="coerce")

    if df.index.tz is None:
        df.index = df.index.tz_localize("UTC")
    else:
        df.index = df.index.tz_convert("UTC")

    return df.sort_index()  # 👈 no deduplicamos aquí


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _build_expected_grid(df: pd.DataFrame, freq: str) -> pd.DatetimeIndex:
    return pd.date_range(df.index.min(), df.index.max(), freq=freq, tz="UTC")


def _check_ohlc_sanity(df: pd.DataFrame) -> tuple[bool, dict[str, int]]:
    problems = {
        "negatives": int(((df[["open", "high", "low", "close", "volume"]] < 0).any(axis=1)).sum()),
        "low_gt_high": int((df["low"] > df["high"]).sum()),
        "mid_outside_hilo": int(
            ~((df["low"] <= df["open"]) & (df["open"] <= df["high"])).sum()
            + ~((df["low"] <= df["close"]) & (df["close"] <= df["high"])).sum()
        ),
        "nan_rows": int(df[["open", "high", "low", "close", "volume"]].isna().any(axis=1).sum()),
    }
    is_ok = (
        problems["negatives"] == 0 and problems["low_gt_high"] == 0 and problems["nan_rows"] == 0
    )
    return is_ok, problems


def _calc_manifest(
    csv_path: Path,
    df: pd.DataFrame,
    freq: str,
    sanity_ohlc: bool,
) -> dict[str, Any]:
    exp_grid = _build_expected_grid(df, freq)
    present = df.index

    missing = exp_grid.difference(present)
    dups_count = int(df.index.duplicated(keep=False).sum())
    completeness = 0.0 if len(exp_grid) == 0 else 100.0 * (len(present.unique()) / len(exp_grid))

    ohlc_ok, ohlc_details = (True, {}) if not sanity_ohlc else _check_ohlc_sanity(df)

    manifest: dict[str, Any] = {
        "file": str(csv_path),
        "sha256": _sha256_file(csv_path),
        "index": {
            "tz": "UTC",
            "start": present.min().isoformat(),
            "end": present.max().isoformat(),
            "freq": freq,
            "expected_candles": len(exp_grid),
            "present_candles": int(len(present)),
            "unique_candles": int(len(present.unique())),
            "completeness_pct": round(completeness, 6),
            "duplicates": dups_count,
            "missing": int(len(missing)),
        },
        "columns": {
            "expected": sorted(EXPECTED_COLS),
            "actual": sorted(map(str, df.columns)),
            "dtypes": {k: str(df[k].dtype) for k in df.columns if k in EXPECTED_COLS},
        },
        "sanity": {
            "checked": bool(sanity_ohlc),
            "ok": bool(ohlc_ok),
            "details": ohlc_details,
        },
        "generated_by": "check_dataset.py",
        "version": "1.1.0",
    }
    return manifest


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="check_dataset",
        description=(
            "Valida un dataset OHLCV contra un contrato "
            "(UTC index, rejilla fija, columnas, dtypes). "
            "Puede emitir un manifiesto JSON con métricas reproducibles."
        ),
    )
    parser.add_argument(
        "csv_path",
        help="Ruta al CSV a validar (índice datetime en la primera columna o columna 'timestamp').",
    )
    parser.add_argument(
        "--freq",
        default=DEFAULT_FREQ_PANDAS,
        help='Frecuencia de rejilla Pandas (por defecto "15min"). Usa "min" y no "T".',
    )
    parser.add_argument(
        "--sanity-ohlc",
        action="store_true",
        help=(
            "Activa chequeos básicos de sanidad (precios/volumen no negativos, "
            "y que se cumpla low<=open/close<=high)."
        ),
    )
    parser.add_argument(
        "--manifest-out",
        type=str,
        default=None,
        help="Si se especifica, escribe un JSON con el manifiesto en esta ruta.",
    )
    parser.add_argument(
        "--strict-grid",
        action="store_true",
        help="Exige que todos los timestamps estén en la rejilla exacta (sin desalineados).",
    )

    args = parser.parse_args(argv)
    csv_path = Path(args.csv_path)

    if not csv_path.exists():
        print(f"[ERROR] No existe el archivo: {csv_path}", file=sys.stderr)
        return ExitCode.BAD_INPUT

    try:
        # Cargamos permitiendo índice en primera col o en 'timestamp'
        # - Si la primera columna tiene aspecto de datetime, se usará como índice.
        # - Si hay columna 'timestamp', la preferimos.
        df = pd.read_csv(csv_path)
        if "timestamp" in df.columns:
            df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
            df = df.set_index("timestamp")
        else:
            first_col = df.columns[0]
            df[first_col] = pd.to_datetime(df[first_col], utc=True, errors="coerce")
            df = df.set_index(first_col)

        # Normalizamos índice
        df = _ensure_utc_index(df)

        # Columnas mínimas
        missing_cols = EXPECTED_COLS.difference(df.columns)
        if missing_cols:
            print(f"[ERROR] Faltan columnas: {sorted(missing_cols)}", file=sys.stderr)
            return ExitCode.CONTRACT_VIOLATION

        # Dtypes recomendados
        for c in ["open", "high", "low", "close", "volume"]:
            if not pd.api.types.is_numeric_dtype(df[c].dtype):
                print(f"[ERROR] Columna no numérica: {c} -> {df[c].dtype}", file=sys.stderr)
                return ExitCode.CONTRACT_VIOLATION

        # Rejilla esperada y desalineados
        exp_grid = _build_expected_grid(df, args.freq)
        if getattr(args, "strict_grid", False):
            # Comprobamos que TODOS los timestamps estén en la rejilla esperada
            not_in_grid = ~df.index.isin(exp_grid)
            if bool(not_in_grid.any()):
                bad_count = int(not_in_grid.sum())
                sample = df.index[not_in_grid][:5]
                print(
                    f"[ERROR] {bad_count} filas desalineadas con la rejilla {args.freq}. "
                    f"Ejemplos: {list(map(lambda x: x.isoformat(), sample))}",
                    file=sys.stderr,
                )
                return ExitCode.CONTRACT_VIOLATION

        # Métricas
        duplicates = int(df.index.duplicated(keep=False).sum())
        missing = int(len(exp_grid.difference(df.index)))
        completeness = (
            0.0 if len(exp_grid) == 0 else 100.0 * (len(df.index.unique()) / len(exp_grid))
        )

        # Sanidad OHLC (opcional)
        ohlc_ok: bool = True
        ohlc_details: dict[str, Any] = {}
        if args.sanity_ohlc:
            _ok, _details = _check_ohlc_sanity(df)
            ohlc_ok = _ok
            ohlc_details = _details
            if not ohlc_ok:
                print(
                    f"[ERROR] Sanidad OHLC/volumen fallida: {ohlc_details}",
                    file=sys.stderr,
                )
                # Permitimos emitir manifiesto para diagnóstico
                # pero devolvemos código de contrato al final.

        # Manifiesto (opcional)
        if args.manifest_out:
            manifest = _calc_manifest(csv_path, df, args.freq, args.sanity_ohlc)
            outp = Path(args.manifest_out)
            outp.parent.mkdir(parents=True, exist_ok=True)
            outp.write_text(json.dumps(manifest, ensure_ascii=False, indent=2))
            print(f"[OK] Manifiesto escrito en {outp}")

        # Reporte simple a stdout
        print(
            "[SUMMARY] "
            f"range={df.index.min().isoformat()} → {df.index.max().isoformat()} | "
            f"freq={args.freq} | expected={len(exp_grid)} | unique={len(df.index.unique())} | "
            f"dups={duplicates} | missing={missing} | completeness={completeness:.4f}%"
        )

        # Códigos de salida
        if duplicates > 0 or missing > 0:
            return ExitCode.CONTRACT_VIOLATION
        if args.sanity_ohlc and not ohlc_ok:
            return ExitCode.CONTRACT_VIOLATION

        return ExitCode.OK

    except FileNotFoundError:
        print(f"[ERROR] No se pudo leer el archivo: {csv_path}", file=sys.stderr)
        return ExitCode.IO_ERROR
    except Exception as e:
        print(f"[ERROR] Excepción no controlada: {type(e).__name__}: {e}", file=sys.stderr)
        return ExitCode.UNKNOWN


if __name__ == "__main__":
    raise SystemExit(main())
