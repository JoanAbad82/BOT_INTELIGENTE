# SPDX-License-Identifier: MIT
# Copyright (c) 2025 Joan Abad and contributors
import pandas as pd


def test_ensure_utc_index_preserves_dups_and_sorts():
    # Importa la función tal como la tienes en tu repo
    from src.tools.check_dataset import _ensure_utc_index

    # Índice con mezcla: naive, tz no-UTC, desordenado y duplicado
    idx = pd.to_datetime(
        [
            "2025-09-18 10:15:00",  # naive
            "2025-09-18 10:00:00+02:00",  # tz-aware (CET)
            "2025-09-18 10:15:00+00:00",  # UTC (duplicado con la primera tras normalizar)
            "2025-09-18 09:45:00+02:00",  # desordenado
        ],
        format="mixed",
        utc=True,  # <- clave para evitar el FutureWarning
    )
    df = pd.DataFrame({"open": [1, 2, 3, 4]}, index=idx)

    out = _ensure_utc_index(df.copy())

    # 1) tz-aware y en UTC
    assert isinstance(out.index, pd.DatetimeIndex)
    assert out.index.tz is not None and str(out.index.tz) == "UTC"

    # 2) ordenado ascendente
    assert list(out.index) == sorted(out.index)

    # 3) NO deduplica: deben quedar 4 filas
    assert len(out) == 4

    # 4) Las dos entradas de 10:15:00 deben colisionar en el mismo instante UTC
    #    (naive 10:15 -> treated as UTC; 10:15+00:00 -> UTC; ambas 10:15Z)
    same = out.index[out.index == pd.Timestamp("2025-09-18 10:15:00", tz="UTC")]
    assert len(same) == 2
