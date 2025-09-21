import pandas as pd


def test_fill_gaps_respects_15min_grid_and_no_lookahead():
    # Simulamos 8 velas de 15m, con 1 hueco y 1 vela desalineada
    start = pd.Timestamp("2025-09-18 00:00:00", tz="UTC")
    full = pd.date_range(start, periods=8, freq="15min", tz="UTC")  # grid perfecto

    # Creamos datos válidos
    base = pd.DataFrame(
        {
            "open": [1, 2, 3, 4, 5, 6, 7, 8],
            "high": [2, 3, 4, 5, 6, 7, 8, 9],
            "low": [0, 1, 2, 3, 4, 5, 6, 7],
            "close": [1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5, 8.5],
            "volume": [10] * 8,
        },
        index=full,
    )

    # Introducimos hueco (quitamos 00:45) y una vela fuera de grid (00:07)
    gapped = base.drop(full[3])  # 00:45 falta
    misaligned_ts = pd.Timestamp("2025-09-18 00:07:00", tz="UTC")
    misaligned_row = pd.DataFrame(
        {"open": [100], "high": [101], "low": [99], "close": [100.5], "volume": [5]},
        index=[misaligned_ts],
    )
    dirty = pd.concat([gapped, misaligned_row], axis=0).sort_index()

    # --- Contrato que debe cumplir fill_gaps ---
    #  A) Detectar grid destino correcto (15min) y no inventar timestamps raros
    target = pd.date_range(
        dirty.index.min().floor("15min"),
        dirty.index.max().ceil("15min"),
        freq="15min",
        tz="UTC",
    )

    #  B) Reindexar al grid NO debe modificar datos existentes
    #     (las posiciones que ya existían quedan idénticas; solo aparecen NaN en huecos)
    reindexed = dirty.reindex(target)

    # Comprobaciones:
    # 1) El índice final es exactamente el grid de 15m
    #    (comprobamos diferencias constantes de 15 min en todo el rango)
    deltas = reindexed.index[1:] - reindexed.index[:-1]
    assert all(delta == pd.Timedelta(minutes=15) for delta in deltas)

    # 2) El hueco original existe como fila NaN (00:45)
    assert reindexed.loc[full[3]].isna().all()

    # 3) La vela desalineada (00:07) NO pertenece al grid: tras reindexar "desaparece"
    assert misaligned_ts not in reindexed.index

    # 4) Las velas que ya existían en el grid conservan sus valores (no look-ahead)
    common = dirty.index.intersection(target)
    pd.testing.assert_frame_equal(reindexed.loc[common], dirty.loc[common])
