import pandas as pd
import pytest


def _valid_df():
    return pd.DataFrame(
        {
            "open": [10.0],
            "high": [11.0],
            "low": [9.0],
            "close": [10.5],
            "volume": [100.0],
        },
        index=[pd.Timestamp("2025-09-18 00:00:00", tz="UTC")],
    )


def test_validate_ok():
    from src.data.ohlcv_downloader import _validate_ohlc_sanity

    df = _valid_df()
    # No debe lanzar
    _validate_ohlc_sanity(df)


def test_volume_negative_rejected():
    from src.data.ohlcv_downloader import _validate_ohlc_sanity

    df = _valid_df()
    df.loc[:, "volume"] = -1.0
    with pytest.raises((RuntimeError, ValueError, AssertionError)):
        _validate_ohlc_sanity(df)


def test_high_lower_than_low_rejected():
    from src.data.ohlcv_downloader import _validate_ohlc_sanity

    df = _valid_df()
    df.loc[:, "high"] = 8.0
    with pytest.raises((RuntimeError, ValueError, AssertionError)):
        _validate_ohlc_sanity(df)


def test_open_outside_range_rejected():
    from src.data.ohlcv_downloader import _validate_ohlc_sanity

    df = _valid_df()
    df.loc[:, "open"] = 12.0  # > high
    with pytest.raises((RuntimeError, ValueError, AssertionError)):
        _validate_ohlc_sanity(df)


def test_close_outside_range_rejected():
    from src.data.ohlcv_downloader import _validate_ohlc_sanity

    df = _valid_df()
    df.loc[:, "close"] = 8.5  # < low
    with pytest.raises((RuntimeError, ValueError, AssertionError)):
        _validate_ohlc_sanity(df)
