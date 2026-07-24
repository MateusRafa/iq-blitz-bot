"""Testes do normalizador OHLC (sem Pocket/Supabase)."""

from bot.ohlc_collector import (
    TIMEFRAMES,
    normalize_candle,
    seconds_until_next_hourly_fetch,
)


def test_normalize_candle_basic():
    row = normalize_candle(
        {
            "time": 1_700_000_000,
            "open": 1.1,
            "high": 1.2,
            "low": 1.0,
            "close": 1.15,
        },
        asset="EURUSD_otc",
        timeframe="1h",
    )
    assert row is not None
    assert row["asset"] == "EURUSD_otc"
    assert row["timeframe"] == "1h"
    assert row["open"] == 1.1
    assert row["high"] == 1.2
    assert row["low"] == 1.0
    assert row["close"] == 1.15
    assert row["opened_at"].startswith("2023-")


def test_normalize_candle_ms_timestamp():
    row = normalize_candle(
        {"time": 1_700_000_000_000, "open": 1, "high": 2, "low": 0.5, "close": 1.5},
        asset="X",
        timeframe="1h",
    )
    assert row is not None
    assert "T" in row["opened_at"]


def test_normalize_candle_incomplete():
    assert (
        normalize_candle({"time": 1, "open": 1}, asset="X", timeframe="1h")
        is None
    )


def test_timeframes_only_1h():
    assert list(TIMEFRAMES.keys()) == ["1h"]
    assert TIMEFRAMES["1h"] == 3600


def test_seconds_until_next_hourly_fetch_positive():
    wait = seconds_until_next_hourly_fetch(after_hour_seconds=120)
    assert wait >= 1.0
    assert wait <= 3600 + 120
