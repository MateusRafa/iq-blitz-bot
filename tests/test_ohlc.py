"""Testes do normalizador OHLC (sem Pocket/Supabase)."""

from bot.ohlc_collector import TIMEFRAMES, normalize_candle


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
        timeframe="5m",
    )
    assert row is not None
    assert row["asset"] == "EURUSD_otc"
    assert row["timeframe"] == "5m"
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
        normalize_candle({"time": 1, "open": 1}, asset="X", timeframe="5m")
        is None
    )


def test_timeframes_six():
    assert list(TIMEFRAMES.keys()) == ["5m", "15m", "30m", "1h", "4h", "1d"]
    assert TIMEFRAMES["1d"] == 86400
