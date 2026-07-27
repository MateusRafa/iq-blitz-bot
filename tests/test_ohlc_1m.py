"""Testes do coletor OHLC 1m (sem Pocket/Supabase)."""

from datetime import datetime, timedelta, timezone

from bot.ohlc_collector_1m import TIMEFRAMES, seconds_until_next_minute_fetch
from bot.ohlc_store import RETENTION_DAYS_1M, WARN_BEFORE_DAYS_1M, candles_to_csv


def test_timeframes_only_1m():
    assert list(TIMEFRAMES.keys()) == ["1m"]
    assert TIMEFRAMES["1m"] == 60


def test_seconds_until_next_minute_fetch_positive():
    wait = seconds_until_next_minute_fetch(after_minute_seconds=5)
    assert wait >= 1.0
    assert wait <= 60 + 5


def test_retention_constants():
    assert RETENTION_DAYS_1M == 90
    assert WARN_BEFORE_DAYS_1M == 1


def test_candles_to_csv_header():
    rows = [
        {
            "asset": "EURUSD_otc",
            "timeframe": "1m",
            "opened_at": datetime.now(timezone.utc).isoformat(),
            "open": 1.1,
            "high": 1.2,
            "low": 1.0,
            "close": 1.15,
            "volume": None,
        }
    ]
    csv_text = candles_to_csv(rows)
    assert "opened_at" in csv_text
    assert "EURUSD_otc" in csv_text
    assert "1.15" in csv_text


def test_warn_window_math():
    now = datetime.now(timezone.utc)
    delete_before = now - timedelta(days=RETENTION_DAYS_1M)
    warn_before = now - timedelta(days=RETENTION_DAYS_1M - WARN_BEFORE_DAYS_1M)
    assert warn_before > delete_before
    assert (warn_before - delete_before) == timedelta(days=1)
