"""Testes do coletor OHLC diario (sem Pocket/Supabase)."""

from datetime import date, datetime, timedelta, timezone
from unittest.mock import patch

from bot.ohlc_collector_1d import (
    PERIOD_D1,
    TIMEFRAME,
    aggregate_hourly_to_daily,
    filter_closed_daily,
    pocket_day_key,
    pocket_midnight_utc,
    seconds_until_next_pocket_daily_fetch,
)
from bot.ohlc_store import candles_to_csv


def test_period_d1_is_86400():
    assert PERIOD_D1 == 86400
    assert TIMEFRAME == "1d"


def test_seconds_until_next_pocket_daily_fetch_positive():
    wait = seconds_until_next_pocket_daily_fetch(
        hour=0, minute=5, pocket_offset=-10800
    )
    assert wait >= 1.0
    assert wait <= 86400 + 60


def test_aggregate_hourly_to_daily_basic():
    off = 7200
    h1_open = datetime(2024, 1, 1, 22, 0, tzinfo=timezone.utc)
    h2_open = h1_open + timedelta(hours=1)
    hourly = [
        {
            "opened_at": h1_open.isoformat(),
            "open": 1.0,
            "high": 1.1,
            "low": 0.9,
            "close": 1.05,
        },
        {
            "opened_at": h2_open.isoformat(),
            "open": 1.05,
            "high": 1.2,
            "low": 1.0,
            "close": 1.15,
        },
    ]
    daily = aggregate_hourly_to_daily(
        hourly,
        asset="EURUSD_otc",
        pocket_offset=off,
        include_today=True,
    )
    assert len(daily) == 1
    assert daily[0]["open"] == 1.0
    assert daily[0]["high"] == 1.2
    assert daily[0]["low"] == 0.9
    assert daily[0]["close"] == 1.15
    expected_open = pocket_midnight_utc(date(2024, 1, 2), offset=off)
    assert daily[0]["opened_at"] == expected_open.isoformat()


def test_pocket_day_key_utc_minus_3():
    off = -10800
    with patch("bot.ohlc_collector_1d.pocket_tz_offset", return_value=off):
        dt = datetime(2024, 6, 15, 2, 0, tzinfo=timezone.utc)
        assert pocket_day_key(dt) == "2024-06-14"
        dt2 = datetime(2024, 6, 15, 3, 0, tzinfo=timezone.utc)
        assert pocket_day_key(dt2) == "2024-06-15"


def test_filter_closed_daily_skips_today():
    fixed = datetime(2024, 6, 15, 15, 0, tzinfo=timezone.utc)
    rows = [
        {
            "opened_at": (fixed - timedelta(days=1)).isoformat(),
            "open": 1.0,
            "high": 1.1,
            "low": 0.9,
            "close": 1.05,
        },
        {
            "opened_at": fixed.isoformat(),
            "open": 1.05,
            "high": 1.2,
            "low": 1.0,
            "close": 1.1,
        },
    ]
    with patch("bot.ohlc_collector_1d.datetime") as mock_dt:
        mock_dt.now.return_value = fixed
        mock_dt.fromisoformat = datetime.fromisoformat
        out = filter_closed_daily(rows)
    assert len(out) == 1


def test_candles_to_csv_header_1d():
    rows = [
        {
            "asset": "EURUSD_otc",
            "timeframe": "1d",
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
    assert "1d" in csv_text
