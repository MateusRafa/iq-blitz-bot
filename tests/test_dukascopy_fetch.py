"""Testes do fetcher Dukascopy (bi5 → 1h) sem rede."""

from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from bot.dukascopy_fetch import (
    _ohlc_from_ticks,
    fetch_eurusd_1h_rows_for_store,
)


def test_ohlc_from_ticks_bid():
    hour = datetime(2026, 7, 28, 12, 0, tzinfo=timezone.utc)
    ticks = [
        (0, 1.10, 1.1002),
        (100, 1.11, 1.1102),
        (200, 1.09, 1.0902),
        (300, 1.105, 1.1052),
    ]
    c = _ohlc_from_ticks(hour, ticks, side="bid")
    assert c is not None
    assert c["open"] == 1.10
    assert c["high"] == 1.11
    assert c["low"] == 1.09
    assert c["close"] == 1.105
    assert c["opened_at"].startswith("2026-07-28T12:00:00")


def test_fetch_includes_current_hour_window():
    """Com include_current_hour, a hora 'agora' entra no intervalo."""
    from bot.dukascopy_fetch import fetch_eurusd_1h

    now = datetime(2026, 7, 29, 20, 15, tzinfo=timezone.utc)
    start = now - timedelta(hours=2)
    calls: list[datetime] = []

    def fake_one(symbol, hour_utc, *, side, timeout, retries):
        calls.append(hour_utc)
        return {
            "opened_at": hour_utc.isoformat(),
            "open": 1.0,
            "high": 1.0,
            "low": 1.0,
            "close": 1.0,
            "volume": 1.0,
        }

    with patch("bot.dukascopy_fetch._fetch_one_hour", side_effect=fake_one):
        rows = fetch_eurusd_1h(start, now, include_current_hour=True)
    hours = [c.hour for c in calls]
    assert 20 in hours  # hora corrente
    assert len(rows) >= 3


def test_fetch_rows_for_store_maps_source():
    fake = [
        {
            "opened_at": "2026-07-28T12:00:00+00:00",
            "open": 1.1,
            "high": 1.2,
            "low": 1.0,
            "close": 1.15,
            "volume": 10,
        }
    ]
    with patch("bot.dukascopy_fetch.fetch_eurusd_1h", return_value=fake):
        rows = fetch_eurusd_1h_rows_for_store(
            datetime(2026, 7, 28, tzinfo=timezone.utc),
            datetime(2026, 7, 29, tzinfo=timezone.utc),
            asset="EURUSD",
        )
    assert len(rows) >= 1
    assert rows[0]["source"] == "dukascopy"
    assert rows[0]["timeframe"] == "1h"
    assert rows[0]["asset"] == "EURUSD"


def test_ensure_provisional_current_hour_when_bi5_missing():
    from bot import dukascopy_fetch as mod

    now = datetime(2026, 7, 29, 20, 40, tzinfo=timezone.utc)  # quarta
    prev = now.replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)
    rows = [
        {
            "asset": "EURUSD",
            "timeframe": "1h",
            "opened_at": prev.isoformat(),
            "open": 1.1,
            "high": 1.1,
            "low": 1.1,
            "close": 1.12,
            "source": "dukascopy",
        }
    ]

    real_dt = datetime

    class _DT:
        @staticmethod
        def now(tz=None):
            return now

        @staticmethod
        def fromisoformat(s):
            return real_dt.fromisoformat(s)

    with patch.object(mod, "datetime", _DT):
        out = mod.ensure_provisional_current_hour(rows, asset="EURUSD")
    assert len(out) == 2
    assert out[-1]["close"] == 1.12
    assert "2026-07-29T20:00:00" in out[-1]["opened_at"]


def test_pad_eurusd_tail_adds_one_hour():
    from bot.ohlc_spread import pad_eurusd_tail_to_otc

    otc = [
        {"opened_at": "2026-07-29T18:00:00+00:00", "close": 1.15},
        {"opened_at": "2026-07-29T19:00:00+00:00", "close": 1.16},
        {"opened_at": "2026-07-29T20:00:00+00:00", "close": 1.17},
    ]
    eu = [
        {
            "asset": "EURUSD",
            "opened_at": "2026-07-29T18:00:00+00:00",
            "open": 1.10,
            "high": 1.10,
            "low": 1.10,
            "close": 1.10,
            "source": "dukascopy",
        },
        {
            "asset": "EURUSD",
            "opened_at": "2026-07-29T19:00:00+00:00",
            "open": 1.11,
            "high": 1.11,
            "low": 1.11,
            "close": 1.11,
            "source": "dukascopy",
        },
    ]
    out = pad_eurusd_tail_to_otc(otc, eu, max_hours=6)
    assert len(out) == 3
    assert "2026-07-29T20:00:00" in out[-1]["opened_at"]
    assert out[-1]["close"] == 1.11
