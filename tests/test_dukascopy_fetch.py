"""Testes do fetcher Dukascopy (bi5 → 1h) sem rede."""

from datetime import datetime, timezone
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
    assert len(rows) == 1
    assert rows[0]["source"] == "dukascopy"
    assert rows[0]["timeframe"] == "1h"
    assert rows[0]["asset"] == "EURUSD"
