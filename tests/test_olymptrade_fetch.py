"""Testes do adapter Olymptrade (sem rede / sem token)."""

from bot.olymptrade_fetch import (
    normalize_olymp_candle,
    rows_for_store,
    timeframe_floor,
)
from olymptrade_ws.api.market import _extract_candles


def test_normalize_olymp_candle_1h():
    raw = {
        "t": 1722337200,  # alinhado a hora
        "open": 1.08,
        "high": 1.09,
        "low": 1.07,
        "close": 1.085,
        "volume": 10,
    }
    row = normalize_olymp_candle(raw, asset="EURUSD_otc_olymp", timeframe="1h")
    assert row is not None
    assert row["source"] == "olymptrade"
    assert row["asset"] == "EURUSD_otc_olymp"
    assert row["open"] == 1.08
    assert row["high"] == 1.09
    assert row["low"] == 1.07
    assert row["close"] == 1.085
    assert "2024-07-30T11:00:00" in row["opened_at"] or row["opened_at"].startswith(
        "2024-07-30T"
    )


def test_normalize_ms_timestamp_and_sanitize():
    raw = {
        "time": 1722337200000,
        "o": 1.1,
        "h": 1.05,  # inconsistente → sanitiza
        "l": 1.2,
        "c": 1.12,
    }
    row = normalize_olymp_candle(raw, asset="EURUSD_otc_olymp", timeframe="1h")
    assert row is not None
    assert row["high"] == max(1.1, 1.05, 1.12)
    assert row["low"] == min(1.1, 1.2, 1.12)


def test_rows_for_store_dedup():
    raw = [
        {"t": 1722337200, "open": 1.0, "high": 1.1, "low": 0.9, "close": 1.05},
        {"t": 1722337200, "open": 1.0, "high": 1.2, "low": 0.8, "close": 1.06},
        {"t": 1722340800, "open": 1.05, "high": 1.07, "low": 1.04, "close": 1.06},
    ]
    rows = rows_for_store(raw, asset="EURUSD_otc_olymp")
    assert len(rows) == 2
    assert all(r["source"] == "olymptrade" for r in rows)


def test_timeframe_floor():
    assert timeframe_floor(1722337259, 3600) == 1722337200


def test_extract_candles_devtools_e18_envelope():
    """Formato real DevTools: e:18 d=[{p, tf, candles:[OHLC...]}]."""
    payload = [
        {
            "p": "EURUSD_OTC",
            "tf": 3600,
            "candles": [
                {
                    "t": 1725552000,
                    "open": 1.15077,
                    "high": 1.15371,
                    "low": 1.15025,
                    "close": 1.15038,
                },
                {
                    "t": 1725555600,
                    "open": 1.15038,
                    "high": 1.15100,
                    "low": 1.14990,
                    "close": 1.15050,
                },
            ],
        }
    ]
    bars = _extract_candles(payload)
    assert bars is not None
    assert len(bars) == 2
    assert bars[0]["p"] == "EURUSD_OTC"
    assert bars[0]["tf"] == 3600
    assert bars[0]["open"] == 1.15077
    rows = rows_for_store(bars, asset="EURUSD_otc_olymp")
    assert len(rows) == 2
    assert rows[0]["close"] == 1.15038
