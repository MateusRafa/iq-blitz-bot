"""Testes do importador CSV Dukascopy."""

import io

from bot.dukascopy_import import parse_dukascopy_csv


def test_parse_dukascopy_dot_format_bid():
    csv_text = """time,open,high,low,close,volume
19.07.2026 13:00:00.000,1.08450,1.08510,1.08420,1.08490,12.5
19.07.2026 14:00:00.000,1.08490,1.08520,1.08470,1.08500,10.0
"""
    rows = parse_dukascopy_csv(io.StringIO(csv_text))
    assert len(rows) == 2
    assert rows[0]["opened_at"].startswith("2026-07-19T13:00:00")
    assert rows[0]["open"] == 1.08450
    assert rows[0]["source"] == "dukascopy"
    assert rows[0]["timeframe"] == "1h"
    assert rows[0]["asset"] == "EURUSD"


def test_parse_dukascopy_bid_columns():
    csv_text = """timestamp,bid_open,bid_high,bid_low,bid_close,volume
2026-07-20 08:00:00,1.08000,1.08100,1.07950,1.08050,5
"""
    rows = parse_dukascopy_csv(io.StringIO(csv_text))
    assert len(rows) == 1
    assert rows[0]["close"] == 1.08050


def test_parse_skips_duplicate_hours():
    csv_text = """time,open,high,low,close
20.07.2026 09:00:00,1.1,1.2,1.0,1.15
20.07.2026 09:00:00,1.2,1.3,1.1,1.25
"""
    rows = parse_dukascopy_csv(io.StringIO(csv_text))
    assert len(rows) == 1
    assert rows[0]["open"] == 1.1
