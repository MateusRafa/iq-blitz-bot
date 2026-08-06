"""Testes do import EURUSD diario (Investing.com PT)."""

from __future__ import annotations

from bot.eurusd_daily_import import parse_eurusd_daily_bytes


SAMPLE_CSV = (
    'Data,"Último","Abertura","Máxima","Mínima","Vol.","Var%"\n'
    '06.08.2026,"1,1523","1,1557","1,1560","1,1515","","-0,29%"\n'
    '05.08.2026,"1,1557","1,1531","1,1560","1,1526","","0,23%"\n'
    '04.08.2026,"1,1530","1,1510","1,1535","1,1502","","0,18%"\n'
)


def test_parse_investing_pt_csv():
    rows = parse_eurusd_daily_bytes(SAMPLE_CSV.encode("utf-8"), filename="eur.csv")
    assert len(rows) == 3
    assert rows[0]["close"] == 1.153
    assert rows[0]["open"] == 1.151
    assert rows[0]["high"] == 1.1535
    assert rows[0]["low"] == 1.1502
    assert rows[0]["timeframe"] == "1d"
    assert rows[0]["source"] == "manual_import"
    # Meia-noite Pocket (UTC-3) → 03:00 UTC
    assert "2026-08-04T03:00:00" in rows[0]["opened_at"]
    assert "2026-08-06T03:00:00" in rows[-1]["opened_at"]


def test_parse_single_column_excel_like_csv():
    # Simula Excel com tudo na coluna A (cada linha e um CSV).
    blob = (
        'Data,"Último","Abertura","Máxima","Mínima","Vol.","Var%"\n'
        '06.08.2026,"1,1523","1,1557","1,1560","1,1515","","-0,29%"\n'
    ).encode("utf-8")
    rows = parse_eurusd_daily_bytes(blob, filename="x.csv")
    assert len(rows) == 1
    assert rows[0]["close"] == 1.1523
    assert rows[0]["open"] == 1.1557
