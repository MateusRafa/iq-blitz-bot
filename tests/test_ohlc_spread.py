"""Testes do spread OTC vs EURUSD (sem Pocket/Supabase)."""

from bot.ohlc_spread import build_spread_1h, detect_eurusd_opens


def test_spread_paired_and_weekend_carry():
    otc = [
        {
            "opened_at": "2024-06-14T20:00:00+00:00",  # sexta
            "close": 1.0800,
        },
        {
            "opened_at": "2024-06-15T12:00:00+00:00",  # sabado
            "close": 1.0850,
        },
        {
            "opened_at": "2024-06-17T08:00:00+00:00",  # segunda
            "close": 1.0810,
        },
    ]
    eurusd = [
        {
            "opened_at": "2024-06-14T20:00:00+00:00",
            "close": 1.0790,
        },
        {
            "opened_at": "2024-06-17T08:00:00+00:00",
            "close": 1.0805,
        },
    ]
    pts = build_spread_1h(otc, eurusd)
    assert len(pts) == 3
    assert abs(pts[0]["spread"] - (1.0800 - 1.0790)) < 1e-9
    assert pts[0]["mode"] == "paired"
    assert abs(pts[1]["spread"] - (1.0850 - 1.0790)) < 1e-9
    assert pts[1]["mode"] == "carry"
    assert pts[1]["after_hours"] is True
    assert abs(pts[2]["spread"] - (1.0810 - 1.0805)) < 1e-9
    assert pts[2]["mode"] == "paired"


def test_detect_eurusd_opens_after_daily_gap():
    rows = [
        {"opened_at": "2024-06-10T10:00:00+00:00"},
        {"opened_at": "2024-06-10T11:00:00+00:00"},
        # gap de 14h (fechamento diario)
        {"opened_at": "2024-06-11T01:00:00+00:00"},
        {"opened_at": "2024-06-11T02:00:00+00:00"},
    ]
    opens = detect_eurusd_opens(rows, gap_hours=2)
    assert len(opens) == 2
    assert opens[0]["opened_at"].startswith("2024-06-10T10:00:00")
    assert opens[1]["opened_at"].startswith("2024-06-11T01:00:00")
