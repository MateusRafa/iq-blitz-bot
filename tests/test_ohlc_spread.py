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


def test_detect_eurusd_opens_one_per_day():
    # UTC-3: 2024-06-10 03:00 UTC = ainda dia 10 local; 02:00 UTC = dia 09 local
    rows = [
        {"opened_at": "2024-06-10T03:00:00+00:00"},  # dia 10 00:00 Pocket
        {"opened_at": "2024-06-10T04:00:00+00:00"},
        {"opened_at": "2024-06-10T10:00:00+00:00"},
        {"opened_at": "2024-06-11T03:00:00+00:00"},  # dia 11
        {"opened_at": "2024-06-11T05:00:00+00:00"},
    ]
    opens = detect_eurusd_opens(rows, pocket_offset=-10800)
    assert len(opens) == 2
    assert opens[0]["day"] == "2024-06-10"
    assert opens[0]["opened_at"].startswith("2024-06-10T03:00:00")
    assert opens[1]["day"] == "2024-06-11"
    assert opens[1]["opened_at"].startswith("2024-06-11T03:00:00")


def test_spread_1d_paired_and_weekend_carry():
    # opened_at = meia-noite Pocket (UTC-3) → 03:00 UTC
    otc = [
        {"opened_at": "2024-06-14T03:00:00+00:00", "close": 1.0800},  # sex
        {"opened_at": "2024-06-15T03:00:00+00:00", "close": 1.0850},  # sab
        {"opened_at": "2024-06-17T03:00:00+00:00", "close": 1.0810},  # seg
    ]
    eurusd = [
        {"opened_at": "2024-06-14T03:00:00+00:00", "close": 1.0790},
        {"opened_at": "2024-06-17T03:00:00+00:00", "close": 1.0805},
    ]
    from bot.ohlc_spread import build_spread_1d

    pts = build_spread_1d(otc, eurusd, pocket_offset=-10800)
    assert len(pts) == 3
    assert pts[0]["mode"] == "paired"
    assert pts[0]["day"] == "2024-06-14"
    assert pts[1]["mode"] == "carry"
    assert pts[1]["weekend"] is True
    assert abs(pts[1]["spread"] - (1.0850 - 1.0790)) < 1e-9
    assert pts[2]["mode"] == "paired"
