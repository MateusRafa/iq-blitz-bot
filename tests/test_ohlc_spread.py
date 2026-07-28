"""Testes do spread OTC vs EURUSD (sem Pocket/Supabase)."""

from bot.ohlc_spread import build_spread_1h


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
    # sexta paired
    assert abs(pts[0]["spread"] - (1.0800 - 1.0790)) < 1e-9
    assert pts[0]["mode"] == "paired"
    # sabado carry do close de sexta
    assert abs(pts[1]["spread"] - (1.0850 - 1.0790)) < 1e-9
    assert pts[1]["mode"] == "carry"
    assert pts[1]["weekend"] is True
    # segunda paired de novo
    assert abs(pts[2]["spread"] - (1.0810 - 1.0805)) < 1e-9
    assert pts[2]["mode"] == "paired"
