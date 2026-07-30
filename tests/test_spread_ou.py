"""Testes do estimador AR(1)/OU no spread."""

import math
import random

from bot.spread_ou import (
    analyze_spread_ou,
    estimate_ar1,
    evaluate_paper_signal,
    prob_reversion_move,
)


def _ar1_series(n: int, a: float, b: float, sigma: float, seed: int = 1) -> list[float]:
    rng = random.Random(seed)
    x = a / (1.0 - b)
    out = [x]
    for _ in range(n - 1):
        x = a + b * x + rng.gauss(0.0, sigma)
        out.append(x)
    return out


def test_estimate_ar1_recovers_mean_reversion():
    # theta=0.002, kappa~0.2/h => phi = exp(-0.2) ≈ 0.8187
    phi_true = math.exp(-0.2)
    theta = 0.002
    a = theta * (1.0 - phi_true)
    xs = _ar1_series(800, a=a, b=phi_true, sigma=0.00015, seed=42)
    est = estimate_ar1(xs, min_n=48)
    assert est["ok"] is True
    assert est["mean_reverting"] is True
    assert abs(est["phi"] - phi_true) < 0.05
    assert abs(est["theta"] - theta) < 0.001
    assert est["half_life_hours"] is not None
    assert 2.5 < est["half_life_hours"] < 5.0  # ln2/0.2 ≈ 3.47


def test_prob_reversion_above_half_when_stretched():
    pr = prob_reversion_move(
        x0=0.010,
        theta=0.002,
        kappa=0.25,
        sigma_eps=0.0002,
        phi=math.exp(-0.25),
        tau_hours=4.0,
    )
    assert pr["usable"] is True
    assert pr["side_spread"] == "down"
    assert pr["otc_bias"] == "put"
    assert pr["p_reversion"] > 0.55
    assert pr["min_payout"] is not None
    assert pr["min_payout"] < 1.0


def test_analyze_spread_ou_paired_filter():
    pts = []
    xs = _ar1_series(120, a=0.0004, b=0.8, sigma=0.0001, seed=7)
    for i, v in enumerate(xs):
        pts.append(
            {
                "spread": v,
                "mode": "paired" if i % 5 else "carry",
            }
        )
    out = analyze_spread_ou(pts, paired_only=True, min_n=40)
    assert out["ok"] is True
    assert out["filter"] == "paired"
    assert "1h" in out["horizons"]
    assert "2h" in out["horizons"]
    assert "4h" in out["horizons"]


def test_estimate_ar1_too_few_points():
    est = estimate_ar1([0.1, 0.2, 0.15], min_n=48)
    assert est["ok"] is False


def test_evaluate_paper_signal_go_with_high_payout():
    ou = {
        "ok": True,
        "mean_reverting": True,
        "z": -1.6,
        "theta": 0.001,
        "x_last": -0.03,
        "half_life_hours": 40.0,
        "horizons": {
            "1h": {
                "hours": 1,
                "usable": True,
                "p_reversion": 0.55,
                "otc_bias": "call",
                "side_spread": "up",
                "min_payout": 0.82,
            },
            "2h": {
                "hours": 2,
                "usable": True,
                "p_reversion": 0.57,
                "otc_bias": "call",
                "side_spread": "up",
                "min_payout": 0.75,
            },
            "4h": {
                "hours": 4,
                "usable": True,
                "p_reversion": 0.60,
                "otc_bias": "call",
                "side_spread": "up",
                "min_payout": 0.67,
            },
        },
    }
    sig = evaluate_paper_signal(ou, payout=0.92, z_min=1.5, edge_margin=0.03)
    assert sig["action"] == "GO"
    assert sig["otc_side"] == "CALL"
    assert sig["horizon"] == "4h"
    assert sig["ev"] > 0


def test_evaluate_paper_signal_skip_low_z():
    ou = {
        "ok": True,
        "mean_reverting": True,
        "z": -0.4,
        "horizons": {
            "4h": {
                "hours": 4,
                "usable": True,
                "p_reversion": 0.7,
                "otc_bias": "call",
                "side_spread": "up",
            }
        },
    }
    sig = evaluate_paper_signal(ou, payout=0.92, z_min=1.5, edge_margin=0.03)
    assert sig["action"] == "SKIP"
