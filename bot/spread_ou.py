"""Estimativa AR(1) / Ornstein-Uhlenbeck no spread OTC - EURUSD (1h).

Fase 1 do sistema de sinal para binarias (expiry <= 4h):
  - theta, kappa, half-life, sigma, z
  - P(reversao em direcao da media) em 1h / 2h / 4h
  - payout minimo para EV >= 0
"""

from __future__ import annotations

import math
from typing import Any


HORIZONS_H = (1, 2, 4)


def _mean(xs: list[float]) -> float:
    return sum(xs) / len(xs)


def _norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def extract_spread_values(
    points: list[dict[str, Any]],
    *,
    paired_only: bool = True,
) -> list[float]:
    """Extrai serie de spread ordenada (ja vem ordenada por build_spread_1h)."""
    out: list[float] = []
    for p in points:
        if paired_only and p.get("mode") != "paired":
            continue
        try:
            out.append(float(p["spread"]))
        except (TypeError, ValueError, KeyError):
            continue
    return out


def estimate_ar1(
    values: list[float],
    *,
    dt_hours: float = 1.0,
    min_n: int = 48,
) -> dict[str, Any]:
    """Regressao OLS: X_{t+1} = a + b X_t + e.

    Mapeia para OU continuo (dt em horas):
      b = exp(-kappa * dt)
      theta = a / (1 - b)
    """
    n = len(values)
    if n < min_n:
        return {
            "ok": False,
            "error": f"Poucos pontos ({n}); minimo {min_n}.",
            "n": n,
        }

    x = values[:-1]
    y = values[1:]
    m = len(x)
    mx = _mean(x)
    my = _mean(y)
    var_x = sum((xi - mx) ** 2 for xi in x)
    if var_x <= 0:
        return {"ok": False, "error": "Variancia zero na serie.", "n": n}

    cov = sum((xi - mx) * (yi - my) for xi, yi in zip(x, y))
    b = cov / var_x
    a = my - b * mx
    resid = [yi - (a + b * xi) for xi, yi in zip(x, y)]
    sse = sum(r * r for r in resid)
    sigma_eps = math.sqrt(sse / max(m - 2, 1))

    mean_reverting = 0.0 < b < 1.0
    if mean_reverting:
        kappa = -math.log(b) / dt_hours
        theta = a / (1.0 - b)
        half_life = math.log(2.0) / kappa if kappa > 0 else None
        sigma_eq = (
            sigma_eps / math.sqrt(1.0 - b * b) if abs(b) < 1 else None
        )
    else:
        kappa = None
        theta = _mean(values)
        half_life = None
        sigma_eq = sigma_eps

    x_last = values[-1]
    z = None
    if sigma_eq and sigma_eq > 0:
        z = (x_last - theta) / sigma_eq

    # R^2
    sst = sum((yi - my) ** 2 for yi in y)
    r2 = 1.0 - (sse / sst) if sst > 0 else 0.0

    return {
        "ok": True,
        "n": n,
        "n_pairs": m,
        "dt_hours": dt_hours,
        "a": a,
        "phi": b,
        "theta": theta,
        "kappa": kappa,
        "half_life_hours": half_life,
        "sigma_eps": sigma_eps,
        "sigma_eq": sigma_eq,
        "x_last": x_last,
        "z": z,
        "r2": r2,
        "mean_reverting": mean_reverting,
    }


def _ou_conditional(
    x0: float,
    theta: float,
    kappa: float,
    sigma_eps: float,
    phi: float,
    tau_hours: float,
    dt_hours: float = 1.0,
) -> tuple[float, float]:
    """Media e desvio condicional de X_{t+tau} sob OU / AR(1)."""
    steps = tau_hours / dt_hours
    # phi^steps = exp(-kappa * tau)
    if kappa is not None and kappa > 0:
        decay = math.exp(-kappa * tau_hours)
    else:
        decay = phi**steps if phi > 0 else 0.0
    mu = theta + (x0 - theta) * decay
    # Var AR(1) n-passos: sigma_eps^2 * (1 - phi^{2n}) / (1 - phi^2)
    if abs(phi) < 1.0 and abs(phi) > 1e-12:
        var = (sigma_eps**2) * (1.0 - phi ** (2.0 * steps)) / (1.0 - phi * phi)
    else:
        var = (sigma_eps**2) * steps
    return mu, math.sqrt(max(var, 0.0))


def prob_reversion_move(
    *,
    x0: float,
    theta: float,
    kappa: float | None,
    sigma_eps: float,
    phi: float,
    tau_hours: float,
    dt_hours: float = 1.0,
) -> dict[str, Any]:
    """P(o spread se mover na direcao da media em tau horas).

    Se x0 > theta: P(X_{t+tau} < x0)
    Se x0 < theta: P(X_{t+tau} > x0)
    Se x0 ~= theta: ~0.5
    """
    if kappa is None or kappa <= 0 or not (0.0 < phi < 1.0):
        return {
            "p_reversion": 0.5,
            "side_spread": "flat",
            "otc_bias": "none",
            "mu": x0,
            "sigma_tau": sigma_eps,
            "usable": False,
        }

    mu, s = _ou_conditional(
        x0, theta, kappa, sigma_eps, phi, tau_hours, dt_hours
    )
    eps = abs(theta) * 1e-9 + 1e-12
    if abs(x0 - theta) < eps or s <= 0:
        p = 0.5
        side = "flat"
        bias = "none"
    elif x0 > theta:
        # espera queda do spread
        p = _norm_cdf((x0 - mu) / s)
        side = "down"
        bias = "put"  # OTC "rico" vs mercado → vies de queda do OTC no spread
    else:
        p = _norm_cdf((mu - x0) / s)
        side = "up"
        bias = "call"

    p = min(max(p, 0.0), 1.0)
    min_payout = (1.0 / p - 1.0) if p > 0 else None
    return {
        "p_reversion": p,
        "side_spread": side,
        "otc_bias": bias,
        "mu": mu,
        "sigma_tau": s,
        "min_payout": min_payout,
        "usable": True,
    }


def analyze_spread_ou(
    points: list[dict[str, Any]],
    *,
    paired_only: bool = True,
    min_n: int = 48,
    horizons: tuple[int, ...] = HORIZONS_H,
) -> dict[str, Any]:
    """Analise completa para o painel / API."""
    values = extract_spread_values(points, paired_only=paired_only)
    est = estimate_ar1(values, dt_hours=1.0, min_n=min_n)
    base = {
        "filter": "paired" if paired_only else "all",
        "note": (
            "Sinal no spread (OTC - EURUSD). otc_bias call/put e vies "
            "sugerido na OTC assumindo que a reversao do spread domina "
            "no horizonte; validar com paper trading."
        ),
    }
    if not est.get("ok"):
        return {**base, **est, "horizons": {}}

    horizons_out: dict[str, Any] = {}
    for h in horizons:
        key = f"{h}h"
        pr = prob_reversion_move(
            x0=float(est["x_last"]),
            theta=float(est["theta"]),
            kappa=est.get("kappa"),
            sigma_eps=float(est["sigma_eps"]),
            phi=float(est["phi"]),
            tau_hours=float(h),
            dt_hours=1.0,
        )
        horizons_out[key] = {
            "hours": h,
            **pr,
        }

    # Melhor horizonte (maior p entre usaveis)
    best = None
    for key, h in horizons_out.items():
        if not h.get("usable"):
            continue
        if best is None or h["p_reversion"] > best["p_reversion"]:
            best = {"horizon": key, **h}

    return {
        **base,
        **est,
        "horizons": horizons_out,
        "best": best,
    }
