"""Spread OTC vs EURUSD (1h): serie para o grafico /ohlc-spread.

spread = close_otc - close_eurusd
Quando EURUSD esta fechado (noite / fds): carrega o ultimo close EURUSD
e compara com cada vela OTC (modo carry).
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _parse_ts(raw: Any) -> datetime | None:
    if not raw:
        return None
    try:
        ts = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
    except ValueError:
        return None
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)


def _key_hour(raw: Any) -> str | None:
    ts = _parse_ts(raw)
    if ts is None:
        return None
    ts = ts.replace(minute=0, second=0, microsecond=0)
    return ts.strftime("%Y-%m-%dT%H:%M:%S")


def detect_eurusd_opens(
    eurusd_rows: list[dict[str, Any]],
    *,
    gap_hours: float | None = None,
) -> list[dict[str, Any]]:
    """Marca aberturas do EURUSD: 1a vela apos buraco >= gap_hours.

    gap padrao: OHLC_SPREAD_OPEN_GAP_HOURS (default 2h) — cobre fechamento
    diario e fim de semana.
    """
    gap_h = (
        _env_float("OHLC_SPREAD_OPEN_GAP_HOURS", 2.0)
        if gap_hours is None
        else float(gap_hours)
    )
    gap_sec = max(gap_h, 1.0) * 3600.0
    times: list[datetime] = []
    for r in eurusd_rows:
        ts = _parse_ts(r.get("opened_at"))
        if ts is not None:
            times.append(ts.replace(minute=0, second=0, microsecond=0))
    times = sorted(set(times))
    opens: list[dict[str, Any]] = []
    prev: datetime | None = None
    for ts in times:
        if prev is None or (ts - prev).total_seconds() >= gap_sec:
            opens.append(
                {
                    "time": int(ts.timestamp()),
                    "opened_at": ts.isoformat(),
                }
            )
        prev = ts
    return opens


def build_spread_1h(
    otc_rows: list[dict[str, Any]],
    eurusd_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Constroi pontos de spread alinhados ao tempo das velas OTC.

    Cada ponto:
      time (unix s), spread, otc_close, eurusd_close, mode (paired|carry),
      weekend (bool — legado; True em sab/dom ou carry),
      after_hours (bool — True quando mode=carry)
    """
    by_eu: dict[str, dict[str, Any]] = {}
    for r in eurusd_rows:
        k = _key_hour(r.get("opened_at"))
        if k:
            by_eu[k] = r

    points: list[dict[str, Any]] = []
    last_eu_close: float | None = None

    otc_sorted = sorted(
        otc_rows, key=lambda r: str(r.get("opened_at") or "")
    )
    eu_sorted = sorted(
        eurusd_rows, key=lambda r: str(r.get("opened_at") or "")
    )
    eu_i = 0

    for row in otc_sorted:
        ts = _parse_ts(row.get("opened_at"))
        if ts is None:
            continue
        key = _key_hour(row.get("opened_at"))
        if not key:
            continue
        try:
            otc_c = float(row["close"])
        except (TypeError, ValueError, KeyError):
            continue

        while eu_i < len(eu_sorted):
            eu_ts = _parse_ts(eu_sorted[eu_i].get("opened_at"))
            if eu_ts is None or eu_ts > ts:
                break
            try:
                last_eu_close = float(eu_sorted[eu_i]["close"])
            except (TypeError, ValueError, KeyError):
                pass
            eu_i += 1

        weekday = ts.weekday()  # 0=seg … 5=sab 6=dom
        is_weekend = weekday >= 5
        paired = key in by_eu

        if paired:
            try:
                eu_c = float(by_eu[key]["close"])
            except (TypeError, ValueError, KeyError):
                eu_c = last_eu_close
            mode = "paired"
        else:
            eu_c = last_eu_close
            mode = "carry"

        if eu_c is None:
            continue

        if paired:
            last_eu_close = eu_c

        after_hours = mode == "carry"
        points.append(
            {
                "time": int(ts.timestamp()),
                "opened_at": ts.isoformat(),
                "spread": otc_c - eu_c,
                "otc_close": otc_c,
                "eurusd_close": eu_c,
                "mode": mode,
                "after_hours": after_hours,
                "weekend": is_weekend or after_hours,
            }
        )
    return points
