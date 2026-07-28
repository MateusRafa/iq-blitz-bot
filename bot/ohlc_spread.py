"""Spread OTC vs EURUSD (1h): serie para o grafico /ohlc-spread.

spread = close_otc - close_eurusd
Quando EURUSD esta fechado (noite / fds): carrega o ultimo close EURUSD
e compara com cada vela OTC (modo carry).
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
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
    pocket_offset: int | None = None,
) -> list[dict[str, Any]]:
    """Marca cada abertura diaria do EURUSD (1a vela do dia civil Pocket)."""
    _ = gap_hours  # legado
    off = (
        int(_env_float("POCKET_TIME_OFFSET", -10800))
        if pocket_offset is None
        else int(pocket_offset)
    )
    by_day: dict[str, datetime] = {}
    for r in eurusd_rows:
        ts = _parse_ts(r.get("opened_at"))
        if ts is None:
            continue
        ts = ts.replace(minute=0, second=0, microsecond=0)
        day = (ts + timedelta(seconds=off)).date().isoformat()
        prev = by_day.get(day)
        if prev is None or ts < prev:
            by_day[day] = ts
    return [
        {"time": int(ts.timestamp()), "opened_at": ts.isoformat(), "day": day}
        for day, ts in sorted(by_day.items(), key=lambda kv: kv[1])
    ]


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
