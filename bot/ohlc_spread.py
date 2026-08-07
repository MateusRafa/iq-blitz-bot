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


def _key_day(raw: Any, *, pocket_offset: int | None = None) -> str | None:
    """Dia civil Pocket (UTC−3 por padrao) — alinhado as velas D1."""
    ts = _parse_ts(raw)
    if ts is None:
        return None
    off = (
        int(_env_float("POCKET_TIME_OFFSET", -10800))
        if pocket_offset is None
        else int(pocket_offset)
    )
    return (ts + timedelta(seconds=off)).date().isoformat()


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
      weekend (bool - legado; True em sab/dom ou carry),
      after_hours (bool - True quando mode=carry)
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

        weekday = ts.weekday()  # 0=seg ... 5=sab 6=dom
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


def _row_px(row: dict[str, Any], field: str) -> float | None:
    try:
        return float(row[field])
    except (TypeError, ValueError, KeyError):
        return None


def build_spread_1d(
    otc_rows: list[dict[str, Any]],
    eurusd_rows: list[dict[str, Any]],
    *,
    pocket_offset: int | None = None,
) -> list[dict[str, Any]]:
    """Spread diario: alinhado ao dia civil das velas OTC D1.

    Mesmo contrato de build_spread_1h (time, spread, mode paired|carry, weekend).
    Fins de semana OTC mantem carry do ultimo close/open EURUSD de sessao.

    ``spread`` = close_otc − close_eurusd (padrao).
    ``spread_open`` = open_otc − open_eurusd (quando ambos existem).
    """
    off = (
        int(_env_float("POCKET_TIME_OFFSET", -10800))
        if pocket_offset is None
        else int(pocket_offset)
    )
    by_eu: dict[str, dict[str, Any]] = {}
    for r in eurusd_rows:
        k = _key_day(r.get("opened_at"), pocket_offset=off)
        if k:
            by_eu[k] = r

    points: list[dict[str, Any]] = []
    last_eu_close: float | None = None
    last_eu_open: float | None = None

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
        key = _key_day(row.get("opened_at"), pocket_offset=off)
        if not key:
            continue
        otc_c = _row_px(row, "close")
        if otc_c is None:
            continue
        otc_o = _row_px(row, "open")

        while eu_i < len(eu_sorted):
            eu_ts = _parse_ts(eu_sorted[eu_i].get("opened_at"))
            if eu_ts is None or eu_ts > ts:
                break
            c = _row_px(eu_sorted[eu_i], "close")
            o = _row_px(eu_sorted[eu_i], "open")
            if c is not None:
                last_eu_close = c
            if o is not None:
                last_eu_open = o
            eu_i += 1

        # Dia civil Pocket do ponto OTC.
        try:
            y, mo, d = (int(x) for x in key.split("-"))
            weekday = datetime(y, mo, d, tzinfo=timezone.utc).weekday()
        except ValueError:
            weekday = ts.weekday()
        is_weekend = weekday >= 5
        paired = key in by_eu

        if paired:
            eu_c = _row_px(by_eu[key], "close")
            if eu_c is None:
                eu_c = last_eu_close
            eu_o = _row_px(by_eu[key], "open")
            if eu_o is None:
                eu_o = last_eu_open
            mode = "paired"
        else:
            eu_c = last_eu_close
            eu_o = last_eu_open
            mode = "carry"

        if eu_c is None:
            continue

        if paired:
            last_eu_close = eu_c
            if eu_o is not None:
                last_eu_open = eu_o

        after_hours = mode == "carry"
        point: dict[str, Any] = {
            "time": int(ts.timestamp()),
            "opened_at": ts.isoformat(),
            "spread": otc_c - eu_c,
            "otc_close": otc_c,
            "eurusd_close": eu_c,
            "mode": mode,
            "after_hours": after_hours,
            "weekend": is_weekend or after_hours,
            "day": key,
        }
        if otc_o is not None:
            point["otc_open"] = otc_o
        if eu_o is not None:
            point["eurusd_open"] = eu_o
        if otc_o is not None and eu_o is not None:
            point["spread_open"] = otc_o - eu_o
        points.append(point)
    return points


def apply_spread_price_field(
    points: list[dict[str, Any]],
    price: str = "close",
) -> list[dict[str, Any]]:
    """Devolve pontos com ``spread`` no campo pedido (close|open)."""
    field = (price or "close").strip().lower()
    if field not in ("close", "open"):
        field = "close"
    if field == "close":
        return points
    out: list[dict[str, Any]] = []
    for p in points:
        so = p.get("spread_open")
        if so is None:
            continue
        q = dict(p)
        q["spread"] = float(so)
        out.append(q)
    return out


def pad_eurusd_tail_to_otc(
    otc_rows: list[dict[str, Any]],
    eurusd_rows: list[dict[str, Any]],
    *,
    max_hours: int = 6,
) -> list[dict[str, Any]]:
    """Completa EURUSD no final com carry para nao ficar 1+ velas atras da OTC.

    Usado na serie do grafico: a Pocket ja tem a vela da hora corrente;
    o bi5 Dukascopy pode atrasar. Nao preenche buracos longos (fds).
    """
    if not otc_rows or not eurusd_rows:
        return eurusd_rows

    otc_times: list[datetime] = []
    otc_keys: set[str] = set()
    for r in otc_rows:
        ts = _parse_ts(r.get("opened_at"))
        if ts is None:
            continue
        ts = ts.replace(minute=0, second=0, microsecond=0)
        otc_times.append(ts)
        otc_keys.add(ts.strftime("%Y-%m-%dT%H:%M:%S"))
    if not otc_times:
        return eurusd_rows

    eu_by: dict[str, dict[str, Any]] = {}
    last_eu_ts: datetime | None = None
    last_close: float | None = None
    for r in eurusd_rows:
        ts = _parse_ts(r.get("opened_at"))
        if ts is None:
            continue
        ts = ts.replace(minute=0, second=0, microsecond=0)
        key = ts.strftime("%Y-%m-%dT%H:%M:%S")
        eu_by[key] = r
        try:
            c = float(r["close"])
        except (TypeError, ValueError, KeyError):
            continue
        if last_eu_ts is None or ts >= last_eu_ts:
            last_eu_ts = ts
            last_close = c

    if last_eu_ts is None or last_close is None:
        return eurusd_rows

    otc_last = max(otc_times)
    if otc_last <= last_eu_ts:
        return eurusd_rows

    gap_h = int((otc_last - last_eu_ts).total_seconds() // 3600)
    if gap_h <= 0 or gap_h > max_hours:
        return eurusd_rows

    asset = str(eurusd_rows[-1].get("asset") or "EURUSD")
    now_iso = datetime.now(timezone.utc).isoformat()
    extra: list[dict[str, Any]] = []
    t = last_eu_ts + timedelta(hours=1)
    while t <= otc_last and len(extra) < max_hours:
        key = t.strftime("%Y-%m-%dT%H:%M:%S")
        if key in otc_keys and key not in eu_by:
            extra.append(
                {
                    "asset": asset,
                    "timeframe": "1h",
                    "opened_at": t.isoformat(),
                    "open": last_close,
                    "high": last_close,
                    "low": last_close,
                    "close": last_close,
                    "volume": 0,
                    "source": "dukascopy",
                    "updated_at": now_iso,
                }
            )
        t += timedelta(hours=1)

    if not extra:
        return eurusd_rows
    return list(eurusd_rows) + extra
