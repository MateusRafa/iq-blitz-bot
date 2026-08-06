"""Sync do /ohlc-spread-1d: OTC D1 Pocket + EURUSD D1 (agregado Dukascopy 1h)."""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import Any

from bot.ohlc_collector_1d import (
    TABLE_1D,
    TIMEFRAME,
    aggregate_hourly_to_daily,
    collector_1d,
    pocket_tz_offset,
)
from bot.ohlc_collector_eurusd import TABLE_EURUSD, collector_eurusd
from bot.ohlc_store import fetch_candles_range, stored_summary, upsert_candles
from bot.ohlc_spread import _key_day
from bot.runner import normalize_asset

# Compat: deploy parcial pode ter sync novo + ohlc_store antigo.
try:
    from bot.ohlc_store import TABLE_EURUSD_1D
except ImportError:  # pragma: no cover
    TABLE_EURUSD_1D = "ohlc_candles_eurusd_1d"

SOURCE_EU_1D = "dukascopy_agg"


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _otc_asset() -> str:
    return normalize_asset(
        os.environ.get("OHLC_SPREAD_1D_OTC_ASSET", "").strip()
        or os.environ.get("OHLC_SPREAD_OTC_ASSET", "").strip()
        or os.environ.get("OHLC_1D_ASSET", "").strip()
        or collector_1d.status().get("asset")
        or "EURUSD_otc"
    )


def _eurusd_asset() -> str:
    return normalize_asset(
        os.environ.get("OHLC_EURUSD_ASSET", "").strip()
        or collector_eurusd.status().get("asset")
        or "EURUSD"
    )


def rebuild_eurusd_1d_from_hourly(
    *,
    eurusd_asset: str | None = None,
    days: int | None = None,
    include_today: bool = True,
) -> dict[str, Any]:
    """Agrega ohlc_candles_eurusd (1h) → ohlc_candles_eurusd_1d."""
    eu_a = normalize_asset(eurusd_asset or _eurusd_asset())
    lookback = max(1, min(int(days or _env_int("OHLC_SPREAD_1D_SYNC_DAYS", 120)), 800))
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=lookback)

    hourly = fetch_candles_range(
        eu_a, timeframe="1h", table=TABLE_EURUSD, after=start
    )
    daily = aggregate_hourly_to_daily(
        hourly,
        asset=eu_a,
        pocket_offset=pocket_tz_offset(),
        include_today=include_today,
    )
    for row in daily:
        row["source"] = SOURCE_EU_1D
        row["timeframe"] = TIMEFRAME

    upserted = upsert_candles(daily, table=TABLE_EURUSD_1D) if daily else 0
    return {
        "ok": True,
        "asset": eu_a,
        "source": SOURCE_EU_1D,
        "table": TABLE_EURUSD_1D,
        "hourly_in": len(hourly),
        "daily_built": len(daily),
        "upserted": upserted,
        "days": lookback,
        "from": start.isoformat(),
        "to": end.isoformat(),
    }


def reconcile_eurusd_1d_to_otc(
    *,
    otc_asset: str | None = None,
    eurusd_asset: str | None = None,
    days: int | None = None,
) -> dict[str, Any]:
    """Pente fino D1: dias OTC de sessao sem EURUSD D1 → re-agrega 1h."""
    otc_a = normalize_asset(otc_asset or _otc_asset())
    eu_a = normalize_asset(eurusd_asset or _eurusd_asset())
    lookback = max(1, min(int(days or _env_int("OHLC_SPREAD_1D_SYNC_DAYS", 120)), 800))
    end = datetime.now(timezone.utc)
    start = (end - timedelta(days=lookback)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    off = pocket_tz_offset()

    otc_win = fetch_candles_range(
        otc_a, timeframe=TIMEFRAME, table=TABLE_1D, after=start
    )
    eu_win = fetch_candles_range(
        eu_a, timeframe=TIMEFRAME, table=TABLE_EURUSD_1D, after=start
    )
    eu_days = {
        k
        for r in eu_win
        if (k := _key_day(r.get("opened_at"), pocket_offset=off))
    }

    gaps: list[str] = []
    weekend_kept = 0
    session_days = 0
    for r in otc_win:
        day = _key_day(r.get("opened_at"), pocket_offset=off)
        if not day:
            continue
        try:
            y, mo, d = (int(x) for x in day.split("-"))
            wd = datetime(y, mo, d, tzinfo=timezone.utc).weekday()
        except ValueError:
            continue
        if wd >= 5:
            weekend_kept += 1
            continue
        session_days += 1
        if day not in eu_days:
            gaps.append(day)

    rebuilt = rebuild_eurusd_1d_from_hourly(
        eurusd_asset=eu_a, days=lookback, include_today=True
    )

    eu_after = fetch_candles_range(
        eu_a, timeframe=TIMEFRAME, table=TABLE_EURUSD_1D, after=start
    )
    eu_days_after = {
        k
        for r in eu_after
        if (k := _key_day(r.get("opened_at"), pocket_offset=off))
    }
    still = [d for d in gaps if d not in eu_days_after]

    return {
        "ok": True,
        "otc_asset": otc_a,
        "eurusd_asset": eu_a,
        "days": lookback,
        "otc_in_window": len(otc_win),
        "otc_session_days": session_days,
        "otc_weekend_kept": weekend_kept,
        "gaps_found": len(gaps),
        "gaps_remaining": len(still),
        "rebuild": rebuilt,
        "upserted": int(rebuilt.get("upserted") or 0),
    }


def sync_spread_1d_sources(
    *,
    days: int | None = None,
    pull_otc: bool = True,
    pull_dukascopy: bool = True,
) -> dict[str, Any]:
    """Pente fino D1: OTC tip Pocket + EURUSD D1 a partir do 1h Dukascopy."""
    otc_a = _otc_asset()
    eu_a = _eurusd_asset()
    lookback = max(1, min(int(days or _env_int("OHLC_SPREAD_1D_SYNC_DAYS", 120)), 800))

    result: dict[str, Any] = {
        "otc_asset": otc_a,
        "eurusd_asset": eu_a,
        "otc": {"ok": False, "upserted": 0, "error": None, "source": "pocket"},
        "eurusd": {
            "ok": False,
            "upserted": 0,
            "error": None,
            "source": SOURCE_EU_1D,
        },
        "days": lookback,
        "mode": "fine_comb_1d",
        "timeframe": TIMEFRAME,
        "table_otc": TABLE_1D,
        "table_eurusd": TABLE_EURUSD_1D,
    }

    if pull_otc:
        try:
            if collector_1d.status().get("asset") != otc_a:
                try:
                    collector_1d.set_asset(otc_a)
                except RuntimeError:
                    pass
            pull = collector_1d.pull_now()
            result["otc"] = {
                "ok": True,
                "upserted": int((pull.get("pull") or {}).get("upserted") or 0),
                "error": None,
                "asset": otc_a,
                "source": "pocket",
            }
        except Exception as exc:  # noqa: BLE001
            result["otc"] = {
                "ok": False,
                "upserted": 0,
                "error": str(exc)[:400],
                "asset": otc_a,
                "source": "pocket",
            }

    if pull_dukascopy:
        try:
            tip_h = min(14, max(3, lookback // 8))
            try:
                if collector_eurusd.status().get("asset") != eu_a:
                    try:
                        collector_eurusd.set_asset(eu_a)
                    except RuntimeError:
                        pass
                collector_eurusd.pull_now(days=tip_h)
            except Exception:  # noqa: BLE001
                pass

            recon = reconcile_eurusd_1d_to_otc(
                otc_asset=otc_a, eurusd_asset=eu_a, days=lookback
            )
            result["eurusd"] = {
                "ok": bool(recon.get("ok")),
                "upserted": int(recon.get("upserted") or 0),
                "error": None,
                "asset": eu_a,
                "source": SOURCE_EU_1D,
                "days": lookback,
                "reconcile": {
                    "gaps_found": recon.get("gaps_found"),
                    "gaps_remaining": recon.get("gaps_remaining"),
                    "otc_session_days": recon.get("otc_session_days"),
                    "otc_weekend_kept": recon.get("otc_weekend_kept"),
                    "otc_in_window": recon.get("otc_in_window"),
                },
            }
            result["reconcile"] = recon
        except Exception as exc:  # noqa: BLE001
            result["eurusd"] = {
                "ok": False,
                "upserted": 0,
                "error": str(exc)[:400],
                "asset": eu_a,
                "source": SOURCE_EU_1D,
            }

    if pull_dukascopy:
        result["ok"] = bool(result["eurusd"]["ok"])
    else:
        result["ok"] = bool(result["otc"]["ok"])
    result["otc_summary"] = stored_summary(otc_a, TIMEFRAME, table=TABLE_1D)
    result["eurusd_summary"] = stored_summary(
        eu_a, TIMEFRAME, table=TABLE_EURUSD_1D
    )
    return result


def backfill_dukascopy_1d(
    *,
    days: int | None = None,
    match_otc: bool = True,
    otc_asset: str | None = None,
) -> dict[str, Any]:
    """Puxa Dukascopy 1h (casado ao OTC D1) e reconstroi EURUSD D1."""
    otc_a = normalize_asset(otc_asset or _otc_asset())
    eu_a = _eurusd_asset()
    lookback = max(1, min(int(days or 120), 800))

    pull = collector_eurusd.pull_history(
        days=lookback,
        match_otc=match_otc,
        otc_asset=otc_a,
        otc_table=TABLE_1D,
        otc_timeframe=TIMEFRAME,
    )
    span = lookback
    if isinstance(pull, dict):
        info = pull.get("pull") or pull
        if info.get("from") and info.get("to"):
            try:
                span = lookback
            except Exception:  # noqa: BLE001
                pass
        if info.get("days"):
            try:
                span = max(span, int(info["days"]))
            except (TypeError, ValueError):
                pass

    rebuilt = rebuild_eurusd_1d_from_hourly(
        eurusd_asset=eu_a, days=max(span, lookback), include_today=True
    )
    hourly_info = pull.get("pull") if isinstance(pull, dict) else None
    return {
        "ok": True,
        "otc_asset": otc_a,
        "eurusd_asset": eu_a,
        "days": lookback,
        "upserted": int(rebuilt.get("upserted") or 0),
        "fetched": int(rebuilt.get("daily_built") or 0),
        "hourly": hourly_info,
        "daily": rebuilt,
        "oldest": (hourly_info or {}).get("oldest")
        or (hourly_info or {}).get("from"),
        "newest": (hourly_info or {}).get("newest")
        or (hourly_info or {}).get("to"),
    }
