"""Sync do /ohlc-spread-expert-1d: OTC Expert D1 + EURUSD D1 (Dukascopy agg)."""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import Any

from bot.ohlc_collector_1d import TIMEFRAME, aggregate_hourly_to_daily, pocket_tz_offset
from bot.ohlc_collector_eurusd import collector_eurusd
from bot.ohlc_collector_expert import TABLE as TABLE_EXPERT, collector_expert
from bot.ohlc_spread import _key_day
from bot.ohlc_spread_1d_sync import (
    SOURCE_EU_1D,
    backfill_dukascopy_1d,
    rebuild_eurusd_1d_from_hourly,
)
from bot.ohlc_store import (
    fetch_candles_range,
    oldest_opened_at,
    stored_summary,
    upsert_candles,
)
from bot.expertoption_fetch import default_store_asset
from bot.runner import normalize_asset

try:
    from bot.ohlc_store import TABLE_EURUSD_1D, TABLE_EXPERT_1D
except ImportError:  # pragma: no cover
    TABLE_EURUSD_1D = "ohlc_candles_eurusd_1d"
    TABLE_EXPERT_1D = "ohlc_candles_expert_1d"

SOURCE_OTC_1D = "expertoption_agg"


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
        os.environ.get("OHLC_SPREAD_EXPERT_1D_OTC_ASSET", "").strip()
        or os.environ.get("OHLC_EXPERT_OTC_ASSET", "").strip()
        or collector_expert.status().get("asset")
        or default_store_asset()
    )


def _eurusd_asset() -> str:
    return normalize_asset(
        os.environ.get("OHLC_EURUSD_ASSET", "").strip()
        or collector_eurusd.status().get("asset")
        or "EURUSD"
    )


def rebuild_expert_1d_from_hourly(
    *,
    otc_asset: str | None = None,
    days: int | None = None,
    include_today: bool = True,
) -> dict[str, Any]:
    """Agrega ohlc_candles_expert (1h) → ohlc_candles_expert_1d."""
    otc_a = normalize_asset(otc_asset or _otc_asset())
    lookback = max(
        1, min(int(days or _env_int("OHLC_SPREAD_EXPERT_1D_SYNC_DAYS", 120)), 800)
    )
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=lookback)

    hourly = fetch_candles_range(
        otc_a, timeframe="1h", table=TABLE_EXPERT, after=start
    )
    daily = aggregate_hourly_to_daily(
        hourly,
        asset=otc_a,
        pocket_offset=pocket_tz_offset(),
        include_today=include_today,
    )
    for row in daily:
        row["source"] = SOURCE_OTC_1D
        row["timeframe"] = TIMEFRAME

    upserted = upsert_candles(daily, table=TABLE_EXPERT_1D) if daily else 0
    return {
        "ok": True,
        "asset": otc_a,
        "source": SOURCE_OTC_1D,
        "table": TABLE_EXPERT_1D,
        "hourly_in": len(hourly),
        "daily_built": len(daily),
        "upserted": upserted,
        "days": lookback,
        "from": start.isoformat(),
        "to": end.isoformat(),
    }


def reconcile_eurusd_to_expert_1d(
    *,
    otc_asset: str | None = None,
    eurusd_asset: str | None = None,
    days: int | None = None,
) -> dict[str, Any]:
    """Pente fino: dias Expert D1 de sessao sem EURUSD D1 → re-agrega 1h."""
    otc_a = normalize_asset(otc_asset or _otc_asset())
    eu_a = normalize_asset(eurusd_asset or _eurusd_asset())
    lookback = max(
        1, min(int(days or _env_int("OHLC_SPREAD_EXPERT_1D_SYNC_DAYS", 120)), 800)
    )
    end = datetime.now(timezone.utc)
    start = (end - timedelta(days=lookback)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    off = pocket_tz_offset()

    otc_win = fetch_candles_range(
        otc_a, timeframe=TIMEFRAME, table=TABLE_EXPERT_1D, after=start
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
        "otc_table": TABLE_EXPERT_1D,
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


def sync_spread_expert_1d_sources(
    *,
    days: int | None = None,
    pull_otc: bool = True,
    pull_dukascopy: bool = True,
) -> dict[str, Any]:
    """Pente fino D1 Expert: tip 1h Expert → agg D1 + EURUSD D1 Dukascopy."""
    otc_a = _otc_asset()
    eu_a = _eurusd_asset()
    lookback = max(
        1, min(int(days or _env_int("OHLC_SPREAD_EXPERT_1D_SYNC_DAYS", 120)), 800)
    )

    result: dict[str, Any] = {
        "otc_asset": otc_a,
        "eurusd_asset": eu_a,
        "otc": {"ok": False, "upserted": 0, "error": None, "source": "expertoption"},
        "eurusd": {
            "ok": False,
            "upserted": 0,
            "error": None,
            "source": SOURCE_EU_1D,
        },
        "days": lookback,
        "mode": "fine_comb_expert_1d",
        "timeframe": TIMEFRAME,
        "table_otc": TABLE_EXPERT_1D,
        "table_eurusd": TABLE_EURUSD_1D,
    }

    if pull_otc:
        try:
            if collector_expert.status().get("asset") != otc_a:
                try:
                    collector_expert.set_asset(otc_a)
                except RuntimeError:
                    pass
            hours = min(lookback * 24, 24 * 60)
            pull = collector_expert.pull_now(otc_a, hours=hours)
            tip_n = int((pull.get("pull") or {}).get("upserted") or 0)
            rebuilt_otc = rebuild_expert_1d_from_hourly(
                otc_asset=otc_a, days=lookback, include_today=True
            )
            result["otc"] = {
                "ok": True,
                "upserted": int(rebuilt_otc.get("upserted") or 0),
                "error": None,
                "asset": otc_a,
                "source": SOURCE_OTC_1D,
                "hourly_tip": tip_n,
                "daily": rebuilt_otc,
            }
        except Exception as exc:  # noqa: BLE001
            try:
                rebuilt_otc = rebuild_expert_1d_from_hourly(
                    otc_asset=otc_a, days=lookback, include_today=True
                )
                result["otc"] = {
                    "ok": int(rebuilt_otc.get("daily_built") or 0) > 0,
                    "upserted": int(rebuilt_otc.get("upserted") or 0),
                    "error": str(exc)[:300],
                    "asset": otc_a,
                    "source": SOURCE_OTC_1D,
                    "daily": rebuilt_otc,
                    "fallback": "agg_only",
                }
            except Exception as exc2:  # noqa: BLE001
                result["otc"] = {
                    "ok": False,
                    "upserted": 0,
                    "error": f"{exc}; {exc2}"[:400],
                    "asset": otc_a,
                    "source": SOURCE_OTC_1D,
                }

    if pull_dukascopy:
        tip_err = None
        try:
            if collector_eurusd.is_pull_busy():
                collector_eurusd.release_pull_lock(force=False)
            if not collector_eurusd.is_pull_busy():
                if collector_eurusd.status().get("asset") != eu_a:
                    try:
                        collector_eurusd.set_asset(eu_a)
                    except RuntimeError:
                        pass
                collector_eurusd.pull_now(days=min(14, max(3, lookback // 8)))
        except Exception as exc:  # noqa: BLE001
            tip_err = str(exc)[:200]

        try:
            recon = reconcile_eurusd_to_expert_1d(
                otc_asset=otc_a, eurusd_asset=eu_a, days=lookback
            )
            result["eurusd"] = {
                "ok": True,
                "upserted": int(recon.get("upserted") or 0),
                "error": tip_err,
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
        result["ok"] = bool(result["eurusd"].get("ok"))
    else:
        result["ok"] = bool(result["otc"].get("ok"))
    result["otc_summary"] = stored_summary(otc_a, TIMEFRAME, table=TABLE_EXPERT_1D)
    result["eurusd_summary"] = stored_summary(
        eu_a, TIMEFRAME, table=TABLE_EURUSD_1D
    )
    return result


def backfill_expert_otc_1d(
    *,
    hours: int | None = None,
    otc_asset: str | None = None,
) -> dict[str, Any]:
    """Puxa historico Expert 1h e agrega D1."""
    otc_a = normalize_asset(otc_asset or _otc_asset())
    hrs = max(24, min(int(hours or 24 * 120), 24 * 800))
    lookback_days = max(1, hrs // 24)

    hourly_info: dict[str, Any] | None = None
    tip_error: str | None = None
    try:
        if collector_expert.status().get("asset") != otc_a:
            try:
                collector_expert.set_asset(otc_a)
            except RuntimeError:
                pass
        pull = collector_expert.pull_now(otc_a, hours=hrs)
        hourly_info = pull.get("pull") if isinstance(pull, dict) else None
    except Exception as exc:  # noqa: BLE001
        tip_error = str(exc)[:300]

    rebuilt = rebuild_expert_1d_from_hourly(
        otc_asset=otc_a, days=lookback_days, include_today=True
    )
    return {
        "ok": int(rebuilt.get("daily_built") or 0) > 0
        or int(rebuilt.get("upserted") or 0) > 0,
        "asset": otc_a,
        "upserted": int(rebuilt.get("upserted") or 0),
        "fetched": int(rebuilt.get("daily_built") or 0),
        "hourly": hourly_info,
        "daily": rebuilt,
        "timeframe": TIMEFRAME,
        "table": TABLE_EXPERT_1D,
        "error": tip_error,
        "note": (
            "OTC Expert D1 e agregado do 1h em ohlc_candles_expert. "
            "Se fetched=0, puxe historico na ferramenta Expert 1h antes."
        ),
    }


def backfill_dukascopy_expert_1d(
    *,
    days: int | None = None,
    match_otc: bool = True,
    otc_asset: str | None = None,
) -> dict[str, Any]:
    """EURUSD D1 casado ao Expert D1."""
    otc_a = normalize_asset(otc_asset or _otc_asset())
    lookback = max(1, min(int(days or 120), 800))

    if match_otc:
        try:
            oldest = oldest_opened_at(otc_a, TIMEFRAME, table=TABLE_EXPERT_1D)
        except Exception:  # noqa: BLE001
            oldest = None
        if oldest is not None:
            span = int(
                (datetime.now(timezone.utc) - oldest).total_seconds() // 86400
            ) + 2
            lookback = max(lookback, min(span, 800))

    rebuild_expert_1d_from_hourly(
        otc_asset=otc_a, days=lookback, include_today=True
    )
    result = backfill_dukascopy_1d(
        days=lookback,
        match_otc=False,
        otc_asset=otc_a,
        force_unlock=True,
    )
    recon = reconcile_eurusd_to_expert_1d(
        otc_asset=otc_a, eurusd_asset=_eurusd_asset(), days=lookback
    )
    result["reconcile"] = recon
    result["upserted"] = int(recon.get("upserted") or result.get("upserted") or 0)
    result["fetched"] = int(
        (recon.get("rebuild") or {}).get("daily_built")
        or result.get("fetched")
        or 0
    )
    result["otc_asset"] = otc_a
    result["otc_table"] = TABLE_EXPERT_1D
    result["ok"] = bool(
        int(result.get("fetched") or 0) > 0 or int(result.get("upserted") or 0) > 0
    )
    return result
