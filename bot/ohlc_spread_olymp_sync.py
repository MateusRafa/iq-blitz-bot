"""Sync do /ohlc-spread-olymp: OTC via Olymptrade + EURUSD via Dukascopy."""

from __future__ import annotations

import os
from typing import Any

from bot.ohlc_collector_eurusd import collector_eurusd
from bot.ohlc_collector_olymp import collector_olymp
from bot.ohlc_store import TABLE, TABLE_EURUSD
from bot.olymptrade_fetch import default_store_asset
from bot.runner import normalize_asset


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
        os.environ.get("OHLC_OLYMP_OTC_ASSET", "").strip()
        or collector_olymp.status().get("asset")
        or default_store_asset()
    )


def _eurusd_asset() -> str:
    return normalize_asset(
        os.environ.get("OHLC_EURUSD_ASSET", "").strip()
        or collector_eurusd.status().get("asset")
        or "EURUSD"
    )


def sync_spread_olymp_sources(
    *,
    days: int | None = None,
    pull_otc: bool = True,
    pull_dukascopy: bool = True,
) -> dict[str, Any]:
    """Puxa EURUSD (Dukascopy) e EURUSD_otc_olymp (Olymp) → Supabase."""
    otc_a = _otc_asset()
    eu_a = _eurusd_asset()
    lookback = max(1, min(int(days or _env_int("OHLC_SPREAD_OLYMP_SYNC_DAYS", 14)), 90))

    result: dict[str, Any] = {
        "otc_asset": otc_a,
        "eurusd_asset": eu_a,
        "otc": {"ok": False, "upserted": 0, "error": None, "source": "olymptrade"},
        "eurusd": {
            "ok": False,
            "upserted": 0,
            "error": None,
            "source": "dukascopy",
        },
        "days": lookback,
    }

    if pull_dukascopy:
        try:
            if collector_eurusd.status().get("asset") != eu_a:
                try:
                    collector_eurusd.set_asset(eu_a)
                except RuntimeError:
                    pass
            pull = collector_eurusd.pull_now(days=lookback)
            result["eurusd"] = {
                "ok": True,
                "upserted": int((pull.get("pull") or {}).get("upserted") or 0),
                "error": None,
                "asset": eu_a,
                "source": "dukascopy",
                "days": lookback,
            }
        except Exception as exc:  # noqa: BLE001
            result["eurusd"] = {
                "ok": False,
                "upserted": 0,
                "error": str(exc)[:400],
                "asset": eu_a,
                "source": "dukascopy",
            }

    if pull_otc:
        try:
            if collector_olymp.status().get("asset") != otc_a:
                try:
                    collector_olymp.set_asset(otc_a)
                except RuntimeError:
                    pass
            pull = collector_olymp.pull_now(otc_a, hours=lookback * 24)
            result["otc"] = {
                "ok": True,
                "upserted": int((pull.get("pull") or {}).get("upserted") or 0),
                "error": None,
                "asset": otc_a,
                "pair": collector_olymp.status().get("pair"),
                "source": "olymptrade",
            }
        except Exception as exc:  # noqa: BLE001
            result["otc"] = {
                "ok": False,
                "upserted": 0,
                "error": str(exc)[:300],
                "asset": otc_a,
                "source": "olymptrade",
            }

    if pull_dukascopy:
        result["ok"] = bool(result["eurusd"]["ok"])
    else:
        result["ok"] = bool(result["otc"]["ok"])
    result["table_otc"] = TABLE
    result["table_eurusd"] = TABLE_EURUSD
    return result
