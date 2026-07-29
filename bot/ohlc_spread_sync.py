"""Sync do /ohlc-spread: OTC via Pocket + EURUSD via Dukascopy."""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import Any

from bot.dukascopy_fetch import fetch_eurusd_1h_rows_for_store
from bot.ohlc_collector import collector
from bot.ohlc_store import (
    TABLE,
    TABLE_EURUSD,
    last_opened_at,
    upsert_candles,
)
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
        os.environ.get("OHLC_SPREAD_OTC_ASSET", "").strip()
        or collector.status().get("asset")
        or "EURUSD_otc"
    )


def _eurusd_asset() -> str:
    return normalize_asset(
        os.environ.get("OHLC_EURUSD_ASSET", "").strip() or "EURUSD"
    )


def sync_spread_sources(
    *,
    days: int | None = None,
    pull_otc: bool = True,
    pull_dukascopy: bool = True,
) -> dict[str, Any]:
    """Puxa EURUSD_otc (Pocket) e EURUSD (Dukascopy) → Supabase.

    days: quantos dias de historico Dukascopy pedir (default env
    OHLC_SPREAD_SYNC_DAYS ou 14). Se ja houver velas, faz incremental
    a partir do ultimo opened_at (com 2h de overlap).
    """
    otc_a = _otc_asset()
    eu_a = _eurusd_asset()
    lookback = max(1, min(int(days or _env_int("OHLC_SPREAD_SYNC_DAYS", 14)), 90))

    result: dict[str, Any] = {
        "otc_asset": otc_a,
        "eurusd_asset": eu_a,
        "otc": {"ok": False, "upserted": 0, "error": None},
        "eurusd": {"ok": False, "upserted": 0, "error": None, "source": "dukascopy"},
        "days": lookback,
    }

    if pull_otc:
        try:
            pull = collector.pull_now(otc_a)
            result["otc"] = {
                "ok": True,
                "upserted": int((pull.get("pull") or {}).get("upserted") or 0),
                "error": None,
                "asset": otc_a,
            }
        except Exception as exc:  # noqa: BLE001
            result["otc"] = {
                "ok": False,
                "upserted": 0,
                "error": str(exc)[:300],
                "asset": otc_a,
            }

    if pull_dukascopy:
        try:
            end = datetime.now(timezone.utc)
            start = end - timedelta(days=lookback)
            try:
                last = last_opened_at(eu_a, "1h", table=TABLE_EURUSD)
            except Exception:  # noqa: BLE001
                last = None
            if last is not None:
                # Overlap de 2h para regravar a ultima vela incompleta.
                candidate = last - timedelta(hours=2)
                if candidate > start:
                    start = candidate
            rows = fetch_eurusd_1h_rows_for_store(start, end, asset=eu_a)
            n = upsert_candles(rows, table=TABLE_EURUSD) if rows else 0
            result["eurusd"] = {
                "ok": True,
                "upserted": n,
                "fetched": len(rows),
                "error": None,
                "asset": eu_a,
                "source": "dukascopy",
                "from": start.isoformat(),
                "to": end.isoformat(),
            }
        except Exception as exc:  # noqa: BLE001
            result["eurusd"] = {
                "ok": False,
                "upserted": 0,
                "error": str(exc)[:300],
                "asset": eu_a,
                "source": "dukascopy",
            }

    result["ok"] = bool(result["otc"]["ok"] or result["eurusd"]["ok"])
    result["table_otc"] = TABLE
    result["table_eurusd"] = TABLE_EURUSD
    return result
