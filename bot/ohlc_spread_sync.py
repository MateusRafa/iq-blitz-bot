"""Sync do /ohlc-spread: OTC via Pocket + EURUSD via Dukascopy."""

from __future__ import annotations

import os
from typing import Any

from bot.ohlc_collector import collector
from bot.ohlc_collector_eurusd import collector_eurusd
from bot.ohlc_store import TABLE, TABLE_EURUSD
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
        os.environ.get("OHLC_EURUSD_ASSET", "").strip()
        or collector_eurusd.status().get("asset")
        or "EURUSD"
    )


def sync_spread_sources(
    *,
    days: int | None = None,
    pull_otc: bool = True,
    pull_dukascopy: bool = True,
) -> dict[str, Any]:
    """Puxa EURUSD_otc (Pocket) e EURUSD (Dukascopy) → Supabase.

    days: janela Dukascopy (default OHLC_SPREAD_SYNC_DAYS=14). Sempre
    rebaixa esses dias (sobrescreve residual Pocket na mesma janela).
    """
    otc_a = _otc_asset()
    eu_a = _eurusd_asset()
    lookback = max(1, min(int(days or _env_int("OHLC_SPREAD_SYNC_DAYS", 14)), 90))

    result: dict[str, Any] = {
        "otc_asset": otc_a,
        "eurusd_asset": eu_a,
        "otc": {"ok": False, "upserted": 0, "error": None},
        "eurusd": {
            "ok": False,
            "upserted": 0,
            "error": None,
            "source": "dukascopy",
        },
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
                "source": "pocket",
            }
        except Exception as exc:  # noqa: BLE001
            result["otc"] = {
                "ok": False,
                "upserted": 0,
                "error": str(exc)[:300],
                "asset": otc_a,
                "source": "pocket",
            }

    if pull_dukascopy:
        try:
            # Garante asset no coletor Dukascopy.
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
                "error": str(exc)[:300],
                "asset": eu_a,
                "source": "dukascopy",
            }

    # Dukascopy e o requisito principal do grafico EURUSD.
    if pull_dukascopy:
        result["ok"] = bool(result["eurusd"]["ok"])
    else:
        result["ok"] = bool(result["otc"]["ok"])
    result["table_otc"] = TABLE
    result["table_eurusd"] = TABLE_EURUSD
    return result
