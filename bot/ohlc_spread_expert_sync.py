"""Sync do /ohlc-spread-expert: OTC Expert + EURUSD Dukascopy (pente fino)."""



from __future__ import annotations



import os

from typing import Any



from bot.ohlc_collector_eurusd import collector_eurusd

from bot.ohlc_collector_expert import collector_expert

from bot.ohlc_spread_reconcile import reconcile_eurusd_to_otc

from bot.ohlc_store import TABLE_EURUSD, TABLE_EXPERT

from bot.expertoption_fetch import default_store_asset

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

        os.environ.get("OHLC_EXPERT_OTC_ASSET", "").strip()

        or collector_expert.status().get("asset")

        or default_store_asset()

    )





def _eurusd_asset() -> str:

    return normalize_asset(

        os.environ.get("OHLC_EURUSD_ASSET", "").strip()

        or collector_eurusd.status().get("asset")

        or "EURUSD"

    )





def sync_spread_expert_sources(

    *,

    days: int | None = None,

    pull_otc: bool = True,

    pull_dukascopy: bool = True,

) -> dict[str, Any]:

    """Pente fino: OTC tip Expert + Dukascopy candle-a-candle nas horas de sessao."""

    otc_a = _otc_asset()

    eu_a = _eurusd_asset()

    lookback = max(1, min(int(days or _env_int("OHLC_SPREAD_EXPERT_SYNC_DAYS", 14)), 90))



    result: dict[str, Any] = {

        "otc_asset": otc_a,

        "eurusd_asset": eu_a,

        "otc": {"ok": False, "upserted": 0, "error": None, "source": "expertoption"},

        "eurusd": {

            "ok": False,

            "upserted": 0,

            "error": None,

            "source": "dukascopy",

        },

        "days": lookback,

        "mode": "fine_comb",

    }



    if pull_otc:

        try:

            if collector_expert.status().get("asset") != otc_a:

                try:

                    collector_expert.set_asset(otc_a)

                except RuntimeError:

                    pass

            pull = collector_expert.pull_now(otc_a, hours=lookback * 24)

            result["otc"] = {

                "ok": True,

                "upserted": int((pull.get("pull") or {}).get("upserted") or 0),

                "error": None,

                "asset": otc_a,

                "pair": collector_expert.status().get("pair"),

                "source": "expertoption",

            }

        except Exception as exc:  # noqa: BLE001

            result["otc"] = {

                "ok": False,

                "upserted": 0,

                "error": str(exc)[:300],

                "asset": otc_a,

                "source": "expertoption",

            }



    if pull_dukascopy:

        try:

            if collector_eurusd.status().get("asset") != eu_a:

                try:

                    collector_eurusd.set_asset(eu_a)

                except RuntimeError:

                    pass

            recon = reconcile_eurusd_to_otc(

                otc_asset=otc_a,

                otc_table=TABLE_EXPERT,

                eurusd_asset=eu_a,

                days=lookback,

            )

            err_list = recon.get("errors") or []

            result["eurusd"] = {

                "ok": bool(recon.get("ok")),

                "upserted": int(recon.get("upserted") or 0),

                "error": ("; ".join(err_list)[:400] if err_list else None),

                "asset": eu_a,

                "source": "dukascopy",

                "days": lookback,

                "reconcile": {

                    "gaps_found": recon.get("gaps_found"),

                    "gaps_remaining": recon.get("gaps_remaining"),

                    "paired_session_before": recon.get("paired_session_before"),

                    "otc_session_hours": recon.get("otc_session_hours"),

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

                "source": "dukascopy",

            }



    if pull_dukascopy:

        result["ok"] = bool(result["eurusd"]["ok"])

    else:

        result["ok"] = bool(result["otc"]["ok"])

    result["table_otc"] = TABLE_EXPERT

    result["table_eurusd"] = TABLE_EURUSD

    return result


