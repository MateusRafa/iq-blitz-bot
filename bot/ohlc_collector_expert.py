"""Coletor OHLC ExpertOption OTC → Supabase (ferramenta /ohlc-spread-expert-1d).

Tabela: ohlc_candles_expert (separada da Pocket ohlc_candles).
Ativo de store: EURUSD_otc_expert.
Fonte: bot.expertoption_fetch (WS ExpertOption, cookie token).
"""

from __future__ import annotations

import os
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Any

from bot.ohlc_collector import seconds_until_next_hourly_fetch
from bot.ohlc_store import (
    TABLE_EXPERT,
    merge_ohlc_with_existing,
    stored_summary,
    supabase_ok,
    upsert_candles,
)
from bot.expertoption_fetch import (
    default_pair,
    default_store_asset,
    fetch_ohlc_1h_rows_for_store,
    expertoption_available,
)
from bot.runner import normalize_asset

TABLE = TABLE_EXPERT
SOURCE = "expertoption"


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


class OhlcCollectorExpert:
    """Thread: Expert WS → candles 1h → upsert ohlc_candles_expert."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._asset = normalize_asset(default_store_asset())
        self._pair = default_pair()
        self._snap: dict[str, Any] = {
            "running": False,
            "asset": self._asset,
            "pair": self._pair,
            "source": SOURCE,
            "table": TABLE,
            "timeframes": ["1h"],
            "supabase_ok": False,
            "supabase_msg": "",
            "expert_ok": False,
            "expert_msg": "",
            "phase": "idle",
            "last_upsert": 0,
            "total_upserted": 0,
            "next_fetch_at": None,
            "poll_mode": "hourly",
            "error": None,
            "updated_at": None,
            "message": "Stand-by",
            "stored_count": None,
            "stored_last": None,
            "stored_err": None,
        }
        self._refresh_flags()
        self._refresh_stored()

    def _refresh_flags(self) -> None:
        ok, msg = supabase_ok()
        self._snap["supabase_ok"] = ok
        self._snap["supabase_msg"] = msg
        o_ok, o_msg = expertoption_available()
        self._snap["expert_ok"] = o_ok
        self._snap["expert_msg"] = o_msg

    def _refresh_stored(self) -> None:
        summary = stored_summary(self._asset, "1h", table=TABLE)
        self._snap["stored_count"] = summary.get("stored_count")
        self._snap["stored_last"] = summary.get("stored_last")
        self._snap["stored_err"] = summary.get("stored_err")

    def is_running(self) -> bool:
        t = self._thread
        return t is not None and t.is_alive()

    def status(self) -> dict[str, Any]:
        with self._lock:
            self._refresh_flags()
            self._refresh_stored()
            out = dict(self._snap)
            out["running"] = self.is_running()
            out["asset"] = self._asset
            out["pair"] = self._pair
            return out

    def set_asset(self, asset: str) -> dict[str, Any]:
        a = normalize_asset(asset)
        if not a:
            raise ValueError("Ativo invalido.")
        with self._lock:
            if self.is_running():
                raise RuntimeError("Pare o coletor Expert antes de trocar o ativo.")
            self._asset = a
            self._snap["asset"] = a
            self._snap["message"] = f"Ativo definido: {a}"
            self._refresh_stored()
        return self.status()

    def set_pair(self, pair: str) -> dict[str, Any]:
        p = (pair or "").strip().upper()
        if not p:
            raise ValueError("Par Expert invalido.")
        with self._lock:
            if self.is_running():
                raise RuntimeError("Pare o coletor Expert antes de trocar o par.")
            self._pair = p
            self._snap["pair"] = p
            self._snap["message"] = f"Par Expert: {p}"
        return self.status()

    def start(self, asset: str | None = None) -> dict[str, Any]:
        ok, msg = supabase_ok()
        if not ok:
            raise RuntimeError(msg)
        o_ok, o_msg = expertoption_available()
        if not o_ok:
            raise RuntimeError(o_msg)
        with self._lock:
            if self.is_running():
                return self.status()
            if asset:
                self._asset = normalize_asset(asset)
            self._stop.clear()
            self._snap.update(
                {
                    "running": True,
                    "asset": self._asset,
                    "pair": self._pair,
                    "phase": "starting",
                    "error": None,
                    "message": "Iniciando coletor Expert…",
                    "total_upserted": 0,
                }
            )
            self._thread = threading.Thread(
                target=self._run,
                name="ohlc-collector-expert",
                daemon=True,
            )
            self._thread.start()
        return self.status()

    def stop(self) -> dict[str, Any]:
        with self._lock:
            self._stop.set()
            self._snap["phase"] = "stopping"
            self._snap["message"] = "Parando…"
            t = self._thread
        if t is not None and t.is_alive():
            t.join(timeout=20.0)
        with self._lock:
            self._snap["running"] = False
            self._snap["phase"] = "idle"
            self._snap["message"] = "Parado"
            self._thread = None
        return self.status()

    def pull_now(
        self,
        asset: str | None = None,
        *,
        hours: int | None = None,
    ) -> dict[str, Any]:
        ok, msg = supabase_ok()
        if not ok:
            raise RuntimeError(msg)
        o_ok, o_msg = expertoption_available()
        if not o_ok:
            raise RuntimeError(o_msg)
        target = normalize_asset(asset) if asset else self._asset
        lookback_h = max(
            12,
            int(hours or _env_int("OHLC_EXPERT_LIVE_HOURS", 24 * 14)),
        )
        try:
            self._set(
                phase="manual_pull",
                message=f"Puxada Expert 1h ({target} / {self._pair})…",
                error=None,
            )
            upserted = self._pull_and_upsert(target, hours=lookback_h)
            was_running = self.is_running()
            self._set(
                phase="waiting" if was_running else "idle",
                message=f"Puxada Expert ok: {upserted} velas ({target})",
            )
            return {
                "ok": True,
                "pull": {
                    "upserted": upserted,
                    "asset": target,
                    "pair": self._pair,
                    "hours": lookback_h,
                    "source": SOURCE,
                },
                "status": self.status(),
            }
        except Exception as exc:  # noqa: BLE001
            self._set(
                phase="error" if not self.is_running() else "waiting",
                error=str(exc),
                message=f"Puxada Expert falhou: {exc}",
            )
            raise

    def pull_history(
        self,
        asset: str | None = None,
        *,
        days: int = 60,
    ) -> dict[str, Any]:
        days = max(1, min(int(days), 800))
        return self.pull_now(asset, hours=days * 24)

    def _pull_and_upsert(self, asset: str, *, hours: int) -> int:
        rows = fetch_ohlc_1h_rows_for_store(
            hours=hours,
            pair=self._pair,
            asset=asset,
        )
        if not rows:
            self._set(message="Expert: 0 candles (par/token?)")
            return 0
        merged = merge_ohlc_with_existing(
            rows, asset=asset, timeframe="1h", table=TABLE
        )
        n = upsert_candles(merged, table=TABLE)
        self._set(
            last_upsert=n,
            total_upserted=int(self._snap.get("total_upserted") or 0) + n,
            updated_at=datetime.now(timezone.utc).isoformat(),
        )
        self._refresh_stored()
        return n

    def _set(self, **kwargs: Any) -> None:
        with self._lock:
            self._snap.update(kwargs)
            self._snap["updated_at"] = datetime.now(timezone.utc).isoformat()

    def _run(self) -> None:
        after_s = _env_int("OHLC_EXPERT_AFTER_HOUR_SECONDS", 120)
        live_poll = _env_int("OHLC_EXPERT_LIVE_POLL_SECONDS", 60)
        live_hours = _env_int("OHLC_EXPERT_LIVE_HOURS", 48)
        align = os.environ.get("OHLC_EXPERT_ALIGN_HOUR", "1").strip() not in (
            "0",
            "false",
            "no",
        )
        # Backfill inicial
        try:
            self._set(phase="backfill", message="Backfill Expert inicial…")
            n = self._pull_and_upsert(
                self._asset,
                hours=_env_int("OHLC_EXPERT_BACKFILL_HOURS", 24 * 30),
            )
            self._set(message=f"Backfill Expert: {n} velas")
        except Exception as exc:  # noqa: BLE001
            self._set(phase="error", error=str(exc), message=str(exc))

        while not self._stop.is_set():
            try:
                if align:
                    wait = seconds_until_next_hourly_fetch(
                        after_hour_seconds=after_s
                    )
                    next_at = datetime.now(timezone.utc) + timedelta(seconds=wait)
                    self._set(
                        phase="waiting",
                        next_fetch_at=next_at.isoformat(),
                        message=f"Aguardando hora UTC (+{after_s}s)…",
                    )
                    # Poll live da vela em formacao
                    deadline = time.monotonic() + wait
                    while not self._stop.is_set() and time.monotonic() < deadline:
                        if live_poll > 0:
                            try:
                                self._set(
                                    phase="live_poll",
                                    message="Live poll Expert…",
                                )
                                self._pull_and_upsert(
                                    self._asset, hours=live_hours
                                )
                                self._set(
                                    phase="waiting",
                                    message="Aguardando proxima hora…",
                                )
                            except Exception as exc:  # noqa: BLE001
                                self._set(
                                    error=str(exc)[:300],
                                    message=f"Live poll falhou: {exc}",
                                )
                            self._stop.wait(live_poll)
                        else:
                            self._stop.wait(min(5.0, deadline - time.monotonic()))
                    if self._stop.is_set():
                        break
                    self._set(phase="hourly", message="Fetch horario Expert…")
                    self._pull_and_upsert(self._asset, hours=live_hours)
                else:
                    poll = max(60, _env_int("OHLC_EXPERT_POLL_SECONDS", 3600))
                    self._set(
                        phase="polling",
                        message=f"Poll a cada {poll}s…",
                    )
                    self._pull_and_upsert(self._asset, hours=live_hours)
                    self._stop.wait(poll)
            except Exception as exc:  # noqa: BLE001
                self._set(
                    phase="error",
                    error=str(exc),
                    message=f"Loop Expert: {exc}",
                )
                self._stop.wait(30.0)

        self._set(phase="idle", running=False, message="Parado")


collector_expert = OhlcCollectorExpert()
