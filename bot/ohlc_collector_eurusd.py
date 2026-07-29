"""Coletor OHLC EURUSD (mercado) via Dukascopy → ohlc_candles_eurusd.

Nao usa Pocket. OTC continua em /ohlc (ohlc_candles).
"""

from __future__ import annotations

import os
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Any

from bot.dukascopy_fetch import fetch_eurusd_1h_rows_for_store
from bot.ohlc_collector import seconds_until_next_hourly_fetch
from bot.ohlc_store import (
    delete_candles_by_source,
    last_opened_at,
    stored_summary,
    supabase_ok,
    upsert_candles,
)
from bot.runner import normalize_asset

TABLE_EURUSD = "ohlc_candles_eurusd"
DEFAULT_ASSET = "EURUSD"
SOURCE = "dukascopy"


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _env_flag_default_on(name: str, default: str = "1") -> bool:
    return os.environ.get(name, default).strip().lower() not in (
        "0",
        "false",
        "no",
    )


def _default_asset() -> str:
    return normalize_asset(
        os.environ.get("OHLC_EURUSD_ASSET", "").strip() or DEFAULT_ASSET
    )


def _lookback_days(*, backfill: bool) -> int:
    if backfill:
        return max(1, min(_env_int("OHLC_SPREAD_SYNC_DAYS", 14), 90))
    return max(1, min(_env_int("OHLC_EURUSD_LIVE_DAYS", 2), 14))


class OhlcCollectorEurusd:
    """Thread: Dukascopy EURUSD 1h (Bid UTC) → upsert ohlc_candles_eurusd."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._pull_lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._asset = _default_asset()
        self._snap: dict[str, Any] = {
            "running": False,
            "asset": self._asset,
            "timeframe": "1h",
            "table": TABLE_EURUSD,
            "source": SOURCE,
            "supabase_ok": False,
            "supabase_msg": "",
            "phase": "idle",
            "last_upsert": 0,
            "total_upserted": 0,
            "next_fetch_at": None,
            "poll_mode": "hourly",
            "per_tf": {"1h": {"ok": 0, "err": None}},
            "error": None,
            "updated_at": None,
            "message": "Stand-by (Dukascopy)",
            "stored_count": None,
            "stored_last": None,
            "stored_err": None,
        }
        self._refresh_supabase_flag()
        self._refresh_stored()

    def _refresh_supabase_flag(self) -> None:
        ok, msg = supabase_ok()
        self._snap["supabase_ok"] = ok
        self._snap["supabase_msg"] = msg

    def _refresh_stored(self) -> None:
        summary = stored_summary(self._asset, "1h", table=TABLE_EURUSD)
        self._snap["stored_count"] = summary.get("stored_count")
        self._snap["stored_last"] = summary.get("stored_last")
        self._snap["stored_err"] = summary.get("stored_err")

    def is_running(self) -> bool:
        t = self._thread
        return t is not None and t.is_alive()

    def status(self) -> dict[str, Any]:
        with self._lock:
            self._refresh_supabase_flag()
            self._refresh_stored()
            out = dict(self._snap)
            out["running"] = self.is_running()
            out["asset"] = self._asset
            out["source"] = SOURCE
            return out

    def set_asset(self, asset: str) -> dict[str, Any]:
        a = normalize_asset(asset)
        if not a:
            raise ValueError("Ativo invalido.")
        if a.lower().endswith("_otc"):
            raise ValueError(
                "Use EURUSD (mercado), nao OTC. OTC fica em /ohlc."
            )
        with self._lock:
            if self.is_running():
                raise RuntimeError("Pare o coletor antes de trocar o ativo.")
            self._asset = a
            self._snap["asset"] = a
            self._snap["message"] = f"Ativo definido: {a} (Dukascopy)"
            self._refresh_stored()
        return self.status()

    def start(self, asset: str | None = None) -> dict[str, Any]:
        ok, msg = supabase_ok()
        if not ok:
            raise RuntimeError(msg)
        with self._lock:
            if self.is_running():
                return self.status()
            if asset:
                a = normalize_asset(asset)
                if a.lower().endswith("_otc"):
                    raise RuntimeError(
                        "Use EURUSD (mercado), nao OTC. OTC fica em /ohlc."
                    )
                self._asset = a
            self._stop.clear()
            self._snap.update(
                {
                    "running": True,
                    "asset": self._asset,
                    "source": SOURCE,
                    "phase": "starting",
                    "error": None,
                    "message": "Iniciando coletor Dukascopy EURUSD…",
                    "total_upserted": 0,
                    "per_tf": {"1h": {"ok": 0, "err": None}},
                }
            )
            self._thread = threading.Thread(
                target=self._run,
                name="ohlc-collector-eurusd-dukascopy",
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
            t.join(timeout=12.0)
        with self._lock:
            self._snap["running"] = False
            self._snap["phase"] = "idle"
            self._snap["message"] = "Parado"
            self._thread = None
        return self.status()

    def pull_now(self, *, days: int | None = None) -> dict[str, Any]:
        ok, msg = supabase_ok()
        if not ok:
            raise RuntimeError(msg)
        if not self._pull_lock.acquire(blocking=False):
            raise RuntimeError(
                "Ja existe uma puxada Dukascopy em andamento. Aguarde."
            )
        asset = self._asset
        upserted = 0
        try:
            self._set(
                phase="manual_pull",
                message="Puxada Dukascopy EURUSD 1h…",
                error=None,
                next_fetch_at=None,
            )
            upserted = self._sync_dukascopy(
                asset,
                days=days or _lookback_days(backfill=True),
                purge_pocket=True,
                prefer_incremental=True,
            )
            if upserted <= 0:
                now = datetime.now(timezone.utc)
                # Sab/dom: mercado FX pode nao ter bi5 — nao trata como bug duro.
                if now.weekday() < 5:
                    raise RuntimeError(
                        "Dukascopy retornou 0 velas. Datafeed indisponivel "
                        "ou bloqueado — tente de novo em alguns minutos."
                    )
                self._set(
                    phase="waiting" if self.is_running() else "idle",
                    message="Dukascopy 0 velas (fim de semana / mercado fechado)",
                    error=None,
                )
            else:
                was_running = self.is_running()
                self._set(
                    phase="waiting" if was_running else "idle",
                    message=f"Puxada Dukascopy ok: {upserted} velas EURUSD",
                    error=None,
                )
        except Exception as exc:  # noqa: BLE001
            self._set(
                phase="error" if not self.is_running() else "waiting",
                error=str(exc)[:300],
                message=f"Puxada Dukascopy falhou: {exc}",
            )
            raise
        finally:
            self._pull_lock.release()
        st = self.status()
        st["pull"] = {
            "upserted": upserted,
            "asset": asset,
            "source": SOURCE,
        }
        return st

    def _set(self, **kwargs: Any) -> None:
        with self._lock:
            self._snap.update(kwargs)
            self._snap["updated_at"] = datetime.now(timezone.utc).isoformat()

    def _resolve_window(
        self,
        asset: str,
        *,
        days: int,
        prefer_incremental: bool,
    ) -> tuple[datetime, datetime]:
        end = datetime.now(timezone.utc)
        start = end - timedelta(days=max(1, days))
        if not prefer_incremental:
            return start, end
        try:
            last = last_opened_at(asset, "1h", table=TABLE_EURUSD)
        except Exception:  # noqa: BLE001
            last = None
        if last is None:
            return start, end
        # Overlap 6h para regravar velas recentes/incompletas.
        candidate = last - timedelta(hours=6)
        # Nao encolher demais: se o gap for grande, respeita lookback days.
        if candidate > start:
            start = candidate
        return start, end

    def _sync_dukascopy(
        self,
        asset: str,
        *,
        days: int,
        purge_pocket: bool,
        prefer_incremental: bool = True,
    ) -> int:
        """Baixa janela Dukascopy e grava (sobrescreve Pocket na janela)."""
        start, end = self._resolve_window(
            asset, days=days, prefer_incremental=prefer_incremental
        )
        # Fatias de no max ~3 dias para nao estourar timeout HTTP do Railway.
        chunk_hours = max(12, min(_env_int("DUKASCOPY_CHUNK_HOURS", 72), 168))
        rows: list[dict[str, Any]] = []
        cursor = start
        while cursor < end:
            piece_end = min(cursor + timedelta(hours=chunk_hours), end)
            part = fetch_eurusd_1h_rows_for_store(
                cursor, piece_end, asset=asset
            )
            rows.extend(part)
            cursor = piece_end

        # Dedup por opened_at (chunks podem se tocar).
        by_t: dict[str, dict[str, Any]] = {}
        for row in rows:
            by_t[str(row["opened_at"])] = row
        rows = sorted(by_t.values(), key=lambda r: str(r["opened_at"]))

        n = upsert_candles(rows, table=TABLE_EURUSD) if rows else 0
        removed = 0
        if purge_pocket:
            try:
                removed = delete_candles_by_source(
                    asset,
                    timeframe="1h",
                    source="pocket",
                    table=TABLE_EURUSD,
                    since=start,
                )
            except Exception as exc:  # noqa: BLE001
                self._set(message=f"Upsert Dukascopy {n}; purge pocket: {exc}")
        with self._lock:
            prev = int(self._snap["per_tf"].get("1h", {}).get("ok", 0) or 0)
            self._snap["per_tf"]["1h"] = {"ok": prev + n, "err": None}
            self._snap["last_upsert"] = n
            self._snap["total_upserted"] = int(
                self._snap.get("total_upserted", 0) or 0
            ) + n
            self._snap["source"] = SOURCE
            self._snap["last_sync_from"] = start.isoformat()
            self._snap["last_sync_to"] = end.isoformat()
            if removed:
                self._snap["message"] = (
                    f"Dukascopy +{n} velas; removidas {removed} Pocket na janela"
                )
            self._refresh_stored()
        return n

    def _sleep_interruptible(self, seconds: float) -> None:
        end = time.monotonic() + max(seconds, 0.0)
        while not self._stop.is_set():
            left = end - time.monotonic()
            if left <= 0:
                break
            time.sleep(min(left, 1.0))

    def _wait_for_next_cycle(self) -> None:
        align = _env_flag_default_on("OHLC_EURUSD_ALIGN_HOUR", "1")
        # Apos virar a hora, espera um pouco para o bi5 da Dukascopy existir.
        after = _env_int("OHLC_EURUSD_AFTER_HOUR_SECONDS", 180)
        if align:
            wait = seconds_until_next_hourly_fetch(after_hour_seconds=after)
            mode = "hourly"
        else:
            wait = float(max(_env_int("OHLC_EURUSD_POLL_SECONDS", 3600), 60))
            mode = "poll"
        next_at = datetime.now(timezone.utc) + timedelta(seconds=wait)
        self._set(
            phase="waiting",
            poll_mode=mode,
            next_fetch_at=next_at.isoformat(),
            message=(
                f"Proximo fetch Dukascopy em ~{int(wait // 60)} min "
                f"({next_at.strftime('%H:%M:%S')} UTC)"
            ),
        )
        self._sleep_interruptible(wait)

    def _run(self) -> None:
        asset = self._asset
        try:
            self._refresh_stored()
            stored = int(self._snap.get("stored_count") or 0)
            days = _lookback_days(backfill=True)
            self._set(
                phase="backfill",
                message=(
                    f"Sync Dukascopy {days}d ({stored} velas salvas)…"
                    if stored > 0
                    else f"Backfill Dukascopy {days}d (base vazia)…"
                ),
            )
            try:
                if not self._pull_lock.acquire(blocking=False):
                    self._set(message="Backfill adiado: puxada manual em curso")
                else:
                    try:
                        n = self._sync_dukascopy(
                            asset,
                            days=days,
                            purge_pocket=True,
                            prefer_incremental=stored > 0,
                        )
                        self._set(message=f"Sync Dukascopy 1h: {n} velas")
                    finally:
                        self._pull_lock.release()
            except Exception as exc:  # noqa: BLE001
                with self._lock:
                    self._snap["per_tf"]["1h"] = {
                        "ok": 0,
                        "err": str(exc)[:200],
                    }
                self._set(
                    message=f"Sync Dukascopy falhou: {exc}",
                    error=str(exc)[:300],
                )

            while not self._stop.is_set():
                self._wait_for_next_cycle()
                if self._stop.is_set():
                    break
                try:
                    live_days = _lookback_days(backfill=False)
                    self._set(
                        phase="fetch",
                        message=f"Buscando Dukascopy EURUSD ({live_days}d)…",
                        next_fetch_at=None,
                    )
                    if not self._pull_lock.acquire(blocking=False):
                        self._set(message="Fetch pulado: puxada manual em curso")
                        continue
                    try:
                        n = self._sync_dukascopy(
                            asset,
                            days=live_days,
                            purge_pocket=True,
                            prefer_incremental=True,
                        )
                        self._set(
                            message=f"Fetch Dukascopy: {n} velas",
                            error=None,
                        )
                    finally:
                        self._pull_lock.release()
                except Exception as exc:  # noqa: BLE001
                    with self._lock:
                        cur = self._snap["per_tf"].get("1h", {})
                        self._snap["per_tf"]["1h"] = {
                            "ok": cur.get("ok", 0),
                            "err": str(exc)[:200],
                        }
                    self._set(
                        message=f"Fetch Dukascopy falhou: {exc}",
                        error=str(exc)[:300],
                    )
        except Exception as exc:  # noqa: BLE001
            self._set(
                phase="error",
                error=str(exc),
                message=f"Erro: {exc}",
                running=False,
            )
        finally:
            with self._lock:
                self._refresh_stored()
                if not self._stop.is_set() and self._snap.get("phase") != "error":
                    self._snap["phase"] = "idle"
                    self._snap["message"] = "Parado"
                self._snap["running"] = False
                self._snap["next_fetch_at"] = None


collector_eurusd = OhlcCollectorEurusd()
