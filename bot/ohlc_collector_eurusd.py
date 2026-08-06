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
    oldest_opened_at,
    stored_summary,
    supabase_ok,
    upsert_candles,
    TABLE,
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
        self._pull_started_at: float | None = None
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
            "pull_busy": False,
        }
        self._refresh_supabase_flag()
        self._refresh_stored()

    def is_pull_busy(self) -> bool:
        return self._pull_lock.locked()

    def release_pull_lock(self, *, force: bool = False) -> bool:
        """Libera lock de puxada travado (ex.: request morto / crash).

        Sem force: so libera se o lock estiver preso ha > STALE segundos.
        """
        stale_sec = max(60, _env_int("DUKASCOPY_PULL_STALE_SEC", 900))
        if not self._pull_lock.locked():
            self._pull_started_at = None
            return False
        started = self._pull_started_at
        age = (time.time() - started) if started else None
        if not force and age is not None and age < stale_sec:
            return False
        try:
            self._pull_lock.release()
        except RuntimeError:
            return False
        self._pull_started_at = None
        self._set(
            message=(
                f"Lock Dukascopy liberado"
                + (f" (preso {int(age)}s)" if age is not None else "")
            ),
            phase="idle" if not self.is_running() else "waiting",
            error=None,
        )
        return True

    def _acquire_pull(self) -> None:
        """Tenta lock; se preso ha muito tempo, libera e tenta de novo."""
        if self._pull_lock.acquire(blocking=False):
            self._pull_started_at = time.time()
            return
        if self.release_pull_lock(force=False):
            if self._pull_lock.acquire(blocking=False):
                self._pull_started_at = time.time()
                return
        raise RuntimeError(
            "Ja existe uma puxada Dukascopy em andamento. Aguarde "
            "alguns minutos ou reinicie o servico."
        )

    def _release_pull(self) -> None:
        self._pull_started_at = None
        try:
            self._pull_lock.release()
        except RuntimeError:
            pass

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
            out["pull_busy"] = self._pull_lock.locked()
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
        self._acquire_pull()
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
            self._release_pull()
        st = self.status()
        st["pull"] = {
            "upserted": upserted,
            "asset": asset,
            "source": SOURCE,
        }
        return st

    def pull_history(
        self,
        *,
        days: int | None = None,
        match_otc: bool = False,
        otc_asset: str | None = None,
        otc_table: str | None = None,
        otc_timeframe: str = "1h",
    ) -> dict[str, Any]:
        """Puxa Dukascopy.

        match_otc=True: desde o candle OTC mais antigo ate agora
        (Pocket em ohlc_candles ou Olymp em ohlc_candles_olymp; D1 em ohlc_candles_1d).
        match_otc=False: incremental desde o ultimo Dukascopy salvo.
        """
        ok, msg = supabase_ok()
        if not ok:
            raise RuntimeError(msg)
        self._acquire_pull()
        asset = self._asset
        upserted = 0
        fetched = 0
        chunks = 0
        skipped = 0
        start: datetime | None = None
        end: datetime | None = None
        otc_a = normalize_asset(
            otc_asset
            or os.environ.get("OHLC_SPREAD_OTC_ASSET", "").strip()
            or "EURUSD_otc"
        )
        otc_tbl = otc_table or TABLE
        otc_tf = (otc_timeframe or "1h").strip() or "1h"
        ndays = max(1, min(int(days or _env_int("OHLC_SPREAD_SYNC_DAYS", 14)), 800))
        matched_otc = False
        oldest_otc: datetime | None = None
        try:
            end = datetime.now(timezone.utc)

            if match_otc:
                try:
                    oldest_otc = oldest_opened_at(otc_a, otc_tf, table=otc_tbl)
                except Exception:  # noqa: BLE001
                    oldest_otc = None
                if oldest_otc is None:
                    raise RuntimeError(
                        f"Sem candles OTC em {otc_tbl} ({otc_a}, {otc_tf}). "
                        "Puxe o historico OTC antes do Dukascopy."
                    )
                # Do mais antigo OTC (1d de folga) ate agora.
                start = oldest_otc - timedelta(days=1)
                matched_otc = True
            else:
                start, end = self._resolve_window(
                    asset, days=ndays, prefer_incremental=True
                )

            span_days = max(1, int((end - start).total_seconds() // 86400) + 1)
            self._set(
                phase="history_pull",
                message=(
                    f"Dukascopy ~{span_days}d "
                    + (
                        f"(desde OTC {otc_a} @ {oldest_otc.date()})"
                        if matched_otc and oldest_otc is not None
                        else "(incremental / recentes)"
                    )
                    + "…"
                ),
                error=None,
                next_fetch_at=None,
            )

            chunk_hours = max(
                12, min(_env_int("DUKASCOPY_CHUNK_HOURS", 48), 72)
            )
            # Janelas longas (casar OTC): chunks curtos + menos 503.
            if matched_otc or span_days > 21:
                chunk_hours = min(chunk_hours, 24)
            elif span_days <= 21:
                chunk_hours = min(chunk_hours, 24)

            cursor = start
            by_t: dict[str, dict[str, Any]] = {}
            while cursor < end:
                chunks += 1
                piece_end = min(cursor + timedelta(hours=chunk_hours), end)
                self._set(
                    message=(
                        f"Dukascopy chunk {chunks}: "
                        f"{cursor.date()} → {piece_end.date()}…"
                    )
                )
                try:
                    part = fetch_eurusd_1h_rows_for_store(
                        cursor, piece_end, asset=asset
                    )
                except Exception as exc:  # noqa: BLE001
                    # 503 / fds: pula chunk e segue (nao zera o pull inteiro).
                    skipped += 1
                    self._set(
                        message=(
                            f"Chunk {chunks} pulado ({cursor.date()}): "
                            f"{str(exc)[:160]}"
                        )
                    )
                    part = []
                    time.sleep(2.0)
                for row in part:
                    by_t[str(row["opened_at"])] = row
                if part:
                    try:
                        n = upsert_candles(part, table=TABLE_EURUSD)
                    except Exception as exc:  # noqa: BLE001
                        skipped += 1
                        self._set(
                            message=(
                                f"Chunk {chunks} upsert falhou: "
                                f"{str(exc)[:180]}"
                            )
                        )
                        part = []
                        time.sleep(1.5)
                        cursor = piece_end
                        continue
                    upserted += n
                    fetched += len(part)
                    try:
                        delete_candles_by_source(
                            asset,
                            timeframe="1h",
                            source="pocket",
                            table=TABLE_EURUSD,
                            since=cursor,
                        )
                    except Exception:  # noqa: BLE001
                        pass
                cursor = piece_end
                time.sleep(0.4)

            rows = sorted(by_t.values(), key=lambda r: str(r["opened_at"]))
            oldest = rows[0]["opened_at"] if rows else None
            newest = rows[-1]["opened_at"] if rows else None
            if upserted <= 0 and end.weekday() < 5 and skipped == chunks:
                raise RuntimeError(
                    "Dukascopy historico retornou 0 velas. "
                    "Datafeed indisponivel ou bloqueado (HTTP 503). "
                    "Aguarde 1–2 min e tente de novo."
                )
            if upserted <= 0 and end.weekday() < 5 and not rows:
                # Pode ser so overlap ja gravado / fds no fim da janela.
                self._set(
                    phase="waiting" if self.is_running() else "idle",
                    message=(
                        "Dukascopy: nada novo nesta janela "
                        f"(chunks={chunks}, skipped={skipped})"
                    ),
                    error=None,
                )
            else:
                was_running = self.is_running()
                self._set(
                    phase="waiting" if was_running else "idle",
                    message=(
                        f"Historico Dukascopy ok: {upserted}/{fetched} velas "
                        f"em {chunks} chunks"
                        + (f" ({skipped} pulados)" if skipped else "")
                        + f" ({oldest or '?'} → {newest or '?'})"
                    ),
                    error=None,
                )
            with self._lock:
                self._snap["last_upsert"] = upserted
                self._snap["total_upserted"] = int(
                    self._snap.get("total_upserted", 0) or 0
                ) + upserted
                self._snap["source"] = SOURCE
                self._snap["last_sync_from"] = start.isoformat()
                self._snap["last_sync_to"] = end.isoformat()
                self._refresh_stored()
        except Exception as exc:  # noqa: BLE001
            self._set(
                phase="error" if not self.is_running() else "waiting",
                error=str(exc)[:300],
                message=f"Historico Dukascopy falhou: {exc}",
            )
            raise
        finally:
            self._release_pull()

        st = self.status()
        st["pull"] = {
            "mode": "history_otc" if matched_otc else "history_incremental",
            "upserted": upserted,
            "fetched": fetched,
            "chunks": chunks,
            "skipped_chunks": skipped,
            "asset": asset,
            "source": SOURCE,
            "match_otc": matched_otc,
            "otc_asset": otc_a,
            "from": start.isoformat() if start else None,
            "to": end.isoformat() if end else None,
            "oldest": None,
            "newest": None,
            "stored_oldest": None,
        }
        try:
            old = oldest_opened_at(asset, "1h", table=TABLE_EURUSD)
            last = last_opened_at(asset, "1h", table=TABLE_EURUSD)
            if old is not None:
                st["pull"]["stored_oldest"] = old.isoformat()
                st["pull"]["oldest"] = old.isoformat()
            if last is not None:
                st["pull"]["newest"] = last.isoformat()
        except Exception:  # noqa: BLE001
            pass
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
        # Overlap 48h para regravar velas recentes/incompletas e curar buracos.
        candidate = last - timedelta(hours=48)
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
        live_every = _env_int("OHLC_EURUSD_LIVE_POLL_SECONDS", 60)
        if align:
            wait = seconds_until_next_hourly_fetch(after_hour_seconds=after)
            mode = "hourly+live" if live_every > 0 else "hourly"
        else:
            wait = float(max(_env_int("OHLC_EURUSD_POLL_SECONDS", 3600), 60))
            mode = "poll"
            live_every = 0
        next_at = datetime.now(timezone.utc) + timedelta(seconds=wait)
        self._set(
            phase="waiting",
            poll_mode=mode,
            next_fetch_at=next_at.isoformat(),
            message=(
                f"Proximo fetch Dukascopy em ~{int(wait // 60)} min "
                f"({next_at.strftime('%H:%M:%S')} UTC)"
                + (f" · live a cada {live_every}s" if live_every > 0 else "")
            ),
        )
        if live_every <= 0 or not align:
            self._sleep_interruptible(wait)
            return

        end = time.monotonic() + max(wait, 0.0)
        asset = self._asset
        while not self._stop.is_set():
            left = end - time.monotonic()
            if left <= 0:
                break
            chunk = min(float(live_every), left)
            self._sleep_interruptible(chunk)
            if self._stop.is_set() or time.monotonic() >= end:
                break
            try:
                if not self._pull_lock.acquire(blocking=False):
                    continue
                self._pull_started_at = time.time()
                try:
                    self._set(
                        phase="live",
                        message="Live Dukascopy — atualizando hora corrente…",
                    )
                    n = self._sync_dukascopy(
                        asset,
                        days=max(1, _lookback_days(backfill=False)),
                        purge_pocket=True,
                        prefer_incremental=True,
                    )
                    left2 = max(0.0, end - time.monotonic())
                    next_at2 = datetime.now(timezone.utc) + timedelta(
                        seconds=left2
                    )
                    self._set(
                        phase="waiting",
                        next_fetch_at=next_at2.isoformat(),
                        message=(
                            f"Live Dukascopy ok ({n}) · horário "
                            f"{next_at.strftime('%H:%M:%S')} UTC"
                        ),
                    )
                finally:
                    self._release_pull()
            except Exception as exc:  # noqa: BLE001
                self._set(
                    phase="waiting",
                    message=f"Live Dukascopy falhou ({exc}); segue até o fetch",
                )

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
                    self._pull_started_at = time.time()
                    try:
                        n = self._sync_dukascopy(
                            asset,
                            days=days,
                            purge_pocket=True,
                            prefer_incremental=stored > 0,
                        )
                        self._set(message=f"Sync Dukascopy 1h: {n} velas")
                    finally:
                        self._release_pull()
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
                    self._pull_started_at = time.time()
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
                        self._release_pull()
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
