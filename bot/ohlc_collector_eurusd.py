"""Coletor OHLC Pocket EURUSD (mercado, sem OTC) → tabela ohlc_candles_eurusd.

Espelha o coletor 1h (/ohlc), ativo fixo EURUSD. Usado pela ferramenta /ohlc-spread.
"""

from __future__ import annotations

import os
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Any

from BinaryOptionsToolsV2.pocketoption import PocketOption

from bot.ohlc_collector import (
    BACKFILL_OFFSET,
    LIVE_OFFSET,
    TIMEFRAMES,
    normalize_candle,
    seconds_until_next_hourly_fetch,
)
from bot.ohlc_store import (
    last_opened_at,
    stored_summary,
    supabase_ok,
    upsert_candles,
)
from bot.runner import is_connection_error, load_ssid, normalize_asset

TABLE_EURUSD = "ohlc_candles_eurusd"
DEFAULT_ASSET = "EURUSD"


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


class OhlcCollectorEurusd:
    """Thread: Pocket EURUSD 1h → upsert ohlc_candles_eurusd."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._asset = _default_asset()
        self._snap: dict[str, Any] = {
            "running": False,
            "asset": self._asset,
            "timeframe": "1h",
            "table": TABLE_EURUSD,
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
            "message": "Stand-by",
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
        summary = stored_summary(
            self._asset, "1h", table=TABLE_EURUSD
        )
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
            self._snap["message"] = f"Ativo definido: {a}"
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
                    "phase": "starting",
                    "error": None,
                    "message": "Iniciando coletor EURUSD…",
                    "total_upserted": 0,
                    "per_tf": {"1h": {"ok": 0, "err": None}},
                }
            )
            self._thread = threading.Thread(
                target=self._run,
                name="ohlc-collector-eurusd",
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

    def pull_now(self) -> dict[str, Any]:
        ok, msg = supabase_ok()
        if not ok:
            raise RuntimeError(msg)
        client: PocketOption | None = None
        upserted = 0
        asset = self._asset
        try:
            self._set(
                phase="manual_pull",
                message="Puxada manual EURUSD 1h…",
                error=None,
                next_fetch_at=None,
            )
            client = self._connect()
            upserted = self._upsert_tf(client, asset, "1h", backfill=True)
            was_running = self.is_running()
            self._set(
                phase="waiting" if was_running else "idle",
                message=f"Puxada manual ok: {upserted} velas EURUSD",
            )
        except Exception as exc:  # noqa: BLE001
            self._set(
                phase="error" if not self.is_running() else "waiting",
                error=str(exc),
                message=f"Puxada manual falhou: {exc}",
            )
            raise
        finally:
            self._close_client(client)
        st = self.status()
        st["pull"] = {"upserted": upserted, "asset": asset}
        return st

    def _set(self, **kwargs: Any) -> None:
        with self._lock:
            self._snap.update(kwargs)
            self._snap["updated_at"] = datetime.now(timezone.utc).isoformat()

    def _offset_for_tf(self, asset: str, tf: str, *, backfill: bool) -> int:
        period = TIMEFRAMES[tf]
        default = BACKFILL_OFFSET[tf] if backfill else LIVE_OFFSET[tf]
        try:
            last = last_opened_at(asset, tf, table=TABLE_EURUSD)
        except Exception:  # noqa: BLE001
            return default
        if last is None:
            return default
        now = datetime.now(timezone.utc)
        gap = int((now - last).total_seconds()) + period * 3
        if backfill:
            return max(gap, period * 3)
        return max(min(gap, default), period * 2)

    def _fetch_tf(
        self, client: PocketOption, asset: str, tf: str, offset: int
    ) -> list[dict[str, Any]]:
        period = TIMEFRAMES[tf]
        raw = client.get_candles(asset, period, int(offset))
        if not isinstance(raw, list):
            raise RuntimeError(f"Resposta inesperada get_candles ({tf})")
        rows: list[dict[str, Any]] = []
        for item in raw:
            if not isinstance(item, dict):
                continue
            norm = normalize_candle(item, asset=asset, timeframe=tf)
            if norm:
                rows.append(norm)
        return rows

    def _upsert_tf(
        self,
        client: PocketOption,
        asset: str,
        tf: str,
        *,
        backfill: bool = False,
    ) -> int:
        offset = self._offset_for_tf(asset, tf, backfill=backfill)
        rows = self._fetch_tf(client, asset, tf, offset)
        if not rows:
            return 0
        try:
            last = last_opened_at(asset, tf, table=TABLE_EURUSD)
        except Exception:  # noqa: BLE001
            last = None
        if last is not None:
            period = TIMEFRAMES[tf]
            cutoff = last - timedelta(seconds=period)
            filtered: list[dict[str, Any]] = []
            for row in rows:
                try:
                    ts = datetime.fromisoformat(
                        str(row["opened_at"]).replace("Z", "+00:00")
                    )
                except ValueError:
                    filtered.append(row)
                    continue
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                if ts >= cutoff:
                    filtered.append(row)
            rows = filtered
        if not rows:
            return 0
        n = upsert_candles(rows, table=TABLE_EURUSD)
        with self._lock:
            prev = int(self._snap["per_tf"].get(tf, {}).get("ok", 0) or 0)
            self._snap["per_tf"][tf] = {"ok": prev + n, "err": None}
            self._snap["last_upsert"] = n
            self._snap["total_upserted"] = int(
                self._snap.get("total_upserted", 0) or 0
            ) + n
            self._refresh_stored()
        return n

    def _connect(self) -> PocketOption:
        ssid = load_ssid()
        client = PocketOption(ssid)
        wait = float(os.environ.get("OHLC_CONNECT_WAIT", "5"))
        time.sleep(max(wait, 2.0))
        return client

    def _close_client(self, client: PocketOption | None) -> None:
        if client is None:
            return
        try:
            client.close()
        except Exception:  # noqa: BLE001
            pass

    def _sleep_interruptible(self, seconds: float) -> None:
        end = time.monotonic() + max(seconds, 0.0)
        while not self._stop.is_set():
            left = end - time.monotonic()
            if left <= 0:
                break
            time.sleep(min(left, 1.0))

    def _wait_for_next_cycle(self) -> None:
        align = _env_flag_default_on("OHLC_EURUSD_ALIGN_HOUR", "1")
        after = _env_int("OHLC_EURUSD_AFTER_HOUR_SECONDS", 120)
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
                f"Proximo fetch EURUSD em ~{int(wait // 60)} min "
                f"({next_at.strftime('%H:%M:%S')} UTC)"
            ),
        )
        self._sleep_interruptible(wait)

    def _run(self) -> None:
        asset = self._asset
        client: PocketOption | None = None
        try:
            self._set(phase="connect", message=f"Conectando Pocket ({asset})…")
            client = self._connect()
            self._refresh_stored()
            stored = int(self._snap.get("stored_count") or 0)
            self._set(
                phase="backfill",
                message=(
                    f"Sync EURUSD incremental ({stored} velas)…"
                    if stored > 0
                    else "Backfill EURUSD (base vazia)…"
                ),
            )
            try:
                n = self._upsert_tf(client, asset, "1h", backfill=True)
                self._set(message=f"Sync EURUSD 1h: {n} velas")
            except Exception as exc:  # noqa: BLE001
                with self._lock:
                    self._snap["per_tf"]["1h"] = {
                        "ok": 0,
                        "err": str(exc)[:200],
                    }
                self._set(message=f"Sync EURUSD falhou: {exc}")

            self._close_client(client)
            client = None

            while not self._stop.is_set():
                self._wait_for_next_cycle()
                if self._stop.is_set():
                    break
                try:
                    self._set(
                        phase="fetch",
                        message=f"Buscando EURUSD 1h ({asset})…",
                        next_fetch_at=None,
                    )
                    client = self._connect()
                    try:
                        n = self._upsert_tf(
                            client, asset, "1h", backfill=False
                        )
                        self._set(message=f"Fetch EURUSD: {n} velas")
                    except Exception as exc:  # noqa: BLE001
                        with self._lock:
                            cur = self._snap["per_tf"].get("1h", {})
                            self._snap["per_tf"]["1h"] = {
                                "ok": cur.get("ok", 0),
                                "err": str(exc)[:200],
                            }
                        if is_connection_error(exc):
                            self._set(
                                phase="reconnect",
                                message=f"Reconectando… ({exc})",
                            )
                            self._close_client(client)
                            time.sleep(3.0)
                            client = self._connect()
                            self._upsert_tf(
                                client, asset, "1h", backfill=False
                            )
                        else:
                            self._set(message=f"Fetch EURUSD falhou: {exc}")
                except Exception as exc:  # noqa: BLE001
                    self._set(message=f"Ciclo falhou: {exc}")
                finally:
                    self._close_client(client)
                    client = None
        except Exception as exc:  # noqa: BLE001
            self._set(
                phase="error",
                error=str(exc),
                message=f"Erro: {exc}",
                running=False,
            )
        finally:
            self._close_client(client)
            with self._lock:
                self._refresh_stored()
                if not self._stop.is_set() and self._snap.get("phase") != "error":
                    self._snap["phase"] = "idle"
                    self._snap["message"] = "Parado"
                self._snap["running"] = False
                self._snap["next_fetch_at"] = None


collector_eurusd = OhlcCollectorEurusd()
