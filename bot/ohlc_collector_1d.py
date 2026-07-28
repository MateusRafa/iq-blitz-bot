"""Coletor OHLC diario Pocket → Supabase (substitui ferramenta 1m).

Puxa candles D1 nativos da Pocket (period=86400). Fallback: agrega 1h.
Sync automatico todos os dias as 00:05 (horario Pocket). Sem limite de retencao.
"""

from __future__ import annotations

import os
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FuturesTimeout
from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable, TypeVar

from BinaryOptionsToolsV2.pocketoption import PocketOption

from bot.ohlc_collector import normalize_candle
from bot.ohlc_store import (
    delete_candles_since,
    last_opened_at,
    stored_summary,
    supabase_ok,
    upsert_candles,
)
from bot.runner import is_connection_error, load_ssid, normalize_asset

T = TypeVar("T")

TIMEFRAME = "1d"
TABLE_1D = "ohlc_candles_1d"
PERIOD_D1 = 86400
SOURCE_TF = "1h"
SOURCE_PERIOD = 3600
DAY_SECONDS = PERIOD_D1

DEFAULT_BACKFILL_DAYS = 3650  # ~10 anos (API devolve o que tiver)
DEFAULT_SYNC_HOUR = 0
DEFAULT_SYNC_MINUTE = 5


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


def pocket_tz_offset() -> int:
    """Segundos somados a UTC para obter horario Pocket (ex.: UTC-3 → -10800)."""
    return _env_int("POCKET_TIME_OFFSET", -10800)


def pocket_local(dt_utc: datetime) -> datetime:
    """UTC → relogio local da Pocket (offset fixo da plataforma)."""
    if dt_utc.tzinfo is None:
        dt_utc = dt_utc.replace(tzinfo=timezone.utc)
    return dt_utc + timedelta(seconds=pocket_tz_offset())


def pocket_day_key(dt_utc: datetime, *, offset: int | None = None) -> str:
    off = pocket_tz_offset() if offset is None else offset
    if dt_utc.tzinfo is None:
        dt_utc = dt_utc.replace(tzinfo=timezone.utc)
    return (dt_utc + timedelta(seconds=off)).date().isoformat()


def pocket_midnight_utc(day: date, *, offset: int | None = None) -> datetime:
    """Meia-noite Pocket (dia civil) como instante UTC."""
    off = pocket_tz_offset() if offset is None else offset
    local_mid = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)
    return local_mid - timedelta(seconds=off)


def seconds_until_next_pocket_daily_fetch(
    *,
    hour: int | None = None,
    minute: int | None = None,
    pocket_offset: int | None = None,
) -> float:
    """Segundos ate o proximo sync diario (padrao 00:05 horario Pocket)."""
    h = DEFAULT_SYNC_HOUR if hour is None else hour
    m = DEFAULT_SYNC_MINUTE if minute is None else minute
    off = pocket_tz_offset() if pocket_offset is None else pocket_offset
    now_utc = datetime.now(timezone.utc)
    local = now_utc + timedelta(seconds=off)
    target_local = local.replace(hour=h, minute=m, second=0, microsecond=0)
    if local >= target_local:
        target_local += timedelta(days=1)
    target_utc = target_local - timedelta(seconds=off)
    return max((target_utc - now_utc).total_seconds(), 1.0)


def _parse_opened_at(raw: Any) -> datetime | None:
    if not raw:
        return None
    try:
        ts = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
    except ValueError:
        return None
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)


def aggregate_hourly_to_daily(
    hourly: list[dict[str, Any]],
    *,
    asset: str,
    pocket_offset: int | None = None,
    include_today: bool = False,
) -> list[dict[str, Any]]:
    """Agrupa velas 1h em D1 (open=1a hora, high=max, low=min, close=ultima)."""
    if not hourly:
        return []
    off = pocket_tz_offset() if pocket_offset is None else pocket_offset
    today_key = pocket_day_key(datetime.now(timezone.utc), offset=off)
    buckets: dict[str, list[dict[str, Any]]] = defaultdict(list)

    for row in hourly:
        ts = _parse_opened_at(row.get("opened_at"))
        if ts is None:
            continue
        dk = pocket_day_key(ts, offset=off)
        if not include_today and dk >= today_key:
            continue
        buckets[dk].append(row)

    now_iso = datetime.now(timezone.utc).isoformat()
    daily: list[dict[str, Any]] = []
    for dk in sorted(buckets.keys()):
        parts = sorted(
            buckets[dk],
            key=lambda r: str(r.get("opened_at") or ""),
        )
        try:
            o = float(parts[0]["open"])
            h = max(float(p["high"]) for p in parts)
            lo = min(float(p["low"]) for p in parts)
            c = float(parts[-1]["close"])
        except (TypeError, ValueError, KeyError):
            continue
        h = max(h, o, c)
        lo = min(lo, o, c)
        vols = [
            float(p["volume"])
            for p in parts
            if p.get("volume") is not None
        ]
        y, mo, d = (int(x) for x in dk.split("-"))
        opened = pocket_midnight_utc(date(y, mo, d), offset=off)
        row: dict[str, Any] = {
            "asset": asset,
            "timeframe": TIMEFRAME,
            "opened_at": opened.isoformat(),
            "open": o,
            "high": h,
            "low": lo,
            "close": c,
            "source": "pocket_agg",
            "updated_at": now_iso,
        }
        if vols:
            row["volume"] = sum(vols)
        daily.append(row)
    return daily


def filter_closed_daily(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Remove o dia civil atual (vela D1 em formacao) se CLOSED_ONLY."""
    if not rows or not _env_flag_default_on("OHLC_1D_CLOSED_ONLY", "1"):
        return rows
    today_key = pocket_day_key(datetime.now(timezone.utc))
    keep: list[dict[str, Any]] = []
    for row in rows:
        ts = _parse_opened_at(row.get("opened_at"))
        if ts is None:
            keep.append(row)
            continue
        if pocket_day_key(ts) >= today_key:
            continue
        keep.append(row)
    return keep


def normalize_d1_rows(
    raw: list[dict[str, Any]], *, asset: str
) -> list[dict[str, Any]]:
    """Normaliza lista bruta da Pocket para linhas D1."""
    by_key: dict[str, dict[str, Any]] = {}
    for item in raw:
        if not isinstance(item, dict):
            continue
        norm = normalize_candle(item, asset=asset, timeframe=TIMEFRAME)
        if not norm:
            continue
        key = str(norm["opened_at"])[:19]
        by_key[key] = norm
    rows = list(by_key.values())
    rows.sort(key=lambda r: str(r.get("opened_at") or ""))
    return rows


def _call_with_timeout(
    fn: Callable[[], T], *, timeout: float, default: T, label: str = ""
) -> T:
    pool = ThreadPoolExecutor(max_workers=1)
    try:
        fut = pool.submit(fn)
        try:
            return fut.result(timeout=max(timeout, 1.0))
        except FuturesTimeout:
            if label:
                print(f"[ohlc_1d] timeout {timeout:.0f}s: {label}", flush=True)
            return default
        except Exception as exc:  # noqa: BLE001
            if label:
                print(f"[ohlc_1d] erro {label}: {exc}", flush=True)
            return default
    finally:
        pool.shutdown(wait=False, cancel_futures=True)


def _default_asset() -> str:
    return normalize_asset(
        os.environ.get("OHLC_1D_ASSET", "").strip()
        or os.environ.get("OHLC_ASSET", "").strip()
        or os.environ.get("POCKET_ASSET", "EURUSD_otc")
    )


class OhlcCollector1d:
    """Thread: Pocket D1 nativo → upsert Supabase; sync diario 00:05 Pocket."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._pull_lock = threading.Lock()
        self._asset = _default_asset()
        self._snap: dict[str, Any] = {
            "running": False,
            "asset": self._asset,
            "timeframe": TIMEFRAME,
            "table": TABLE_1D,
            "supabase_ok": False,
            "supabase_msg": "",
            "phase": "idle",
            "last_upsert": 0,
            "total_upserted": 0,
            "next_fetch_at": None,
            "poll_mode": "daily_pocket",
            "pocket_tz_offset": pocket_tz_offset(),
            "sync_at_pocket": f"{DEFAULT_SYNC_HOUR:02d}:{DEFAULT_SYNC_MINUTE:02d}",
            "per_tf": {TIMEFRAME: {"ok": 0, "err": None}},
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
        summary = stored_summary(self._asset, TIMEFRAME, table=TABLE_1D)
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
                self._asset = normalize_asset(asset)
            self._stop.clear()
            self._snap.update(
                {
                    "running": True,
                    "asset": self._asset,
                    "phase": "starting",
                    "error": None,
                    "message": "Iniciando coletor diario…",
                    "total_upserted": 0,
                    "per_tf": {TIMEFRAME: {"ok": 0, "err": None}},
                }
            )
            self._thread = threading.Thread(
                target=self._run,
                name="ohlc-collector-1d",
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
            t.join(timeout=15.0)
        with self._lock:
            self._snap["running"] = False
            self._snap["phase"] = "idle"
            self._snap["message"] = "Parado"
            self._thread = None
        return self.status()

    def pull_now(self) -> dict[str, Any]:
        """Puxada manual: backfill D1 nativo + upsert."""
        ok, msg = supabase_ok()
        if not ok:
            raise RuntimeError(msg)
        if not self._pull_lock.acquire(blocking=False):
            raise RuntimeError("Ja existe uma puxada manual em andamento.")
        client: PocketOption | None = None
        upserted = 0
        asset = self._asset
        try:
            saved_next = None
            with self._lock:
                if self.is_running():
                    saved_next = self._snap.get("next_fetch_at")
            self._set(
                phase="manual_pull",
                message="Puxada manual (D1 nativo Pocket)…",
                error=None,
                next_fetch_at=None,
            )
            client = self._connect()
            upserted = self._full_pull(client, asset)
            was_running = self.is_running()
            if was_running:
                self._set(
                    phase="waiting",
                    next_fetch_at=saved_next,
                    message=f"Puxada manual ok: {upserted} velas D1 upsert",
                )
            else:
                self._set(
                    phase="idle",
                    message=f"Puxada manual ok: {upserted} velas D1 upsert",
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
            self._pull_lock.release()
        st = self.status()
        st["pull"] = {"upserted": upserted, "asset": asset}
        return st

    def resync_recent(self, days: int = 30) -> dict[str, Any]:
        """Apaga os ultimos N dias no DB e repuxa da Pocket."""
        ok, msg = supabase_ok()
        if not ok:
            raise RuntimeError(msg)
        ndays = max(1, min(int(days), 365))
        asset = self._asset
        now = datetime.now(timezone.utc)
        since = pocket_midnight_utc(
            (pocket_local(now) - timedelta(days=ndays)).date()
        )
        self._set(
            phase="resync",
            message=f"Re-sync: apagando desde {since.date().isoformat()}…",
            error=None,
        )
        deleted = delete_candles_since(
            asset, since, timeframe=TIMEFRAME, table=TABLE_1D
        )
        client: PocketOption | None = None
        upserted = 0
        try:
            client = self._connect()
            offset = ndays * DAY_SECONDS + DAY_SECONDS * 3
            upserted = self._pull_and_upsert(client, asset, offset_sec=offset)
            was_running = self.is_running()
            self._set(
                phase="waiting" if was_running else "idle",
                message=(
                    f"Re-sync ok: apagadas {deleted}, upsert {upserted} D1"
                ),
            )
        except Exception as exc:  # noqa: BLE001
            self._set(
                phase="error" if not self.is_running() else "waiting",
                error=str(exc),
                message=f"Re-sync falhou: {exc}",
            )
            raise
        finally:
            self._close_client(client)
        st = self.status()
        st["resync"] = {
            "deleted": deleted,
            "upserted": upserted,
            "since": since.isoformat(),
            "days": ndays,
            "asset": asset,
        }
        return st

    def _set(self, **kwargs: Any) -> None:
        with self._lock:
            self._snap.update(kwargs)
            self._snap["updated_at"] = datetime.now(timezone.utc).isoformat()

    def _backfill_offset_sec(self, asset: str) -> int:
        days = _env_int("OHLC_1D_BACKFILL_DAYS", DEFAULT_BACKFILL_DAYS)
        default = max(days, 30) * DAY_SECONDS
        try:
            oldest = last_opened_at(asset, TIMEFRAME, table=TABLE_1D)
        except Exception:  # noqa: BLE001
            return default
        if oldest is None:
            return default
        gap = int((datetime.now(timezone.utc) - oldest).total_seconds())
        return max(gap + DAY_SECONDS * 3, default, DAY_SECONDS * 7)

    def _fetch_d1_native(
        self, client: PocketOption, asset: str, offset_sec: int
    ) -> list[dict[str, Any]]:
        """Puxa D1 nativo (period=86400) — mesmo timeframe da UI Pocket."""
        offset_sec = int(offset_sec)
        api_timeout = float(_env_int("OHLC_1D_API_TIMEOUT", 120))
        days = max(offset_sec / DAY_SECONDS, 1)
        max_rows = min(int(days) + 30, 10_000)
        self._set(
            message=(
                f"Pocket D1 nativo (~{days:.0f} dias, max {max_rows} velas)…"
            )
        )

        def _pull_live() -> list[dict[str, Any]]:
            hours = max(24.0, offset_sec / 3600.0)
            it = client.get_candles_live(
                asset, PERIOD_D1, hours=hours, max_rows=max_rows
            )
            try:
                closed, _forming = next(it)
                if isinstance(closed, list):
                    return normalize_d1_rows(closed, asset=asset)
                return []
            finally:
                try:
                    it.close()  # type: ignore[attr-defined]
                except Exception:  # noqa: BLE001
                    pass

        rows = _call_with_timeout(
            _pull_live,
            timeout=api_timeout,
            default=[],
            label="get_candles_live_D1",
        )
        if rows:
            return rows

        def _pull_get() -> list[dict[str, Any]]:
            raw = client.get_candles(asset, PERIOD_D1, offset_sec)
            if not isinstance(raw, list):
                return []
            return normalize_d1_rows(raw, asset=asset)

        return _call_with_timeout(
            _pull_get,
            timeout=api_timeout,
            default=[],
            label="get_candles_D1",
        )

    def _fetch_hourly(
        self, client: PocketOption, asset: str, offset_sec: int
    ) -> list[dict[str, Any]]:
        offset_sec = int(offset_sec)
        api_timeout = float(_env_int("OHLC_1D_API_TIMEOUT", 120))
        days = offset_sec / DAY_SECONDS
        self._set(
            message=f"Pocket get_candles 1h (~{days:.0f} dias de historico)…"
        )

        def _pull() -> list[dict[str, Any]]:
            raw = client.get_candles(asset, SOURCE_PERIOD, offset_sec)
            if not isinstance(raw, list):
                return []
            rows: list[dict[str, Any]] = []
            for item in raw:
                if not isinstance(item, dict):
                    continue
                norm = normalize_candle(
                    item, asset=asset, timeframe=SOURCE_TF
                )
                if norm:
                    rows.append(norm)
            rows.sort(key=lambda r: str(r.get("opened_at") or ""))
            return rows

        hourly = _call_with_timeout(
            _pull,
            timeout=api_timeout,
            default=[],
            label="get_candles_1h",
        )
        if not hourly:
            raise RuntimeError(
                "Pocket devolveu 0 velas 1h — verifique SSID e ativo."
            )
        return hourly

    def _fetch_daily(
        self, client: PocketOption, asset: str, offset_sec: int
    ) -> tuple[list[dict[str, Any]], str]:
        """Retorna (velas D1, fonte: native|agg)."""
        native = self._fetch_d1_native(client, asset, offset_sec)
        if native:
            return native, "native"
        self._set(message="D1 nativo vazio — fallback agregando 1h…")
        hourly = self._fetch_hourly(client, asset, offset_sec)
        include_today = not _env_flag_default_on("OHLC_1D_CLOSED_ONLY", "1")
        agg = aggregate_hourly_to_daily(
            hourly,
            asset=asset,
            include_today=include_today,
        )
        return agg, "agg"

    def _pull_and_upsert(
        self,
        client: PocketOption,
        asset: str,
        *,
        offset_sec: int | None = None,
    ) -> int:
        if offset_sec is None:
            offset_sec = self._backfill_offset_sec(asset)
        daily, source = self._fetch_daily(client, asset, offset_sec)
        if not daily:
            raise RuntimeError(
                "Pocket devolveu 0 velas D1 — verifique SSID e ativo."
            )
        daily = filter_closed_daily(daily)
        if source == "native":
            for row in daily:
                row["source"] = "pocket"
        if not daily:
            return 0
        try:
            last = last_opened_at(asset, TIMEFRAME, table=TABLE_1D)
        except Exception:  # noqa: BLE001
            last = None
        if last is not None:
            cutoff = last - timedelta(days=2)
            filtered: list[dict[str, Any]] = []
            for row in daily:
                ts = _parse_opened_at(row.get("opened_at"))
                if ts is None or ts >= cutoff:
                    filtered.append(row)
            daily = filtered
        if not daily:
            return 0
        n = upsert_candles(daily, table=TABLE_1D)
        with self._lock:
            prev = int(self._snap["per_tf"].get(TIMEFRAME, {}).get("ok", 0) or 0)
            self._snap["per_tf"][TIMEFRAME] = {"ok": prev + n, "err": None}
            self._snap["last_upsert"] = n
            self._snap["total_upserted"] = int(
                self._snap.get("total_upserted", 0) or 0
            ) + n
            self._refresh_stored()
        return n

    def _full_pull(self, client: PocketOption, asset: str) -> int:
        offset = self._backfill_offset_sec(asset)
        return self._pull_and_upsert(client, asset, offset_sec=offset)

    def _arm_daily_wait(self) -> float:
        hour = _env_int("OHLC_1D_SYNC_HOUR", DEFAULT_SYNC_HOUR)
        minute = _env_int("OHLC_1D_SYNC_MINUTE", DEFAULT_SYNC_MINUTE)
        wait = seconds_until_next_pocket_daily_fetch(hour=hour, minute=minute)
        next_at = datetime.now(timezone.utc) + timedelta(seconds=wait)
        off = pocket_tz_offset()
        local_next = next_at + timedelta(seconds=off)
        self._set(
            phase="waiting",
            poll_mode="daily_pocket",
            next_fetch_at=next_at.isoformat(),
            sync_at_pocket=f"{hour:02d}:{minute:02d}",
            message=(
                f"Proximo sync as {hour:02d}:{minute:02d} Pocket "
                f"({local_next.strftime('%Y-%m-%d %H:%M')} local) "
                f"— em ~{int(wait // 3600)}h {int((wait % 3600) // 60)}min"
            ),
        )
        return wait

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

    def _run(self) -> None:
        asset = self._asset
        client: PocketOption | None = None
        try:
            self._set(phase="connect", message=f"Conectando Pocket ({asset})…")
            client = self._connect()
            self._refresh_stored()
            self._set(
                phase="backfill",
                message="1a puxada (D1 nativo Pocket)…",
                next_fetch_at=None,
            )
            try:
                n = self._full_pull(client, asset)
                self._set(
                    message=(
                        f"Sync D1: {n} velas novas/atualizadas"
                        if n > 0
                        else "Sync D1: 0 velas — verifique SSID/ativo"
                    )
                )
            except Exception as exc:  # noqa: BLE001
                with self._lock:
                    self._snap["per_tf"][TIMEFRAME] = {
                        "ok": 0,
                        "err": str(exc)[:200],
                    }
                self._set(message=f"Sync D1 falhou: {exc}")

            self._close_client(client)
            client = None

            while not self._stop.is_set():
                wait = self._arm_daily_wait()
                self._sleep_interruptible(wait)
                if self._stop.is_set():
                    break
                if not self._pull_lock.acquire(blocking=False):
                    self._set(message="Puxada manual em andamento — pulando ciclo…")
                    continue
                try:
                    self._set(
                        phase="fetch",
                        message="Sync diario (00:05 Pocket) — D1 nativo…",
                        next_fetch_at=None,
                    )
                    client = self._connect()
                    try:
                        n = self._pull_and_upsert(client, asset)
                        self._set(message=f"Sync D1: {n} velas upsert")
                    except Exception as exc:  # noqa: BLE001
                        with self._lock:
                            cur = self._snap["per_tf"].get(TIMEFRAME, {})
                            self._snap["per_tf"][TIMEFRAME] = {
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
                            self._pull_and_upsert(client, asset)
                        else:
                            self._set(message=f"Sync D1 falhou: {exc}")
                except Exception as exc:  # noqa: BLE001
                    self._set(message=f"Ciclo falhou: {exc}")
                finally:
                    self._close_client(client)
                    client = None
                    try:
                        self._pull_lock.release()
                    except RuntimeError:
                        pass
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


collector_1d = OhlcCollector1d()
