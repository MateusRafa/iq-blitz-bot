"""Coletor OHLC Pocket → Supabase (ferramenta separada do bot).

Ativo fixo (escolhido na UI). Apenas timeframe 1h.
"""

from __future__ import annotations

import os
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Any

from BinaryOptionsToolsV2.pocketoption import PocketOption

from bot.ohlc_store import supabase_ok, upsert_candles
from bot.runner import is_connection_error, load_ssid, normalize_asset

# label UI → segundos da vela (somente 1h)
TIMEFRAMES: dict[str, int] = {
    "1h": 3600,
}

# Quanto historico pedir no backfill inicial (segundos de offset).
BACKFILL_OFFSET: dict[str, int] = {
    "1h": 30 * 86400,  # ~30 dias
}

# No loop horario: velas recentes (offset em segundos).
LIVE_OFFSET: dict[str, int] = {
    "1h": 3600 * 12,  # ~12 velas
}


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


def seconds_until_next_hourly_fetch(*, after_hour_seconds: int) -> float:
    """Segundos ate o proximo fetch alinhado a hora UTC + margem."""
    now = datetime.now(timezone.utc)
    next_hour = (now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1))
    target = next_hour + timedelta(seconds=max(after_hour_seconds, 0))
    wait = (target - now).total_seconds()
    return max(wait, 1.0)


def _default_asset() -> str:
    return normalize_asset(
        os.environ.get("OHLC_ASSET", "").strip()
        or os.environ.get("POCKET_ASSET", "EURUSD_otc")
    )


def _candle_time_unix(raw: dict[str, Any]) -> int | None:
    for key in ("time", "timestamp", "t", "from", "open_time"):
        if key not in raw:
            continue
        v = raw[key]
        try:
            ts = int(float(v))
        except (TypeError, ValueError):
            continue
        # ms → s
        if ts > 10_000_000_000:
            ts //= 1000
        return ts
    return None


def _f(raw: dict[str, Any], *keys: str) -> float | None:
    for k in keys:
        if k not in raw:
            continue
        try:
            return float(raw[k])
        except (TypeError, ValueError):
            continue
    return None


def normalize_candle(
    raw: dict[str, Any], *, asset: str, timeframe: str
) -> dict[str, Any] | None:
    ts = _candle_time_unix(raw)
    o = _f(raw, "open", "Open", "o")
    h = _f(raw, "high", "High", "h", "max")
    lo = _f(raw, "low", "Low", "l", "min")
    c = _f(raw, "close", "Close", "c")
    if ts is None or o is None or h is None or lo is None or c is None:
        return None
    vol = _f(raw, "volume", "Volume", "v")
    opened = datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
    row: dict[str, Any] = {
        "asset": asset,
        "timeframe": timeframe,
        "opened_at": opened,
        "open": o,
        "high": h,
        "low": lo,
        "close": c,
        "source": "pocket",
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    if vol is not None:
        row["volume"] = vol
    return row


class OhlcCollector:
    """Thread: connect Pocket → backfill → loop get_candles → upsert Supabase."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._asset = _default_asset()
        self._snap: dict[str, Any] = {
            "running": False,
            "asset": self._asset,
            "timeframes": list(TIMEFRAMES.keys()),
            "supabase_ok": False,
            "supabase_msg": "",
            "phase": "idle",
            "last_upsert": 0,
            "total_upserted": 0,
            "next_fetch_at": None,
            "poll_mode": "hourly",
            "per_tf": {tf: {"ok": 0, "err": None} for tf in TIMEFRAMES},
            "error": None,
            "updated_at": None,
            "message": "Stand-by",
        }
        self._refresh_supabase_flag()

    def _refresh_supabase_flag(self) -> None:
        ok, msg = supabase_ok()
        self._snap["supabase_ok"] = ok
        self._snap["supabase_msg"] = msg

    def is_running(self) -> bool:
        t = self._thread
        return t is not None and t.is_alive()

    def status(self) -> dict[str, Any]:
        with self._lock:
            self._refresh_supabase_flag()
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
                raise RuntimeError(
                    "Pare o coletor antes de trocar o ativo."
                )
            self._asset = a
            self._snap["asset"] = a
            self._snap["message"] = f"Ativo definido: {a}"
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
                    "message": "Iniciando coletor…",
                    "total_upserted": 0,
                    "per_tf": {
                        tf: {"ok": 0, "err": None} for tf in TIMEFRAMES
                    },
                }
            )
            self._thread = threading.Thread(
                target=self._run,
                name="ohlc-collector",
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

    def _set(self, **kwargs: Any) -> None:
        with self._lock:
            self._snap.update(kwargs)
            self._snap["updated_at"] = datetime.now(timezone.utc).isoformat()

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
        self, client: PocketOption, asset: str, tf: str, offset: int
    ) -> int:
        rows = self._fetch_tf(client, asset, tf, offset)
        if not rows:
            return 0
        n = upsert_candles(rows)
        with self._lock:
            prev = int(self._snap["per_tf"].get(tf, {}).get("ok", 0) or 0)
            self._snap["per_tf"][tf] = {"ok": prev + n, "err": None}
            self._snap["last_upsert"] = n
            self._snap["total_upserted"] = int(
                self._snap.get("total_upserted", 0) or 0
            ) + n
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
        """Espera ate a proxima coleta (alinhada a hora UTC por padrao)."""
        align = _env_flag_default_on("OHLC_ALIGN_HOUR", "1")
        after = _env_int("OHLC_AFTER_HOUR_SECONDS", 120)
        if align:
            wait = seconds_until_next_hourly_fetch(after_hour_seconds=after)
            mode = "hourly"
        else:
            wait = float(max(_env_int("OHLC_POLL_SECONDS", 3600), 60))
            mode = "poll"
        next_at = datetime.now(timezone.utc) + timedelta(seconds=wait)
        self._set(
            phase="waiting",
            poll_mode=mode,
            next_fetch_at=next_at.isoformat(),
            message=(
                f"Proximo fetch em ~{int(wait // 60)} min "
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
            self._set(phase="backfill", message="Backfill historico…")
            for tf in TIMEFRAMES:
                if self._stop.is_set():
                    break
                try:
                    n = self._upsert_tf(
                        client, asset, tf, BACKFILL_OFFSET[tf]
                    )
                    self._set(
                        message=f"Backfill {tf}: {n} velas",
                    )
                except Exception as exc:  # noqa: BLE001
                    with self._lock:
                        self._snap["per_tf"][tf] = {
                            "ok": 0,
                            "err": str(exc)[:200],
                        }
                    self._set(message=f"Backfill {tf} falhou: {exc}")

            # Fecha apos backfill; reconecta a cada ciclo horario.
            self._close_client(client)
            client = None

            while not self._stop.is_set():
                self._wait_for_next_cycle()
                if self._stop.is_set():
                    break
                try:
                    self._set(
                        phase="fetch",
                        message=f"Buscando 1h ({asset})…",
                        next_fetch_at=None,
                    )
                    client = self._connect()
                    for tf in TIMEFRAMES:
                        if self._stop.is_set():
                            break
                        try:
                            n = self._upsert_tf(
                                client, asset, tf, LIVE_OFFSET[tf]
                            )
                            self._set(message=f"Fetch {tf}: {n} velas")
                        except Exception as exc:  # noqa: BLE001
                            with self._lock:
                                cur = self._snap["per_tf"].get(tf, {})
                                self._snap["per_tf"][tf] = {
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
                                    client, asset, tf, LIVE_OFFSET[tf]
                                )
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
                if not self._stop.is_set() and self._snap.get("phase") != "error":
                    self._snap["phase"] = "idle"
                    self._snap["message"] = "Parado"
                self._snap["running"] = False
                self._snap["next_fetch_at"] = None


collector = OhlcCollector()
