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

from bot.ohlc_store import (
    last_opened_at,
    merge_ohlc_with_existing,
    oldest_opened_at,
    stored_summary,
    supabase_ok,
    upsert_candles,
)
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

# Durante a hora: atualiza a vela em formacao (e as recentes) periodicamente.
# Sem isso o grafico fica preso no OHLC de ~2 min apos a virada da hora,
# enquanto a Pocket continua desenhando pavios/corpo ao vivo.
DEFAULT_LIVE_POLL_SECONDS = 60



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
    raw: dict[str, Any],
    *,
    asset: str,
    timeframe: str,
    time_offset: int = 0,
) -> dict[str, Any] | None:
    ts = _candle_time_unix(raw)
    o = _f(raw, "open", "Open", "o")
    h = _f(raw, "high", "High", "h", "max")
    lo = _f(raw, "low", "Low", "l", "min")
    c = _f(raw, "close", "Close", "c")
    if ts is None or o is None or h is None or lo is None or c is None:
        return None
    # Pocket (BinaryOptionsToolsV2) usa timestamps com offset de plataforma (~7200s).
    if time_offset:
        ts -= int(time_offset)
    # Sanitiza OHLC (evita pavios inconsistentes / outliers de API).
    h = max(h, o, c)
    lo = min(lo, o, c)
    # Timeframe 1m: alinha opened_at ao minuto UTC.
    if timeframe == "1m":
        ts = (ts // 60) * 60
    elif timeframe == "1h":
        ts = (ts // 3600) * 3600
    elif timeframe == "1d":
        # D1 nativo: timestamp da API ja e a abertura do dia (fuso Pocket).
        pass
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
        """Le contagem/ultimo candle do Supabase (independente da sessao)."""
        summary = stored_summary(self._asset, "1h")
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
                raise RuntimeError(
                    "Pare o coletor antes de trocar o ativo."
                )
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

    def pull_now(self, asset: str | None = None) -> dict[str, Any]:
        """Puxada manual 1h (backfill incremental) → ohlc_candles."""
        ok, msg = supabase_ok()
        if not ok:
            raise RuntimeError(msg)
        client: PocketOption | None = None
        upserted = 0
        target = normalize_asset(asset) if asset else self._asset
        try:
            self._set(
                phase="manual_pull",
                message=f"Puxada manual 1h ({target})…",
                error=None,
                next_fetch_at=None,
            )
            client = self._connect()
            upserted = self._upsert_tf(client, target, "1h", backfill=True)
            was_running = self.is_running()
            self._set(
                phase="waiting" if was_running else "idle",
                message=f"Puxada manual ok: {upserted} velas ({target})",
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
        st["pull"] = {"upserted": upserted, "asset": target}
        return st

    def pull_history(
        self,
        asset: str | None = None,
        *,
        days: int = 600,
    ) -> dict[str, Any]:
        """Backfill profundo paginado via get_candles_advanced.

        A UI Pocket mostra OTC ate 2024; get_candles simples so devolve uma
        janela curta. Aqui paginamos para tras ate `days` (ou ate a API
        parar de devolver velas novas), gravando cada pagina no Supabase.
        """
        ok, msg = supabase_ok()
        if not ok:
            raise RuntimeError(msg)
        ndays = max(1, min(int(days), 800))
        client: PocketOption | None = None
        target = normalize_asset(asset) if asset else self._asset
        upserted = 0
        fetched = 0
        pages = 0
        oldest = None
        newest = None
        try:
            self._set(
                phase="history_pull",
                message=f"Historico OTC Pocket ~{ndays}d paginado ({target})…",
                error=None,
                next_fetch_at=None,
            )
            client = self._connect()
            rows, pages = self._fetch_history_paged(
                client, target, "1h", days=ndays
            )
            fetched = len(rows)
            if rows:
                times: list[datetime] = []
                for row in rows:
                    try:
                        ts = datetime.fromisoformat(
                            str(row["opened_at"]).replace("Z", "+00:00")
                        )
                        times.append(ts)
                    except ValueError:
                        continue
                if times:
                    oldest = min(times).isoformat()
                    newest = max(times).isoformat()
                rows = merge_ohlc_with_existing(
                    rows,
                    asset=target,
                    timeframe="1h",
                    lookback=min(500, max(48, len(rows))),
                )
                upserted = upsert_candles(rows)
            was_running = self.is_running()
            self._set(
                phase="waiting" if was_running else "idle",
                message=(
                    f"Historico OTC ok: {upserted}/{fetched} velas "
                    f"em {pages} paginas "
                    f"({oldest or '?'} → {newest or '?'})"
                ),
            )
            with self._lock:
                prev = int(self._snap["per_tf"].get("1h", {}).get("ok", 0) or 0)
                self._snap["per_tf"]["1h"] = {
                    "ok": prev + upserted,
                    "err": None,
                }
                self._snap["last_upsert"] = upserted
                self._snap["total_upserted"] = int(
                    self._snap.get("total_upserted", 0) or 0
                ) + upserted
                self._refresh_stored()
        except Exception as exc:  # noqa: BLE001
            self._set(
                phase="error" if not self.is_running() else "waiting",
                error=str(exc),
                message=f"Historico OTC falhou: {exc}",
            )
            raise
        finally:
            self._close_client(client)
        st = self.status()
        st["pull"] = {
            "mode": "history",
            "upserted": upserted,
            "fetched": fetched,
            "pages": pages,
            "days": ndays,
            "asset": target,
            "oldest": oldest,
            "newest": newest,
            "stored_oldest": None,
        }
        try:
            old = oldest_opened_at(target, "1h")
            if old is not None:
                st["pull"]["stored_oldest"] = old.isoformat()
        except Exception:  # noqa: BLE001
            pass
        return st

    def _fetch_history_paged(
        self,
        client: PocketOption,
        asset: str,
        tf: str,
        *,
        days: int,
    ) -> tuple[list[dict[str, Any]], int]:
        """Pagina para tras com get_candles_advanced(asset, period, offset, time)."""
        period = TIMEFRAMES[tf]
        chunk_days = max(3, min(_env_int("OHLC_OTC_HISTORY_CHUNK_DAYS", 14), 30))
        chunk_sec = chunk_days * 86400
        target_start = datetime.now(timezone.utc) - timedelta(days=days)
        target_ts = int(target_start.timestamp())
        ref = int(datetime.now(timezone.utc).timestamp())
        by_key: dict[str, dict[str, Any]] = {}
        pages = 0
        max_pages = max(30, (days // chunk_days) + 10)
        stagnant = 0

        while pages < max_pages and ref > target_ts:
            pages += 1
            self._set(
                message=(
                    f"Historico OTC pagina {pages}/{max_pages} "
                    f"(~{chunk_days}d, ref={datetime.fromtimestamp(ref, tz=timezone.utc).date()})…"
                )
            )
            raw: list[Any] = []
            try:
                if hasattr(client, "get_candles_advanced"):
                    raw = client.get_candles_advanced(
                        asset, period, int(chunk_sec), int(ref)
                    )
                else:
                    raw = client.get_candles(asset, period, int(chunk_sec))
            except Exception as exc:  # noqa: BLE001
                if pages == 1:
                    # Fallback: um get_candles grande.
                    try:
                        raw = client.get_candles(
                            asset, period, int(days * 86400)
                        )
                    except Exception as exc2:  # noqa: BLE001
                        raise RuntimeError(
                            f"Pocket historico falhou: {exc2}"
                        ) from exc2
                else:
                    self._set(message=f"Pagina {pages} falhou ({exc}); parando.")
                    break
            if not isinstance(raw, list) or not raw:
                break

            before = len(by_key)
            page_times: list[int] = []
            page_rows: list[dict[str, Any]] = []
            for item in raw:
                if not isinstance(item, dict):
                    continue
                norm = normalize_candle(item, asset=asset, timeframe=tf)
                if not norm:
                    continue
                key = str(norm["opened_at"])[:19]
                by_key[key] = norm
                page_rows.append(norm)
                try:
                    ts = datetime.fromisoformat(
                        str(norm["opened_at"]).replace("Z", "+00:00")
                    )
                    page_times.append(int(ts.timestamp()))
                except ValueError:
                    continue

            # Grava a pagina ja (progresso se o HTTP do Railway cortar).
            if page_rows:
                try:
                    upsert_candles(page_rows)
                except Exception as exc:  # noqa: BLE001
                    self._set(message=f"Upsert pagina {pages}: {exc}")

            added = len(by_key) - before
            if added <= 0 or not page_times:
                stagnant += 1
                if stagnant >= 2:
                    break
            else:
                stagnant = 0

            oldest_page = min(page_times)
            if oldest_page <= target_ts:
                break
            if oldest_page >= ref:
                # Sem progresso no eixo do tempo.
                stagnant += 1
                if stagnant >= 2:
                    break
                ref = ref - chunk_sec
            else:
                # Proxima pagina: imediatamente antes da vela mais antiga.
                ref = oldest_page - 1

            time.sleep(0.35)

        rows = sorted(by_key.values(), key=lambda r: str(r.get("opened_at") or ""))
        return rows, pages

    def _set(self, **kwargs: Any) -> None:
        with self._lock:
            self._snap.update(kwargs)
            self._snap["updated_at"] = datetime.now(timezone.utc).isoformat()

    def _offset_for_tf(self, asset: str, tf: str, *, backfill: bool) -> int:
        """Offset em segundos: do ultimo salvo ate agora (+folga), ou historico cheio."""
        period = TIMEFRAMES[tf]
        default = BACKFILL_OFFSET[tf] if backfill else LIVE_OFFSET[tf]
        try:
            last = last_opened_at(asset, tf)
        except Exception:  # noqa: BLE001
            return default
        if last is None:
            return default
        now = datetime.now(timezone.utc)
        # Folga: 3 velas para regravar a ultima (pode estar incompleta) + margem
        gap = int((now - last).total_seconds()) + period * 3
        # Nunca pedir menos que LIVE; no backfill, se gap pequeno, so incremental
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
        # Se ja ha historico, so envia velas >= (ultimo - 1 periodo) para nao
        # reenviar 30 dias a cada start — upsert ainda e idempotente.
        try:
            last = last_opened_at(asset, tf)
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
        # Preserva pavios ja salvos (high=max, low=min) — evita tick incompleto
        # da API apagar o low/high que a Pocket ja mostrou no grafico.
        rows = merge_ohlc_with_existing(
            rows, asset=asset, timeframe=tf, lookback=max(48, len(rows) + 12)
        )
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
        """Espera ate a proxima coleta (alinhada a hora UTC por padrao).

        Se OHLC_LIVE_POLL_SECONDS > 0, durante a espera faz fetches curtos
        para a vela em formacao acompanhar a Pocket (pavios/corpo).
        """
        align = _env_flag_default_on("OHLC_ALIGN_HOUR", "1")
        after = _env_int("OHLC_AFTER_HOUR_SECONDS", 120)
        live_every = _env_int(
            "OHLC_LIVE_POLL_SECONDS", DEFAULT_LIVE_POLL_SECONDS
        )
        if align:
            wait = seconds_until_next_hourly_fetch(after_hour_seconds=after)
            mode = "hourly+live" if live_every > 0 else "hourly"
        else:
            wait = float(max(_env_int("OHLC_POLL_SECONDS", 3600), 60))
            mode = "poll"
            live_every = 0
        next_at = datetime.now(timezone.utc) + timedelta(seconds=wait)
        self._set(
            phase="waiting",
            poll_mode=mode,
            next_fetch_at=next_at.isoformat(),
            message=(
                f"Proximo fetch horario em ~{int(wait // 60)} min "
                f"({next_at.strftime('%H:%M:%S')} UTC)"
                + (
                    f" · live a cada {live_every}s"
                    if live_every > 0
                    else ""
                )
            ),
        )
        if live_every <= 0 or not align:
            self._sleep_interruptible(wait)
            return

        # Live poll: reconecta periodicamente e atualiza as ultimas velas.
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
            client: PocketOption | None = None
            try:
                self._set(
                    phase="live",
                    message=f"Live 1h ({asset}) — atualizando vela em formação…",
                )
                client = self._connect()
                n = self._upsert_tf(client, asset, "1h", backfill=False)
                left2 = max(0.0, end - time.monotonic())
                next_at2 = datetime.now(timezone.utc) + timedelta(seconds=left2)
                self._set(
                    phase="waiting",
                    next_fetch_at=next_at2.isoformat(),
                    message=(
                        f"Live ok ({n} velas) · próximo horário "
                        f"{next_at.strftime('%H:%M:%S')} UTC · "
                        f"live a cada {live_every}s"
                    ),
                )
            except Exception as exc:  # noqa: BLE001
                self._set(
                    phase="waiting",
                    message=f"Live falhou ({exc}); segue até o fetch horário",
                )
            finally:
                self._close_client(client)

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
                    f"Sync incremental ({stored} velas ja no Supabase)…"
                    if stored > 0
                    else "Backfill historico (base vazia)…"
                ),
            )
            for tf in TIMEFRAMES:
                if self._stop.is_set():
                    break
                try:
                    n = self._upsert_tf(
                        client, asset, tf, backfill=True
                    )
                    self._set(
                        message=f"Sync {tf}: {n} velas novas/atualizadas",
                    )
                except Exception as exc:  # noqa: BLE001
                    with self._lock:
                        self._snap["per_tf"][tf] = {
                            "ok": 0,
                            "err": str(exc)[:200],
                        }
                    self._set(message=f"Sync {tf} falhou: {exc}")

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
                        message=f"Buscando 1h novos ({asset})…",
                        next_fetch_at=None,
                    )
                    client = self._connect()
                    for tf in TIMEFRAMES:
                        if self._stop.is_set():
                            break
                        try:
                            n = self._upsert_tf(
                                client, asset, tf, backfill=False
                            )
                            self._set(
                                message=f"Fetch {tf}: {n} velas (desde ultimo salvo)"
                            )
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
                                    client, asset, tf, backfill=False
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
                self._refresh_stored()
                if not self._stop.is_set() and self._snap.get("phase") != "error":
                    self._snap["phase"] = "idle"
                    self._snap["message"] = "Parado"
                self._snap["running"] = False
                self._snap["next_fetch_at"] = None


collector = OhlcCollector()
