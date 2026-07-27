"""Coletor OHLC Pocket → Supabase (ferramenta 1m).

Ativo fixo (UI). Timeframe 1m. Tabela ohlc_candles_1m.
Retencao 90 dias; aviso 1 dia antes na UI.
"""

from __future__ import annotations

import os
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Any

from BinaryOptionsToolsV2.pocketoption import PocketOption

from bot.ohlc_collector import normalize_candle
from bot.ohlc_store import (
    TABLE_1M,
    delete_candles_since,
    fetch_candles,
    last_opened_at,
    run_retention_cleanup_1m,
    retention_status_1m,
    stored_summary,
    supabase_ok,
    upsert_candles,
)
from bot.runner import is_connection_error, load_ssid, normalize_asset

TIMEFRAMES: dict[str, int] = {
    "1m": 60,
}

# Historico inicial (~7 dias de 1m).
BACKFILL_OFFSET: dict[str, int] = {
    "1m": 7 * 86400,
}

# No loop: janela ampla para tapar buracos apos queda/reconexao.
LIVE_OFFSET: dict[str, int] = {
    "1m": 3600 * 6,
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


def seconds_until_next_minute_fetch(*, after_minute_seconds: int) -> float:
    """Segundos ate o proximo fetch alinhado ao minuto UTC + margem."""
    now = datetime.now(timezone.utc)
    next_min = now.replace(second=0, microsecond=0) + timedelta(minutes=1)
    target = next_min + timedelta(seconds=max(after_minute_seconds, 0))
    wait = (target - now).total_seconds()
    return max(wait, 1.0)


def _default_asset() -> str:
    return normalize_asset(
        os.environ.get("OHLC_1M_ASSET", "").strip()
        or os.environ.get("OHLC_ASSET", "").strip()
        or os.environ.get("POCKET_ASSET", "EURUSD_otc")
    )


class OhlcCollector1m:
    """Thread: Pocket → backfill 1m → loop → upsert Supabase (tabela 1m)."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._asset = _default_asset()
        self._snap: dict[str, Any] = {
            "running": False,
            "asset": self._asset,
            "timeframes": list(TIMEFRAMES.keys()),
            "timeframe": "1m",
            "table": TABLE_1M,
            "supabase_ok": False,
            "supabase_msg": "",
            "phase": "idle",
            "last_upsert": 0,
            "total_upserted": 0,
            "next_fetch_at": None,
            "poll_mode": "minutely",
            "per_tf": {tf: {"ok": 0, "err": None} for tf in TIMEFRAMES},
            "error": None,
            "updated_at": None,
            "message": "Stand-by",
            "stored_count": None,
            "stored_last": None,
            "stored_err": None,
            "retention": None,
        }
        self._refresh_supabase_flag()
        self._refresh_stored()

    def _refresh_supabase_flag(self) -> None:
        ok, msg = supabase_ok()
        self._snap["supabase_ok"] = ok
        self._snap["supabase_msg"] = msg

    def _refresh_stored(self) -> None:
        summary = stored_summary(self._asset, "1m", table=TABLE_1M)
        self._snap["stored_count"] = summary.get("stored_count")
        self._snap["stored_last"] = summary.get("stored_last")
        self._snap["stored_err"] = summary.get("stored_err")
        try:
            self._snap["retention"] = retention_status_1m(self._asset)
        except Exception as exc:  # noqa: BLE001
            self._snap["retention"] = {"warn": False, "err": str(exc)[:200]}

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
                    "message": "Iniciando coletor 1m…",
                    "total_upserted": 0,
                    "per_tf": {
                        tf: {"ok": 0, "err": None} for tf in TIMEFRAMES
                    },
                }
            )
            self._thread = threading.Thread(
                target=self._run,
                name="ohlc-collector-1m",
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

    def resync_recent(self, minutes: int = 20) -> dict[str, Any]:
        """Apaga os ultimos N minutos no DB e repuxa da Pocket (upsert, sem duplicar)."""
        ok, msg = supabase_ok()
        if not ok:
            raise RuntimeError(msg)
        mins = max(1, min(int(minutes), 180))
        asset = self._asset
        now = datetime.now(timezone.utc)
        since = (now - timedelta(minutes=mins)).replace(
            second=0, microsecond=0
        )
        self._set(
            phase="resync",
            message=f"Re-sync: apagando ≥ {since.strftime('%H:%M')} UTC ({mins} min)…",
            error=None,
        )
        deleted = delete_candles_since(
            asset, since, timeframe="1m", table=TABLE_1M
        )
        client: PocketOption | None = None
        upserted = 0
        try:
            self._set(
                message=(
                    f"Re-sync: {deleted} apagadas — buscando Pocket "
                    f"({mins} min)…"
                )
            )
            client = self._connect()
            period = TIMEFRAMES["1m"]
            offset = mins * 60 + period * 10
            fetched = self._fetch_tf(client, asset, "1m", offset)
            cutoff = since - timedelta(seconds=period)
            keep: list[dict[str, Any]] = []
            for row in fetched:
                try:
                    ts = datetime.fromisoformat(
                        str(row["opened_at"]).replace("Z", "+00:00")
                    )
                except ValueError:
                    keep.append(row)
                    continue
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                if ts >= cutoff:
                    keep.append(row)
            if keep:
                upserted = upsert_candles(keep, table=TABLE_1M)
            with self._lock:
                self._snap["last_upsert"] = upserted
                self._snap["total_upserted"] = int(
                    self._snap.get("total_upserted", 0) or 0
                ) + upserted
                self._refresh_stored()
            was_running = self.is_running()
            self._set(
                phase="fetch" if was_running else "idle",
                message=(
                    f"Re-sync ok: apagadas {deleted}, upsert {upserted} "
                    f"(desde {since.strftime('%H:%M')} UTC)"
                ),
            )
        except Exception as exc:  # noqa: BLE001
            self._set(
                phase="error" if not self.is_running() else "fetch",
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
            "minutes": mins,
            "asset": asset,
        }
        return st

    def _set(self, **kwargs: Any) -> None:
        with self._lock:
            self._snap.update(kwargs)
            self._snap["updated_at"] = datetime.now(timezone.utc).isoformat()

    def _offset_for_tf(self, asset: str, tf: str, *, backfill: bool) -> int:
        period = TIMEFRAMES[tf]
        default = BACKFILL_OFFSET[tf] if backfill else LIVE_OFFSET[tf]
        try:
            last = last_opened_at(asset, tf, table=TABLE_1M)
        except Exception:  # noqa: BLE001
            return default
        if last is None:
            return default
        now = datetime.now(timezone.utc)
        # Sempre cobre do ultimo salvo ate agora + folga (tapa buracos).
        gap = int((now - last).total_seconds()) + period * 10
        if backfill:
            return max(gap, default, period * 10)
        # Live: no minimo 30 min, no maximo LIVE_OFFSET, mas nunca menos que o gap.
        return max(min(max(gap, period * 30), default), period * 5)

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
            last = last_opened_at(asset, tf, table=TABLE_1M)
        except Exception:  # noqa: BLE001
            last = None
        now = datetime.now(timezone.utc)
        period = TIMEFRAMES[tf]
        # Se o banco esta atrasado >2 min, NAO descarte o meio da janela —
        # precisamos regravar o buraco inteiro ate o presente.
        if last is not None:
            lag = (now - last).total_seconds()
            if lag > period * 2 or backfill:
                cutoff = last - timedelta(seconds=period * 5)
            else:
                cutoff = last - timedelta(seconds=period * 2)
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
        n = upsert_candles(rows, table=TABLE_1M)
        with self._lock:
            prev = int(self._snap["per_tf"].get(tf, {}).get("ok", 0) or 0)
            self._snap["per_tf"][tf] = {"ok": prev + n, "err": None}
            self._snap["last_upsert"] = n
            self._snap["total_upserted"] = int(
                self._snap.get("total_upserted", 0) or 0
            ) + n
            self._refresh_stored()
        return n

    def _catchup_if_lagging(
        self, client: PocketOption, asset: str
    ) -> None:
        """Se o ultimo candle esta velho, forca backfill da janela em atraso."""
        try:
            last = last_opened_at(asset, "1m", table=TABLE_1M)
        except Exception:  # noqa: BLE001
            return
        if last is None:
            return
        lag = (datetime.now(timezone.utc) - last).total_seconds()
        if lag <= 90:
            return
        self._set(
            message=f"Catch-up 1m: atraso de ~{int(lag)}s — repondo buracos…",
            phase="catchup",
        )
        self._upsert_tf(client, asset, "1m", backfill=True)

    def _repair_internal_gaps(
        self, client: PocketOption, asset: str
    ) -> None:
        """Detecta buracos no meio do historico recente e puxa de novo da Pocket."""
        try:
            rows = fetch_candles(
                asset, timeframe="1m", limit=360, table=TABLE_1M
            )
        except Exception:  # noqa: BLE001
            return
        if len(rows) < 3:
            return
        times: list[datetime] = []
        for r in rows:
            raw = r.get("opened_at")
            if not raw:
                continue
            try:
                ts = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
            except ValueError:
                continue
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            times.append(ts)
        if len(times) < 3:
            return
        times.sort()
        period = TIMEFRAMES["1m"]
        worst_gap = 0
        gap_from: datetime | None = None
        for i in range(1, len(times)):
            gap = int((times[i] - times[i - 1]).total_seconds())
            if gap > period * 2 and gap > worst_gap:
                worst_gap = gap
                gap_from = times[i - 1]
        if worst_gap <= period * 2 or gap_from is None:
            return
        # Offset: do inicio do buraco ate agora (+folga).
        now = datetime.now(timezone.utc)
        offset = int((now - gap_from).total_seconds()) + period * 5
        offset = max(offset, period * 30)
        self._set(
            message=(
                f"Reparando buraco 1m de ~{worst_gap // 60} min "
                f"(desde {gap_from.strftime('%H:%M')} UTC)…"
            ),
            phase="repair",
        )
        try:
            fetched = self._fetch_tf(client, asset, "1m", offset)
            if not fetched:
                return
            # Aceita tudo a partir do inicio do buraco (sem filtro no "last").
            cutoff = gap_from - timedelta(seconds=period)
            keep: list[dict[str, Any]] = []
            for row in fetched:
                try:
                    ts = datetime.fromisoformat(
                        str(row["opened_at"]).replace("Z", "+00:00")
                    )
                except ValueError:
                    keep.append(row)
                    continue
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                if ts >= cutoff:
                    keep.append(row)
            if keep:
                n = upsert_candles(keep, table=TABLE_1M)
                self._set(message=f"Buraco 1m reparado: {n} velas upsert")
                with self._lock:
                    self._refresh_stored()
        except Exception as exc:  # noqa: BLE001
            self._set(message=f"Reparo de buraco falhou: {exc}")

    def _maybe_cleanup(self, asset: str) -> None:
        try:
            result = run_retention_cleanup_1m(asset)
            deleted = int(result.get("deleted") or 0)
            if deleted > 0:
                self._set(
                    message=f"Limpeza 1m: {deleted} velas >90 dias removidas",
                    retention=result.get("retention"),
                )
            else:
                with self._lock:
                    self._snap["retention"] = result.get("retention")
        except Exception as exc:  # noqa: BLE001
            self._set(message=f"Limpeza 1m falhou: {exc}")

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
        # Padrao: poll a cada 30s (grafico atualiza mais rapido).
        # OHLC_1M_ALIGN_MINUTE=1 volta ao alinhamento por minuto UTC.
        align = os.environ.get("OHLC_1M_ALIGN_MINUTE", "0").strip().lower() not in (
            "0",
            "false",
            "no",
            "",
        )
        after = _env_int("OHLC_1M_AFTER_MINUTE_SECONDS", 5)
        if align:
            wait = seconds_until_next_minute_fetch(after_minute_seconds=after)
            mode = "minutely"
        else:
            wait = float(max(_env_int("OHLC_1M_POLL_SECONDS", 30), 15))
            mode = "poll"
        next_at = datetime.now(timezone.utc) + timedelta(seconds=wait)
        self._set(
            phase="waiting",
            poll_mode=mode,
            next_fetch_at=next_at.isoformat(),
            message=(
                f"Proximo fetch 1m em ~{int(wait)}s "
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
                    f"Sync incremental 1m ({stored} velas no Supabase)…"
                    if stored > 0
                    else "Backfill 1m (base vazia, ~7 dias)…"
                ),
            )
            for tf in TIMEFRAMES:
                if self._stop.is_set():
                    break
                try:
                    n = self._upsert_tf(client, asset, tf, backfill=True)
                    self._set(message=f"Sync {tf}: {n} velas novas/atualizadas")
                except Exception as exc:  # noqa: BLE001
                    with self._lock:
                        self._snap["per_tf"][tf] = {
                            "ok": 0,
                            "err": str(exc)[:200],
                        }
                    self._set(message=f"Sync {tf} falhou: {exc}")

            self._maybe_cleanup(asset)
            if client is not None:
                self._repair_internal_gaps(client, asset)
            # Mantem a conexao aberta no loop de 30s (reconecta so se cair).
            while not self._stop.is_set():
                self._wait_for_next_cycle()
                if self._stop.is_set():
                    break
                try:
                    self._set(
                        phase="fetch",
                        message=f"Buscando 1m novos ({asset})…",
                        next_fetch_at=None,
                    )
                    if client is None:
                        client = self._connect()
                    self._catchup_if_lagging(client, asset)
                    self._repair_internal_gaps(client, asset)
                    for tf in TIMEFRAMES:
                        if self._stop.is_set():
                            break
                        try:
                            n = self._upsert_tf(
                                client, asset, tf, backfill=False
                            )
                            self._set(
                                message=(
                                    f"Fetch {tf}: {n} velas "
                                    f"(desde ultimo salvo)"
                                )
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
                                client = None
                                time.sleep(2.0)
                                client = self._connect()
                                self._upsert_tf(
                                    client, asset, tf, backfill=False
                                )
                    self._repair_internal_gaps(client, asset)
                    # Limpeza so de vez em quando (a cada ~10 min de uptime).
                    if int(time.time()) % 600 < 35:
                        self._maybe_cleanup(asset)
                except Exception as exc:  # noqa: BLE001
                    self._set(message=f"Ciclo falhou: {exc}")
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


collector_1m = OhlcCollector1m()
