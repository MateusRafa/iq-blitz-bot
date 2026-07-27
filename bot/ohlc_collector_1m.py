"""Coletor OHLC Pocket → Supabase (ferramenta 1m).

Ativo fixo (UI). Timeframe 1m. Tabela ohlc_candles_1m.
Retencao 90 dias; aviso 1 dia antes na UI.
"""

from __future__ import annotations

import json
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
    sanitize_ohlc_spikes,
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


def _parse_row_opened(row: dict[str, Any]) -> datetime | None:
    raw = row.get("opened_at")
    if not raw:
        return None
    try:
        ts = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
    except ValueError:
        return None
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)


def filter_closed_candles(
    rows: list[dict[str, Any]],
    period: int,
    *,
    grace_seconds: int | None = None,
) -> list[dict[str, Any]]:
    """Mantem so velas ja fechadas (evita gravar o minuto em formacao).

    A vela que abriu em T fecha em T+period. So persiste se
    now >= T + period + grace (Pocket costuma fechar OHLC alguns segundos depois).
    """
    if not rows:
        return []
    grace = (
        grace_seconds
        if grace_seconds is not None
        else _env_int("OHLC_1M_CLOSE_GRACE_SECONDS", 8)
    )
    if not _env_flag_default_on("OHLC_1M_CLOSED_ONLY", "1"):
        return rows
    now = datetime.now(timezone.utc)
    keep: list[dict[str, Any]] = []
    for row in rows:
        ts = _parse_row_opened(row)
        if ts is None:
            continue
        closed_at = ts + timedelta(seconds=period + max(grace, 0))
        if now >= closed_at:
            keep.append(row)
    return keep


def _candle_body(row: dict[str, Any]) -> float:
    try:
        return abs(float(row["close"]) - float(row["open"]))
    except (TypeError, ValueError, KeyError):
        return 0.0


def _candle_range(row: dict[str, Any]) -> float:
    try:
        return float(row["high"]) - float(row["low"])
    except (TypeError, ValueError, KeyError):
        return 0.0


def _prefer_better_candle(
    a: dict[str, Any], b: dict[str, Any]
) -> dict[str, Any]:
    """Escolhe OHLC com corpo real (como na Pocket) em vez de traco O=C.

    Amplia high/low entre as duas fontes; open/close vêm da fonte com maior corpo.
    Ignora pavio de fonte bodyless se o range for absurdo vs a fonte com corpo.
    """
    body_a = _candle_body(a)
    body_b = _candle_body(b)
    # Prefere quem tem corpo; se empatar, maior range; se empatar, A (estavel).
    if body_b > body_a + 1e-12:
        base, other = b, a
    elif body_a > body_b + 1e-12:
        base, other = a, b
    elif _candle_range(b) > _candle_range(a) + 1e-12:
        base, other = b, a
    else:
        base, other = a, b
    out = dict(base)
    try:
        # Nao misturar pavio gigante de um "traco" O=C em cima de vela com corpo.
        other_body = _candle_body(other)
        other_range = _candle_range(other)
        base_range = max(_candle_range(base), 1e-12)
        if other_body <= 1e-12 and other_range > base_range * 3:
            return out
        out["high"] = max(float(base["high"]), float(other["high"]))
        out["low"] = min(float(base["low"]), float(other["low"]))
        out["high"] = max(out["high"], float(out["open"]), float(out["close"]))
        out["low"] = min(out["low"], float(out["open"]), float(out["close"]))
    except (TypeError, ValueError, KeyError):
        pass
    return out


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
            "auto_repair_flat": True,
            "last_flat_repair_at": None,
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
            keep = filter_closed_candles(keep, TIMEFRAMES["1m"])
            if keep:
                keep = sanitize_ohlc_spikes(keep)
                upserted = upsert_candles(keep, table=TABLE_1M) if keep else 0
            self._purge_forming_candle(asset, "1m")
            with self._lock:
                self._snap["last_upsert"] = upserted
                self._snap["total_upserted"] = int(
                    self._snap.get("total_upserted", 0) or 0
                ) + upserted
                self._snap["last_flat_repair_at"] = datetime.now(
                    timezone.utc
                ).isoformat()
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

    def _as_candle_list(self, raw: Any) -> list[dict[str, Any]]:
        if raw is None:
            return []
        if isinstance(raw, str):
            try:
                raw = json.loads(raw)
            except json.JSONDecodeError:
                return []
        if isinstance(raw, dict):
            for key in ("data", "candles", "history"):
                val = raw.get(key)
                if isinstance(val, list):
                    return [x for x in val if isinstance(x, dict)]
            return []
        if isinstance(raw, list):
            return [x for x in raw if isinstance(x, dict)]
        return []

    def _fetch_tf(
        self, client: PocketOption, asset: str, tf: str, offset: int
    ) -> list[dict[str, Any]]:
        """Historico limpo da Pocket — evita get_candles()/live (max_rows=100 + ticks).

        get_candles() na BinaryOptionsToolsV2 chama get_candles_live com max_rows=100
        e mescla ticks no OHLC, gerando pavios/flats errados. Aqui usamos
        get_candles_advanced + history + compile_candles (como a propria lib no seed).
        """
        period = TIMEFRAMES[tf]
        offset = int(offset)
        # Mesmo offset da lib (Pocket platform time ≈ UTC+2).
        platform_offset = _env_int("POCKET_TIME_OFFSET", 7200)
        platform_time = int(time.time()) + platform_offset
        groups: list[list[dict[str, Any]]] = []

        try:
            groups.append(
                self._as_candle_list(
                    client.get_candles_advanced(
                        asset, period, offset, platform_time
                    )
                )
            )
        except Exception:  # noqa: BLE001
            pass
        try:
            groups.append(self._as_candle_list(client.history(asset, period)))
        except Exception:  # noqa: BLE001
            pass
        try:
            groups.append(
                self._as_candle_list(
                    client.compile_candles(asset, period, offset)
                )
            )
        except Exception:  # noqa: BLE001
            pass

        # Fallback controlado: live com max_rows alto (ja vem com time ajustado).
        if not any(groups):
            hours = max(0.1, offset / 3600.0)
            max_rows = min(max(offset // period + 20, 300), 5000)
            it = None
            try:
                it = client.get_candles_live(
                    asset, period, hours=hours, max_rows=max_rows
                )
                closed, _forming = next(it)
                groups.append(self._as_candle_list(closed))
            except Exception:  # noqa: BLE001
                pass
            finally:
                # Encerra o generator para unsubscribe de ticks.
                if it is not None:
                    try:
                        it.close()  # type: ignore[attr-defined]
                    except Exception:  # noqa: BLE001
                        pass

        by_key: dict[str, dict[str, Any]] = {}
        for group in groups:
            # advanced/history/compile: aplicar offset. live fallback: time ja corrigido.
            # Heuristica: se o time bruto esta ~7200s a frente de agora, aplicar offset.
            for item in group:
                ts_raw = None
                for key in ("time", "timestamp", "t", "from", "open_time"):
                    if key in item:
                        try:
                            ts_raw = int(float(item[key]))
                            break
                        except (TypeError, ValueError):
                            continue
                use_offset = platform_offset
                if ts_raw is not None:
                    if ts_raw > 10_000_000_000:
                        ts_raw //= 1000
                    now_u = int(time.time())
                    # Se ja parece UTC (perto de agora), nao subtrai de novo.
                    if abs(ts_raw - now_u) < 3600 * 6:
                        use_offset = 0
                    elif ts_raw - now_u > 1800:
                        use_offset = platform_offset
                norm = normalize_candle(
                    item,
                    asset=asset,
                    timeframe=tf,
                    time_offset=use_offset,
                )
                if not norm:
                    continue
                key = str(norm["opened_at"])
                prev = by_key.get(key)
                if prev is None:
                    by_key[key] = norm
                    continue
                # Varias fontes no mesmo minuto: NAO sobrescrever um corpo bom
                # com um "traco" (O=C) de outra fonte — era o bug vs Pocket.
                by_key[key] = _prefer_better_candle(prev, norm)
        rows = list(by_key.values())
        rows.sort(key=lambda r: str(r.get("opened_at") or ""))
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
        # Nao grava a vela do minuto atual (OHLC incompleto = tracos / pavios errados).
        rows = filter_closed_candles(rows, period)
        if not rows:
            return 0
        rows = sanitize_ohlc_spikes(rows)
        rows = self._reconcile_prefer_body(rows, asset, tf)
        if not rows:
            return 0
        n = upsert_candles(rows, table=TABLE_1M)
        self._purge_forming_candle(asset, tf)
        with self._lock:
            prev = int(self._snap["per_tf"].get(tf, {}).get("ok", 0) or 0)
            self._snap["per_tf"][tf] = {"ok": prev + n, "err": None}
            self._snap["last_upsert"] = n
            self._snap["total_upserted"] = int(
                self._snap.get("total_upserted", 0) or 0
            ) + n
            self._refresh_stored()
        return n

    def _reconcile_prefer_body(
        self, rows: list[dict[str, Any]], asset: str, tf: str
    ) -> list[dict[str, Any]]:
        """Nao deixa upsert bodyless apagar vela com corpo ja salva (e vice-versa)."""
        if not rows:
            return []
        try:
            recent = fetch_candles(
                asset, timeframe=tf, limit=max(180, len(rows) + 30), table=TABLE_1M
            )
        except Exception:  # noqa: BLE001
            return rows
        by_old: dict[str, dict[str, Any]] = {}
        for r in recent:
            raw = r.get("opened_at")
            if raw:
                by_old[str(raw)[:19]] = {
                    "asset": asset,
                    "timeframe": tf,
                    "opened_at": r.get("opened_at"),
                    "open": r.get("open"),
                    "high": r.get("high"),
                    "low": r.get("low"),
                    "close": r.get("close"),
                    "source": "pocket",
                }
        out: list[dict[str, Any]] = []
        for row in rows:
            key = str(row.get("opened_at") or "")[:19]
            old = by_old.get(key)
            if old is None:
                out.append(row)
            else:
                out.append(_prefer_better_candle(old, row))
        return out

    def _purge_forming_candle(self, asset: str, tf: str) -> None:
        """Remove do DB o minuto ainda aberto (lixo de ticks incompletos)."""
        if not _env_flag_default_on("OHLC_1M_CLOSED_ONLY", "1"):
            return
        now_min = datetime.now(timezone.utc).replace(second=0, microsecond=0)
        try:
            delete_candles_since(
                asset, now_min, timeframe=tf, table=TABLE_1M
            )
        except Exception:  # noqa: BLE001
            pass

    def _trailing_flats_need_repair(self, asset: str) -> bool:
        """True se >=2 velas ja fechadas no fim estao flat (O=H=L=C)."""
        try:
            rows = fetch_candles(
                asset, timeframe="1m", limit=40, table=TABLE_1M
            )
        except Exception:  # noqa: BLE001
            return False
        if len(rows) < 5:
            return False
        now_min = datetime.now(timezone.utc).replace(
            second=0, microsecond=0
        )
        closed: list[dict[str, Any]] = []
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
            ts = ts.astimezone(timezone.utc).replace(second=0, microsecond=0)
            if ts < now_min:
                closed.append(r)
        tail = closed[-4:]
        if len(tail) < 2:
            return False
        flat_n = 0
        for r in tail:
            try:
                o = float(r["open"])
                h = float(r["high"])
                lo = float(r["low"])
                c = float(r["close"])
            except (TypeError, ValueError, KeyError):
                continue
            if max(h, o, c) - min(lo, o, c) <= 1e-12:
                flat_n += 1
        return flat_n >= 2

    def _maybe_repair_trailing_flats(
        self, client: PocketOption | None, asset: str
    ) -> None:
        """Flag auto_repair_flat: se recentes flat, apaga e repuxa da Pocket."""
        if not _env_flag_default_on("OHLC_1M_AUTO_REPAIR_FLAT", "1"):
            with self._lock:
                self._snap["auto_repair_flat"] = False
            return
        with self._lock:
            self._snap["auto_repair_flat"] = True
            last_at = self._snap.get("last_flat_repair_at")
        # Cooldown 2 min para nao loop de resync.
        if last_at:
            try:
                prev = datetime.fromisoformat(
                    str(last_at).replace("Z", "+00:00")
                )
                if prev.tzinfo is None:
                    prev = prev.replace(tzinfo=timezone.utc)
                if (datetime.now(timezone.utc) - prev).total_seconds() < 120:
                    return
            except ValueError:
                pass
        if not self._trailing_flats_need_repair(asset):
            return
        self._set(
            message="Flag flat: velas recentes invalidas — re-sync 15 min…",
            phase="repair_flat",
        )
        own_client = client is None
        use = client
        try:
            if use is None:
                use = self._connect()
            # Apaga so a ponta corrompida e repuxa (upsert + merge).
            since = (
                datetime.now(timezone.utc) - timedelta(minutes=15)
            ).replace(second=0, microsecond=0)
            deleted = delete_candles_since(
                asset, since, timeframe="1m", table=TABLE_1M
            )
            period = TIMEFRAMES["1m"]
            offset = 15 * 60 + period * 10
            fetched = self._fetch_tf(use, asset, "1m", offset)
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
            keep = filter_closed_candles(keep, period)
            keep = sanitize_ohlc_spikes(keep)
            n = upsert_candles(keep, table=TABLE_1M) if keep else 0
            self._purge_forming_candle(asset, "1m")
            with self._lock:
                self._snap["last_flat_repair_at"] = datetime.now(
                    timezone.utc
                ).isoformat()
                self._refresh_stored()
            self._set(
                message=(
                    f"Flag flat: reparado (apagadas {deleted}, upsert {n})"
                ),
                phase="fetch",
            )
        except Exception as exc:  # noqa: BLE001
            self._set(message=f"Flag flat: reparo falhou: {exc}")
        finally:
            if own_client:
                self._close_client(use)

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
            keep = filter_closed_candles(keep, period)
            if keep:
                keep = sanitize_ohlc_spikes(keep)
                n = upsert_candles(keep, table=TABLE_1M) if keep else 0
                self._purge_forming_candle(asset, "1m")
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
        # Padrao: 1 fetch por minuto UTC, ~8s apos o fechamento da vela.
        # (Poll a cada 30s no meio do minuto gravava OHLC incompleto = bug na ponta.)
        # OHLC_1M_ALIGN_MINUTE=0 volta ao poll por intervalo (OHLC_1M_POLL_SECONDS).
        align_raw = os.environ.get("OHLC_1M_ALIGN_MINUTE", "1").strip().lower()
        align = align_raw not in ("0", "false", "no")
        after = _env_int("OHLC_1M_AFTER_MINUTE_SECONDS", 8)
        if align:
            wait = seconds_until_next_minute_fetch(after_minute_seconds=after)
            mode = "minutely"
        else:
            wait = float(max(_env_int("OHLC_1M_POLL_SECONDS", 60), 30))
            mode = "poll"
        next_at = datetime.now(timezone.utc) + timedelta(seconds=wait)
        self._set(
            phase="waiting",
            poll_mode=mode,
            next_fetch_at=next_at.isoformat(),
            message=(
                f"Proximo fetch 1m em ~{int(wait)}s "
                f"({next_at.strftime('%H:%M:%S')} UTC, so velas fechadas)"
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
                self._maybe_repair_trailing_flats(client, asset)
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
                    self._maybe_repair_trailing_flats(client, asset)
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
