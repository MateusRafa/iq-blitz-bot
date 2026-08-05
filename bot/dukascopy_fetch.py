"""Baixa ticks Dukascopy (bi5) e agrega em velas 1h OHLC (UTC / Bid)."""

from __future__ import annotations

import logging
import lzma
import os
import struct
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from typing import Any

logger = logging.getLogger(__name__)

DATAFEED = "https://datafeed.dukascopy.com/datafeed"
# EURUSD: precos em bi5 = valor * 100000
_POINT = 100_000
_TICK_FMT = ">IIIff"  # ms, ask, bid, askVol, bidVol
_TICK_SIZE = struct.calcsize(_TICK_FMT)


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _hour_url(symbol: str, hour_utc: datetime) -> str:
    # Dukascopy usa mes 0-indexed no path.
    y = hour_utc.year
    m = hour_utc.month - 1
    d = hour_utc.day
    h = hour_utc.hour
    return f"{DATAFEED}/{symbol}/{y}/{m:02d}/{d:02d}/{h:02d}h_ticks.bi5"


def _download_bi5(
    url: str,
    *,
    timeout: float = 30.0,
    retries: int = 3,
) -> bytes | None:
    last_err: Exception | None = None
    for attempt in range(max(1, retries)):
        req = urllib.request.Request(
            url,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (compatible; iq-blitz-bot/1.1; "
                    "+https://railway.app)"
                ),
                "Accept": "*/*",
                "Accept-Encoding": "identity",
            },
        )
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                data = resp.read()
                return data or None
        except urllib.error.HTTPError as exc:
            if exc.code in (404, 204):
                return None
            last_err = RuntimeError(f"Dukascopy HTTP {exc.code}: {url}")
            # 403/429/5xx: tenta de novo
            if exc.code not in (403, 408, 425, 429, 500, 502, 503, 504):
                raise last_err from exc
            # 503: datafeed saturado — espera mais antes de retry.
            if exc.code == 503:
                time.sleep(1.2 * (attempt + 1))
                continue
        except urllib.error.URLError as exc:
            last_err = RuntimeError(f"Dukascopy rede: {exc.reason}")
        except TimeoutError as exc:
            last_err = RuntimeError(f"Dukascopy timeout: {url}")
            last_err.__cause__ = exc
        if attempt + 1 < retries:
            time.sleep(0.5 * (attempt + 1))
    if last_err is not None:
        raise last_err
    return None


def _ticks_from_bi5(payload: bytes) -> list[tuple[int, float, float]]:
    """Retorna lista (ms_offset, bid, ask)."""
    if not payload:
        return []
    try:
        raw = lzma.decompress(payload)
    except lzma.LZMAError as exc:
        raise RuntimeError(f"Falha ao descomprimir bi5: {exc}") from exc
    n = len(raw) // _TICK_SIZE
    out: list[tuple[int, float, float]] = []
    for i in range(n):
        chunk = raw[i * _TICK_SIZE : (i + 1) * _TICK_SIZE]
        ms, ask_i, bid_i, _av, _bv = struct.unpack(_TICK_FMT, chunk)
        out.append((ms, bid_i / _POINT, ask_i / _POINT))
    return out


def _ohlc_from_ticks(
    hour_utc: datetime,
    ticks: list[tuple[int, float, float]],
    *,
    side: str = "bid",
) -> dict[str, Any] | None:
    if not ticks:
        return None
    prices = [t[1] if side == "bid" else t[2] for t in ticks]
    o = prices[0]
    c = prices[-1]
    h = max(prices)
    lo = min(prices)
    opened = hour_utc.replace(minute=0, second=0, microsecond=0, tzinfo=timezone.utc)
    return {
        "opened_at": opened.isoformat(),
        "open": o,
        "high": h,
        "low": lo,
        "close": c,
        "volume": float(len(ticks)),
    }


def _fetch_one_hour(
    symbol: str,
    hour_utc: datetime,
    *,
    side: str,
    timeout: float,
    retries: int,
) -> dict[str, Any] | None:
    url = _hour_url(symbol, hour_utc)
    payload = _download_bi5(url, timeout=timeout, retries=retries)
    if payload is None:
        return None
    ticks = _ticks_from_bi5(payload)
    return _ohlc_from_ticks(hour_utc, ticks, side=side)


def fetch_eurusd_1h(
    start: datetime,
    end: datetime | None = None,
    *,
    symbol: str | None = None,
    side: str = "bid",
    max_workers: int | None = None,
    timeout: float | None = None,
    include_current_hour: bool = True,
) -> list[dict[str, Any]]:
    """Baixa e agrega velas 1h Bid (UTC) para o intervalo [start, end].

    Por padrao inclui a hora corrente (arquivo bi5 parcial da Dukascopy),
    para o grafico nao ficar 1h atrasado.
    """
    sym = (symbol or os.environ.get("DUKASCOPY_SYMBOL", "EURUSD")).strip().upper()
    end_dt = end or datetime.now(timezone.utc)
    if start.tzinfo is None:
        start = start.replace(tzinfo=timezone.utc)
    else:
        start = start.astimezone(timezone.utc)
    if end_dt.tzinfo is None:
        end_dt = end_dt.replace(tzinfo=timezone.utc)
    else:
        end_dt = end_dt.astimezone(timezone.utc)

    start_h = start.replace(minute=0, second=0, microsecond=0)
    end_h = end_dt.replace(minute=0, second=0, microsecond=0)
    if include_current_hour:
        end_exclusive = end_h + timedelta(hours=1)
    else:
        end_exclusive = end_h
    if end_exclusive <= start_h:
        return []

    hours: list[datetime] = []
    cur = start_h
    while cur < end_exclusive:
        hours.append(cur)
        cur += timedelta(hours=1)

    workers = max(1, min(max_workers or _env_int("DUKASCOPY_WORKERS", 4), 8))
    to = float(timeout if timeout is not None else _env_int("DUKASCOPY_TIMEOUT", 25))
    retries = max(1, _env_int("DUKASCOPY_RETRIES", 5))
    offer = "bid" if side.lower() != "ask" else "ask"

    rows: list[dict[str, Any]] = []
    errors: list[str] = []
    empty = 0
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futs = {
            pool.submit(
                _fetch_one_hour,
                sym,
                h,
                side=offer,
                timeout=to,
                retries=retries,
            ): h
            for h in hours
        }
        for fut in as_completed(futs):
            hour = futs[fut]
            try:
                candle = fut.result()
            except Exception as exc:  # noqa: BLE001
                errors.append(f"{hour.isoformat()}: {exc}")
                continue
            if candle:
                rows.append(candle)
            else:
                empty += 1

    rows.sort(key=lambda r: r["opened_at"])

    def _is_fx_closed(h: datetime) -> bool:
        # Sabado inteiro + domingo antes ~21:00 UTC: sem bi5 tipico.
        if h.weekday() == 5:
            return True
        if h.weekday() == 6 and h.hour < 21:
            return True
        return False

    session_hours = [h for h in hours if not _is_fx_closed(h)]
    # Fds / mercado fechado: vazio e ok.
    if hours and not rows and empty == len(hours):
        return []
    if hours and not rows and not session_hours:
        return []
    # Chunk so de fds com 503: nao derruba o pull inteiro.
    if hours and not rows and errors and not session_hours:
        return []
    if hours and not rows and errors:
        # Se a maioria das horas de sessao veio vazia (404) e poucos 503,
        # trata como mercado fechado / buraco — nao aborta.
        if len(errors) <= max(3, len(hours) // 10):
            return []
        sample = "; ".join(errors[:3])
        raise RuntimeError(
            f"Dukascopy sem velas ({len(errors)} erros / {len(hours)} horas). "
            f"Ex.: {sample}"
        )
    # Com algumas velas + 503: devolve o que veio (cura parcial).
    if rows and errors:
        logger.warning(
            "Dukascopy parcial: %s/%s horas com erro; gravando %s velas.",
            len(errors),
            len(hours),
            len(rows),
        )
    return rows


def fetch_eurusd_1h_rows_for_store(
    start: datetime,
    end: datetime | None = None,
    *,
    asset: str = "EURUSD",
) -> list[dict[str, Any]]:
    """Converte OHLC Dukascopy para linhas upsert (ohlc_candles_eurusd)."""
    raw = fetch_eurusd_1h(start, end)
    now_iso = datetime.now(timezone.utc).isoformat()
    out: list[dict[str, Any]] = []
    for c in raw:
        o, h, lo, cl = c["open"], c["high"], c["low"], c["close"]
        h = max(h, o, cl)
        lo = min(lo, o, cl)
        out.append(
            {
                "asset": asset,
                "timeframe": "1h",
                "opened_at": c["opened_at"],
                "open": o,
                "high": h,
                "low": lo,
                "close": cl,
                "volume": float(c["volume"]) if c.get("volume") is not None else 0.0,
                "source": "dukascopy",
                "updated_at": now_iso,
            }
        )
    return ensure_provisional_current_hour(out, asset=asset)


def ensure_provisional_current_hour(
    rows: list[dict[str, Any]],
    *,
    asset: str = "EURUSD",
) -> list[dict[str, Any]]:
    """Se o bi5 da hora UTC atual ainda nao existe, cria vela provisoria.

    A Pocket OTC ja abre a vela da hora corrente; sem isso o EURUSD Dukascopy
    fica 1 candle atrasado no grafico ate o arquivo bi5 aparecer.
    """
    if not rows:
        return rows
    now = datetime.now(timezone.utc)
    # Sab/dom: mercado FX fechado — nao inventa vela.
    if now.weekday() >= 5:
        return rows
    hour = now.replace(minute=0, second=0, microsecond=0)
    hour_key = hour.strftime("%Y-%m-%dT%H:%M:%S")
    have = False
    last_close: float | None = None
    last_ts: datetime | None = None
    for r in rows:
        try:
            c = float(r["close"])
            ts = datetime.fromisoformat(
                str(r["opened_at"]).replace("Z", "+00:00")
            )
        except (TypeError, ValueError, KeyError):
            continue
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        else:
            ts = ts.astimezone(timezone.utc)
        if last_ts is None or ts >= last_ts:
            last_ts = ts
            last_close = c
        opened = ts.replace(minute=0, second=0, microsecond=0)
        if opened.strftime("%Y-%m-%dT%H:%M:%S") == hour_key:
            have = True
    if have or last_close is None:
        return rows
    # So completa se a ultima vela real for a hora imediatamente anterior
    # (atraso tipico de 1h), nao se faltar dias de dados.
    if last_ts is not None and hour - last_ts.replace(
        minute=0, second=0, microsecond=0
    ) > timedelta(hours=2):
        return rows
    provisional = {
        "asset": asset,
        "timeframe": "1h",
        "opened_at": hour.isoformat(),
        "open": last_close,
        "high": last_close,
        "low": last_close,
        "close": last_close,
        "volume": 0.0,
        "source": "dukascopy",
        "updated_at": now.isoformat(),
    }
    return list(rows) + [provisional]
