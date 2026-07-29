"""Baixa ticks Dukascopy (bi5) e agrega em velas 1h OHLC (UTC / Bid)."""

from __future__ import annotations

import lzma
import os
import struct
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from typing import Any

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


def _download_bi5(url: str, *, timeout: float = 30.0) -> bytes | None:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (compatible; iq-blitz-bot/1.0)",
            "Accept": "*/*",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.read()
    except urllib.error.HTTPError as exc:
        if exc.code in (404, 204):
            return None
        raise RuntimeError(f"Dukascopy HTTP {exc.code}: {url}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"Dukascopy rede: {exc.reason}") from exc


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
) -> dict[str, Any] | None:
    url = _hour_url(symbol, hour_utc)
    payload = _download_bi5(url, timeout=timeout)
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
) -> list[dict[str, Any]]:
    """Baixa e agrega velas 1h Bid (UTC) para o intervalo [start, end)."""
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
    if end_h <= start_h:
        return []

    hours: list[datetime] = []
    cur = start_h
    while cur < end_h:
        # Mercado FX: sabado quase vazio; domingo abre ~21/22 UTC — ainda assim
        # tentamos e ignoramos 404.
        hours.append(cur)
        cur += timedelta(hours=1)

    workers = max(1, min(max_workers or _env_int("DUKASCOPY_WORKERS", 8), 16))
    to = float(timeout if timeout is not None else _env_int("DUKASCOPY_TIMEOUT", 30))
    offer = "bid" if side.lower() != "ask" else "ask"

    rows: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futs = {
            pool.submit(_fetch_one_hour, sym, h, side=offer, timeout=to): h
            for h in hours
        }
        for fut in as_completed(futs):
            try:
                candle = fut.result()
            except Exception:
                # Uma hora falhou: propaga se for rede sistemica? Preferimos
                # continuar e reportar depois via contagem.
                continue
            if candle:
                rows.append(candle)

    rows.sort(key=lambda r: r["opened_at"])
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
                "volume": c.get("volume"),
                "source": "dukascopy",
                "updated_at": now_iso,
            }
        )
    return out
