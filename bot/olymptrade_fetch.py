"""Cliente nao oficial Olymptrade (WS) → candles OHLC.

Pacote vendorizado em ./olymptrade_ws (precisa estar no deploy / PYTHONPATH=.).

Auth: OLYMPTRADE_ACCESS_TOKEN (cookie access_token no browser).
Par OTC tipico: OLYMPTRADE_PAIR=EURUSD_otc.
"""

from __future__ import annotations

import asyncio
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

SOURCE = "olymptrade"

# Garante que o pacote vendorizado na raiz do repo seja importavel no Railway.
_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

_IMPORT_ERR: str | None = None
try:
    from olymptrade_ws import OlympTradeClient  # type: ignore
except ImportError as exc:  # pragma: no cover
    OlympTradeClient = None  # type: ignore[misc, assignment]
    _IMPORT_ERR = str(exc)


def _env(name: str, default: str = "") -> str:
    return (os.environ.get(name) or default).strip()


def olymptrade_available() -> tuple[bool, str]:
    if OlympTradeClient is None:
        detail = _IMPORT_ERR or "ImportError"
        vendored = (_ROOT / "olymptrade_ws" / "__init__.py").is_file()
        if not vendored:
            return (
                False,
                'Pasta "olymptrade_ws/" ausente no deploy. '
                "Faca push dessa pasta no GitHub e redeploy no Railway.",
            )
        return (
            False,
            f'Falha ao importar olymptrade_ws: {detail}. '
            "Confira websockets/aiohttp no requirements.txt.",
        )
    if not _env("OLYMPTRADE_ACCESS_TOKEN"):
        return False, "Defina OLYMPTRADE_ACCESS_TOKEN no ambiente."
    return True, ""


def default_pair() -> str:
    return _env("OLYMPTRADE_PAIR", "EURUSD_otc") or "EURUSD_otc"


def default_store_asset() -> str:
    """Asset gravado no Supabase (separado da Pocket)."""
    return _env("OHLC_OLYMP_OTC_ASSET", "EURUSD_otc_olymp") or "EURUSD_otc_olymp"


def _candle_time_unix(raw: dict[str, Any]) -> int | None:
    for key in ("t", "time", "timestamp", "from", "open_time"):
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


def normalize_olymp_candle(
    raw: dict[str, Any],
    *,
    asset: str,
    timeframe: str = "1h",
    pair: str | None = None,
) -> dict[str, Any] | None:
    """Normaliza candle Olymp → row ohlc_candles (source=olymptrade)."""
    if not isinstance(raw, dict):
        return None
    # Respostas aninhadas: {"candle": {...}} / {"d": {...}}
    if "open" not in raw and "close" not in raw:
        for nest in ("candle", "d", "data"):
            inner = raw.get(nest)
            if isinstance(inner, dict):
                raw = inner
                break
    ts = _candle_time_unix(raw)
    o = _f(raw, "open", "Open", "o")
    h = _f(raw, "high", "High", "h", "max")
    lo = _f(raw, "low", "Low", "l", "min")
    c = _f(raw, "close", "Close", "c")
    if ts is None or o is None or h is None or lo is None or c is None:
        return None
    if timeframe == "1h":
        ts = (ts // 3600) * 3600
    elif timeframe == "1m":
        ts = (ts // 60) * 60
    h = max(h, o, c)
    lo = min(lo, o, c)
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
        "source": SOURCE,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    if vol is not None:
        row["volume"] = vol
    if pair:
        row["meta_pair"] = pair  # nao gravar se coluna nao existir — removido no upsert
    return row


def rows_for_store(
    raw_candles: list[dict[str, Any]],
    *,
    asset: str | None = None,
    timeframe: str = "1h",
    pair: str | None = None,
) -> list[dict[str, Any]]:
    store_asset = asset or default_store_asset()
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for raw in raw_candles:
        row = normalize_olymp_candle(
            raw, asset=store_asset, timeframe=timeframe, pair=pair
        )
        if row is None:
            continue
        row.pop("meta_pair", None)
        key = str(row["opened_at"])[:19]
        if key in seen:
            continue
        seen.add(key)
        out.append(row)
    out.sort(key=lambda r: str(r.get("opened_at") or ""))
    return out


async def _with_client(
    coro_fn: Callable[[Any], Any],
    *,
    access_token: str | None = None,
) -> Any:
    ok, msg = olymptrade_available()
    if OlympTradeClient is None:
        raise RuntimeError(msg)
    token = (access_token or _env("OLYMPTRADE_ACCESS_TOKEN")).strip()
    if not token:
        raise RuntimeError("Defina OLYMPTRADE_ACCESS_TOKEN no ambiente.")

    log_raw = _env("OLYMPTRADE_LOG_RAW", "0").lower() in ("1", "true", "yes")
    client = OlympTradeClient(access_token=token, log_raw_messages=log_raw)
    await client.start()
    try:
        # Bootstrap leve: subscriptions. Nao bloqueia em balance/demo.
        init = getattr(client, "initialize_session", None)
        if callable(init):
            try:
                await asyncio.wait_for(init(), timeout=20.0)
            except Exception:  # noqa: BLE001
                pass
        await asyncio.sleep(0.5)
        return await coro_fn(client)
    finally:
        stop = getattr(client, "stop", None)
        if callable(stop):
            await stop()


async def fetch_candles_async(
    pair: str | None = None,
    *,
    size: int = 3600,
    count: int = 48,
    end_time: datetime | int | None = None,
    access_token: str | None = None,
) -> list[dict[str, Any]]:
    """Pede um lote de candles historicos (event 10 → 1003 na lib)."""
    p = (pair or default_pair()).strip() or "EURUSD_otc"

    async def _run(client: Any) -> list[dict[str, Any]]:
        return await _get_candles_on_client(
            client, p, size=size, count=count, end_time=end_time
        )

    return await _with_client(_run, access_token=access_token)


def fetch_candles(
    pair: str | None = None,
    *,
    size: int = 3600,
    count: int = 48,
    end_time: datetime | int | None = None,
    access_token: str | None = None,
) -> list[dict[str, Any]]:
    """Wrapper sync para threads do coletor."""
    return asyncio.run(
        fetch_candles_async(
            pair,
            size=size,
            count=count,
            end_time=end_time,
            access_token=access_token,
        )
    )


async def _get_candles_on_client(
    client: Any,
    pair: str,
    *,
    size: int,
    count: int,
    end_time: datetime | int | None,
) -> list[dict[str, Any]]:
    market = getattr(client, "market", None)
    if market is None or not hasattr(market, "get_candles"):
        raise RuntimeError("OlympTradeClient sem market.get_candles.")
    raw = await market.get_candles(
        pair, size=size, count=count, end_time=end_time
    )
    if raw is None:
        return []
    if isinstance(raw, dict):
        nested = raw.get("d") or raw.get("data") or raw.get("candles")
        if isinstance(nested, list):
            raw = nested
        else:
            return []
    if not isinstance(raw, list):
        return []
    return [x for x in raw if isinstance(x, dict)]


async def fetch_candles_history_async(
    pair: str | None = None,
    *,
    size: int = 3600,
    hours: int = 24 * 14,
    chunk: int = 72,
    access_token: str | None = None,
    on_chunk: Callable[[list[dict[str, Any]]], None] | None = None,
) -> list[dict[str, Any]]:
    """Pagina para tras com end_time ate cobrir `hours` (1 sessao WS)."""
    p = (pair or default_pair()).strip() or "EURUSD_otc"
    hours = max(1, int(hours))
    chunk = max(8, min(int(chunk), 500))

    async def _run(client: Any) -> list[dict[str, Any]]:
        end: datetime | int | None = None
        collected: list[dict[str, Any]] = []
        seen: set[int] = set()
        remaining = hours
        while remaining > 0:
            n = min(chunk, remaining + 2)
            batch = await _get_candles_on_client(
                client, p, size=size, count=n, end_time=end
            )
            if not batch:
                break
            new_rows = 0
            oldest_ts: int | None = None
            for raw in batch:
                ts = _candle_time_unix(raw)
                if ts is None:
                    continue
                floored = timeframe_floor(ts, size)
                if floored in seen:
                    continue
                seen.add(floored)
                collected.append(raw)
                new_rows += 1
                if oldest_ts is None or ts < oldest_ts:
                    oldest_ts = ts
            if on_chunk is not None:
                on_chunk(batch)
            if new_rows == 0 or oldest_ts is None:
                break
            end = oldest_ts - 1
            remaining -= max(new_rows, 1)
            if len(seen) >= hours + chunk:
                break
        collected.sort(key=lambda r: _candle_time_unix(r) or 0)
        return collected

    return await _with_client(_run, access_token=access_token)


def timeframe_floor(ts: int, size: int) -> int:
    size = max(1, int(size))
    return (int(ts) // size) * size


def fetch_candles_history(
    pair: str | None = None,
    *,
    size: int = 3600,
    hours: int = 24 * 14,
    chunk: int = 72,
    access_token: str | None = None,
) -> list[dict[str, Any]]:
    return asyncio.run(
        fetch_candles_history_async(
            pair,
            size=size,
            hours=hours,
            chunk=chunk,
            access_token=access_token,
        )
    )


def fetch_ohlc_1h_rows_for_store(
    *,
    hours: int = 48,
    pair: str | None = None,
    asset: str | None = None,
    access_token: str | None = None,
) -> list[dict[str, Any]]:
    """Candles 1h normalizados prontos para upsert em ohlc_candles."""
    raw = fetch_candles_history(
        pair,
        size=3600,
        hours=hours,
        chunk=min(96, max(24, hours)),
        access_token=access_token,
    )
    return rows_for_store(raw, asset=asset, timeframe="1h", pair=pair or default_pair())
