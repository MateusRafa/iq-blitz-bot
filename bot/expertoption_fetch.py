"""Cliente nao oficial ExpertOption (WS) → candles OHLC 1h.

Auth: cookie `token` em app.expertoption.com → EXPERTOPTION_AUTH_TOKEN.
Par OTC tipico: EURUSD_OTC (asset_id 179).
"""

from __future__ import annotations

import asyncio
import json
import os
import time
import uuid
from datetime import datetime, timezone
from typing import Any

SOURCE = "expertoption"
DEFAULT_WS = "wss://fr24g1eu.expertoption.com/"
PERIOD_1H = 3600

# IDs conhecidos (comunidade / server map). Preferir resolve via assets se disponivel.
ASSET_IDS = {
    "EURUSD": 142,
    "EURUSD_OTC": 179,
    "GBPUSD_OTC": 180,
    "USDJPY_OTC": 181,
}


def _env(name: str, default: str = "") -> str:
    return (os.environ.get(name) or default).strip()


def expertoption_available() -> tuple[bool, str]:
    try:
        import websockets  # noqa: F401
    except ImportError:
        return False, "Instale websockets (ja no requirements.txt)."
    if not _env("EXPERTOPTION_AUTH_TOKEN"):
        return (
            False,
            "Defina EXPERTOPTION_AUTH_TOKEN (cookie `token` em app.expertoption.com).",
        )
    return True, ""


def default_pair() -> str:
    return (_env("EXPERTOPTION_PAIR", "EURUSD_OTC") or "EURUSD_OTC").upper()


def default_store_asset() -> str:
    return _env("OHLC_EXPERT_OTC_ASSET", "EURUSD_otc_expert") or "EURUSD_otc_expert"


def default_asset_id() -> int:
    raw = _env("EXPERTOPTION_ASSET_ID")
    if raw.isdigit():
        return int(raw)
    return int(ASSET_IDS.get(default_pair(), 179))


def is_demo() -> bool:
    return _env("EXPERTOPTION_DEMO", "1").lower() in ("1", "true", "yes", "demo")


def ws_url() -> str:
    return _env("EXPERTOPTION_WS_URI", DEFAULT_WS) or DEFAULT_WS


def _parse_history_message(message: dict[str, Any], timeframe: int) -> list[dict[str, Any]]:
    """Extrai candles de respostas assetHistoryCandles / candles."""
    out: list[dict[str, Any]] = []
    candles = message.get("candles") or []
    if not isinstance(candles, list) or not candles:
        return out

    # Historico: [{ "periods": [[start, [[o,h,l,c], ...]], ...] }]
    first = candles[0]
    if isinstance(first, dict) and "periods" in first:
        for period in first.get("periods") or []:
            if not isinstance(period, (list, tuple)) or len(period) < 2:
                continue
            period_start = int(period[0])
            period_candles = period[1]
            if not isinstance(period_candles, list):
                continue
            t = period_start
            for candle in period_candles:
                if isinstance(candle, list) and len(candle) >= 4:
                    out.append(
                        {
                            "time": t,
                            "open": float(candle[0]),
                            "high": float(candle[1]),
                            "low": float(candle[2]),
                            "close": float(candle[3]),
                            "timeframe": timeframe,
                        }
                    )
                    t += timeframe
        return out

    for candle in candles:
        if isinstance(candle, dict) and "t" in candle and "v" in candle:
            v = candle["v"]
            if isinstance(v, list) and len(v) >= 4:
                out.append(
                    {
                        "time": int(candle["t"]),
                        "open": float(v[0]),
                        "high": float(v[1]),
                        "low": float(v[2]),
                        "close": float(v[3]),
                        "timeframe": timeframe,
                    }
                )
        elif isinstance(candle, list) and len(candle) >= 2:
            vals = candle[1]
            if isinstance(vals, list) and len(vals) >= 4:
                out.append(
                    {
                        "time": int(candle[0]),
                        "open": float(vals[0]),
                        "high": float(vals[1]),
                        "low": float(vals[2]),
                        "close": float(vals[3]),
                        "timeframe": timeframe,
                    }
                )
    return out


def normalize_expert_candle(
    raw: dict[str, Any],
    *,
    asset: str,
    timeframe: str = "1h",
) -> dict[str, Any] | None:
    try:
        ts = int(raw["time"])
        o = float(raw["open"])
        h = float(raw["high"])
        lo = float(raw["low"])
        c = float(raw["close"])
    except (KeyError, TypeError, ValueError):
        return None
    if timeframe == "1h":
        ts = (ts // 3600) * 3600
    h = max(h, o, c)
    lo = min(lo, o, c)
    return {
        "asset": asset,
        "timeframe": timeframe,
        "opened_at": datetime.fromtimestamp(ts, tz=timezone.utc).isoformat(),
        "open": o,
        "high": h,
        "low": lo,
        "close": c,
        "source": SOURCE,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }


def rows_for_store(
    raw_candles: list[dict[str, Any]],
    *,
    asset: str | None = None,
    timeframe: str = "1h",
) -> list[dict[str, Any]]:
    store_asset = asset or default_store_asset()
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for raw in raw_candles:
        row = normalize_expert_candle(raw, asset=store_asset, timeframe=timeframe)
        if row is None:
            continue
        key = str(row["opened_at"])[:19]
        if key in seen:
            continue
        seen.add(key)
        out.append(row)
    out.sort(key=lambda r: str(r.get("opened_at") or ""))
    return out


class _ExpertWs:
    def __init__(self, token: str, *, demo: bool, url: str) -> None:
        self.token = token
        self.demo = demo
        self.url = url
        self._ws: Any = None
        self._ns = 0

    def _next_ns(self) -> str:
        self._ns += 1
        return f"{self._ns}-{uuid.uuid4().hex[:8]}"

    async def connect(self) -> None:
        import websockets

        self._ws = await websockets.connect(
            self.url,
            ping_interval=20,
            ping_timeout=20,
            max_size=8 * 1024 * 1024,
            origin="https://app.expertoption.com",
        )
        await self.send(
            {
                "action": "setContext",
                "message": {"is_demo": 1 if self.demo else 0},
                "token": self.token,
                "ns": self._next_ns(),
            }
        )
        await self.send(
            {
                "action": "profile",
                "message": {},
                "token": self.token,
                "ns": self._next_ns(),
            }
        )
        await asyncio.sleep(0.3)

    async def close(self) -> None:
        if self._ws is not None:
            await self._ws.close()
            self._ws = None

    async def send(self, payload: dict[str, Any]) -> None:
        assert self._ws is not None
        await self._ws.send(json.dumps(payload))

    async def recv_json(self, timeout: float = 8.0) -> dict[str, Any] | None:
        assert self._ws is not None
        try:
            raw = await asyncio.wait_for(self._ws.recv(), timeout=timeout)
        except asyncio.TimeoutError:
            return None
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", errors="replace")
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            return None
        return data if isinstance(data, dict) else None

    async def subscribe_candles(self, asset_id: int, period: int) -> None:
        await self.send(
            {
                "action": "subscribeCandles",
                "message": {"assets": [{"id": asset_id, "timeframes": [period]}]},
                "ns": self._next_ns(),
                "token": self.token,
            }
        )

    async def request_history(
        self,
        asset_id: int,
        *,
        period: int,
        start_ts: int,
        end_ts: int,
    ) -> None:
        await self.send(
            {
                "action": "assetHistoryCandles",
                "message": {
                    "assetid": asset_id,
                    "periods": [[start_ts, end_ts]],
                    "timeframes": [period],
                },
                "ns": self._next_ns(),
                "token": self.token,
            }
        )


async def fetch_candles_async(
    *,
    asset_id: int | None = None,
    pair: str | None = None,
    hours: int = 48,
    period: int = PERIOD_1H,
    token: str | None = None,
) -> list[dict[str, Any]]:
    ok, msg = expertoption_available()
    tok = (token or _env("EXPERTOPTION_AUTH_TOKEN")).strip()
    if not tok:
        raise RuntimeError(msg if not ok else "Token ExpertOption vazio.")

    aid = asset_id
    if aid is None:
        p = (pair or default_pair()).upper()
        aid = int(ASSET_IDS.get(p, default_asset_id()))

    lookback = max(1, int(hours))
    chunk_hours = max(6, min(72, int(_env("EXPERTOPTION_CHUNK_HOURS", "48") or "48")))
    end = int(time.time())
    start = end - lookback * 3600

    client = _ExpertWs(tok, demo=is_demo(), url=ws_url())
    collected: list[dict[str, Any]] = []
    try:
        await client.connect()
        await client.subscribe_candles(aid, period)
        await asyncio.sleep(0.4)

        cursor_end = end
        while cursor_end > start:
            cursor_start = max(start, cursor_end - chunk_hours * 3600)
            await client.request_history(
                aid, period=period, start_ts=cursor_start, end_ts=cursor_end
            )
            deadline = time.monotonic() + 10.0
            got = False
            while time.monotonic() < deadline:
                msg_in = await client.recv_json(timeout=2.0)
                if not msg_in:
                    continue
                action = str(msg_in.get("action") or "")
                body = msg_in.get("message")
                if not isinstance(body, dict):
                    continue
                if action in ("candles", "assetHistoryCandles", "historyCandles"):
                    if body.get("assetId") not in (None, aid, str(aid)):
                        # algumas respostas usam assetid
                        if body.get("assetid") not in (None, aid, str(aid)):
                            continue
                    batch = _parse_history_message(body, period)
                    if batch:
                        collected.extend(batch)
                        got = True
                        break
            if not got:
                # avanca mesmo assim para nao ficar preso
                pass
            cursor_end = cursor_start
            await asyncio.sleep(0.15)
    finally:
        await client.close()

    # dedup
    by_t: dict[int, dict[str, Any]] = {}
    for c in collected:
        try:
            t = int(c["time"])
        except (KeyError, TypeError, ValueError):
            continue
        if period == PERIOD_1H:
            t = (t // 3600) * 3600
            c = dict(c)
            c["time"] = t
        by_t[t] = c
    out = [by_t[k] for k in sorted(by_t.keys())]
    return out


def fetch_candles(
    *,
    asset_id: int | None = None,
    pair: str | None = None,
    hours: int = 48,
    period: int = PERIOD_1H,
    token: str | None = None,
) -> list[dict[str, Any]]:
    return asyncio.run(
        fetch_candles_async(
            asset_id=asset_id,
            pair=pair,
            hours=hours,
            period=period,
            token=token,
        )
    )


def fetch_ohlc_1h_rows_for_store(
    *,
    hours: int = 48,
    pair: str | None = None,
    asset: str | None = None,
    asset_id: int | None = None,
) -> list[dict[str, Any]]:
    raw = fetch_candles(
        asset_id=asset_id,
        pair=pair or default_pair(),
        hours=hours,
        period=PERIOD_1H,
    )
    return rows_for_store(raw, asset=asset or default_store_asset(), timeframe="1h")
