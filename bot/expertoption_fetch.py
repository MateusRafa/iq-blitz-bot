"""Cliente nao oficial ExpertOption (WS) → candles OHLC.

Foco: puxar historico 1D (period=86400) para ohlc_candles_expert_1d.
Auth: cookie `token` (ou `auth`) → EXPERTOPTION_AUTH_TOKEN.
"""

from __future__ import annotations

import asyncio
import json
import os
import ssl
import time
import uuid
from collections import deque
from datetime import date, datetime, timezone
from typing import Any

SOURCE = "expertoption"
DEFAULT_WS = "wss://fr24g1eu.expertoption.com/"
PERIOD_1H = 3600
PERIOD_1D = 86400

ASSET_IDS = {
    "EURUSD": 142,
    "EURUSD_OTC": 179,
    "GBPUSD_OTC": 180,
    "USDJPY_OTC": 181,
}


def _env(name: str, default: str = "") -> str:
    return (os.environ.get(name) or default).strip()


def _auth_token() -> str:
    return (
        _env("EXPERTOPTION_AUTH_TOKEN")
        or _env("EXPERTOPTION_TOKEN")
        or _env("EXPERTOPTION_AUTH")
    )


def expertoption_available() -> tuple[bool, str]:
    try:
        import websockets  # noqa: F401
    except ImportError:
        return False, "Instale websockets (ja no requirements.txt)."
    if not _auth_token():
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
    out: list[dict[str, Any]] = []
    candles = message.get("candles")
    if candles is None:
        # algumas respostas metem em message.candles[0].periods
        candles = message.get("data") or []
    if not isinstance(candles, list) or not candles:
        return out

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
                        "timeframe": int(candle.get("tf") or timeframe),
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
    timeframe: str = "1d",
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
        opened = datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
    elif timeframe == "1d":
        # Alinha ao dia civil Pocket (UTC-3) como as outras ferramentas 1D.
        from bot.ohlc_collector_1d import pocket_day_key, pocket_midnight_utc

        day_key = pocket_day_key(datetime.fromtimestamp(ts, tz=timezone.utc))
        y, mo, d = (int(x) for x in day_key.split("-"))
        opened = pocket_midnight_utc(date(y, mo, d)).isoformat()
    else:
        opened = datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()

    h = max(h, o, c)
    lo = min(lo, o, c)
    return {
        "asset": asset,
        "timeframe": timeframe,
        "opened_at": opened,
        "open": o,
        "high": h,
        "low": lo,
        "close": c,
        "source": SOURCE if timeframe != "1d" else SOURCE,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }


def rows_for_store(
    raw_candles: list[dict[str, Any]],
    *,
    asset: str | None = None,
    timeframe: str = "1d",
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
        self._recv_task: asyncio.Task | None = None
        self._pending: dict[str, asyncio.Future] = {}
        self._candle_q: deque[dict[str, Any]] = deque(maxlen=2000)
        self._history_q: deque[dict[str, Any]] = deque(maxlen=2000)
        self._errors: list[str] = []
        self._last_actions: deque[str] = deque(maxlen=30)
        self.assets: list[dict[str, Any]] = []

    def _ns(self) -> str:
        return uuid.uuid4().hex

    async def connect(self) -> None:
        import websockets

        headers = {
            "Origin": "https://app.expertoption.com",
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
        }
        ssl_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ssl_ctx.check_hostname = False
        ssl_ctx.verify_mode = ssl.CERT_NONE
        try:
            self._ws = await websockets.connect(
                self.url,
                ssl=ssl_ctx,
                additional_headers=headers,
                ping_interval=20,
                ping_timeout=20,
                max_size=16 * 1024 * 1024,
            )
        except TypeError:
            # websockets antigo usa extra_headers
            self._ws = await websockets.connect(
                self.url,
                ssl=ssl_ctx,
                extra_headers=headers,
                ping_interval=20,
                ping_timeout=20,
                max_size=16 * 1024 * 1024,
            )
        self._recv_task = asyncio.create_task(self._receive_loop())

        await self.send_action(
            "setContext", {"is_demo": 1 if self.demo else 0}, wait=False
        )
        await asyncio.sleep(0.2)
        # Bootstrap leve
        try:
            await self.send_action("profile", {}, wait=True, timeout=6.0)
        except Exception:  # noqa: BLE001
            pass
        try:
            assets_msg = await self.send_action("assets", {}, wait=True, timeout=8.0)
            body = (assets_msg or {}).get("message") or {}
            raw_assets = body.get("assets") or body.get("data") or []
            if isinstance(raw_assets, list):
                self.assets = [a for a in raw_assets if isinstance(a, dict)]
        except Exception:  # noqa: BLE001
            self.assets = []

    async def close(self) -> None:
        if self._recv_task is not None:
            self._recv_task.cancel()
            try:
                await self._recv_task
            except asyncio.CancelledError:
                pass
            self._recv_task = None
        if self._ws is not None:
            await self._ws.close()
            self._ws = None

    async def send_action(
        self,
        action: str,
        message: dict[str, Any] | None = None,
        *,
        wait: bool = False,
        timeout: float = 8.0,
    ) -> dict[str, Any] | None:
        assert self._ws is not None
        ns = self._ns()
        payload = {
            "action": action,
            "message": message or {},
            "ns": ns,
            "token": self.token,
        }
        fut: asyncio.Future | None = None
        if wait:
            fut = asyncio.get_running_loop().create_future()
            self._pending[ns] = fut
        await self._ws.send(json.dumps(payload))
        if not wait or fut is None:
            return None
        try:
            return await asyncio.wait_for(fut, timeout=timeout)
        except asyncio.TimeoutError as exc:
            self._pending.pop(ns, None)
            raise TimeoutError(f"Timeout aguardando resposta de {action}") from exc

    async def _receive_loop(self) -> None:
        assert self._ws is not None
        try:
            async for raw in self._ws:
                if isinstance(raw, bytes):
                    raw = raw.decode("utf-8", errors="replace")
                try:
                    data = json.loads(raw)
                except json.JSONDecodeError:
                    continue
                if not isinstance(data, dict):
                    continue
                action = str(data.get("action") or "")
                ns = data.get("ns")
                self._last_actions.append(action)

                if action == "token":
                    new_tok = ((data.get("message") or {}) or {}).get("token")
                    if new_tok:
                        self.token = str(new_tok)

                if action == "error":
                    msg = data.get("message")
                    self._errors.append(str(msg)[:300])
                    if ns and str(ns) in self._pending:
                        fut = self._pending.pop(str(ns))
                        if not fut.done():
                            fut.set_exception(RuntimeError(str(msg)))
                    continue

                if action == "candles":
                    self._candle_q.append(data)
                if action == "assetHistoryCandles":
                    self._history_q.append(data)

                if ns is not None and str(ns) in self._pending:
                    fut = self._pending.pop(str(ns))
                    if not fut.done():
                        fut.set_result(data)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            self._errors.append(f"recv_loop: {exc}"[:300])

    def resolve_asset_id(self, pair: str, fallback: int) -> int:
        want = pair.upper().replace("/", "").replace(" ", "")
        # Preferir ativo ativo com o simbolo.
        candidates: list[dict[str, Any]] = []
        for a in self.assets:
            sym = str(a.get("symbol") or a.get("name") or "").upper()
            sym = sym.replace("/", "").replace(" ", "")
            if sym == want or sym.replace("_", "") == want.replace("_", ""):
                candidates.append(a)
            # EURUSD OTC variants
            if want.endswith("_OTC") and (
                sym == want or (want.replace("_OTC", "") in sym and "OTC" in sym)
            ):
                candidates.append(a)
        if not candidates:
            return fallback
        # Prefer is_active / profit
        def score(a: dict[str, Any]) -> tuple:
            return (
                int(a.get("is_active") or 0),
                float(a.get("profit") or 0),
                -int(a.get("id") or 0),
            )

        best = max(candidates, key=score)
        try:
            return int(best.get("id"))
        except (TypeError, ValueError):
            return fallback

    async def subscribe(self, asset_id: int, period: int) -> None:
        await self.send_action(
            "subscribeCandles",
            {"assets": [{"id": asset_id, "timeframes": [period]}]},
            wait=False,
        )
        await asyncio.sleep(0.4)

    async def history(
        self,
        asset_id: int,
        *,
        period: int,
        start_ts: int,
        end_ts: int,
        timeout: float = 10.0,
    ) -> list[dict[str, Any]]:
        before = len(self._history_q) + len(self._candle_q)
        ns = self._ns()
        payload = {
            "action": "assetHistoryCandles",
            "message": {
                "assetid": asset_id,
                "periods": [[int(start_ts), int(end_ts)]],
                "timeframes": [int(period)],
            },
            "ns": ns,
            "token": self.token,
        }
        fut = asyncio.get_running_loop().create_future()
        self._pending[ns] = fut
        assert self._ws is not None
        await self._ws.send(json.dumps(payload))

        try:
            resp = await asyncio.wait_for(fut, timeout=timeout)
            body = resp.get("message") if isinstance(resp, dict) else None
            if isinstance(body, dict):
                batch = _parse_history_message(body, period)
                if batch:
                    return batch
        except Exception:  # noqa: BLE001
            self._pending.pop(ns, None)

        # Fallback: drain queues for a short time
        deadline = time.monotonic() + min(timeout, 6.0)
        out: list[dict[str, Any]] = []
        while time.monotonic() < deadline:
            while self._history_q:
                msg = self._history_q.popleft()
                body = msg.get("message") or {}
                if isinstance(body, dict):
                    out.extend(_parse_history_message(body, period))
            while self._candle_q:
                msg = self._candle_q.popleft()
                body = msg.get("message") or {}
                if isinstance(body, dict):
                    out.extend(_parse_history_message(body, period))
            if out and (len(self._history_q) + len(self._candle_q)) == before:
                break
            await asyncio.sleep(0.05)
        return out


async def fetch_candles_async(
    *,
    asset_id: int | None = None,
    pair: str | None = None,
    hours: int | None = None,
    days: int | None = None,
    period: int = PERIOD_1D,
    token: str | None = None,
) -> list[dict[str, Any]]:
    ok, msg = expertoption_available()
    tok = (token or _auth_token()).strip()
    if not tok:
        raise RuntimeError(msg if not ok else "Token ExpertOption vazio.")

    p = (pair or default_pair()).upper()
    fallback_id = asset_id if asset_id is not None else default_asset_id()

    if period == PERIOD_1D:
        lookback_days = max(1, int(days or max(1, (hours or 24 * 120) // 24)))
        lookback_sec = lookback_days * 86400
        chunk_sec = max(86400 * 7, min(86400 * 60, int(_env("EXPERTOPTION_CHUNK_DAYS", "30") or "30") * 86400))
    else:
        lookback_hours = max(1, int(hours or (days or 14) * 24))
        lookback_sec = lookback_hours * 3600
        chunk_sec = max(6 * 3600, min(72 * 3600, int(_env("EXPERTOPTION_CHUNK_HOURS", "48") or "48") * 3600))

    end = int(time.time())
    start = end - lookback_sec

    client = _ExpertWs(tok, demo=is_demo(), url=ws_url())
    collected: list[dict[str, Any]] = []
    resolved_id = fallback_id
    try:
        await client.connect()
        resolved_id = client.resolve_asset_id(p, fallback_id)
        await client.subscribe(resolved_id, period)

        cursor_end = end
        empty_streak = 0
        while cursor_end > start:
            cursor_start = max(start, cursor_end - chunk_sec)
            batch = await client.history(
                resolved_id,
                period=period,
                start_ts=cursor_start,
                end_ts=cursor_end,
            )
            if batch:
                collected.extend(batch)
                empty_streak = 0
            else:
                empty_streak += 1
                if empty_streak >= 3 and not collected:
                    # Sem dados desde o inicio — para cedo com diagnostico
                    break
            cursor_end = cursor_start
            await asyncio.sleep(0.12)

        if not collected:
            err = "; ".join(client._errors[-3:]) if client._errors else ""
            actions = ",".join(list(client._last_actions)[-8:])
            raise RuntimeError(
                "ExpertOption devolveu 0 candles. "
                f"pair={p} asset_id={resolved_id} period={period} demo={is_demo()} "
                f"assets_n={len(client.assets)} actions=[{actions}] "
                f"errors=[{err or 'nenhum'}]. "
                "Confirme token (cookie token), EXPERTOPTION_DEMO e asset_id no Network/WS."
            )
    finally:
        await client.close()

    by_t: dict[int, dict[str, Any]] = {}
    for c in collected:
        try:
            t = int(c["time"])
        except (KeyError, TypeError, ValueError):
            continue
        if period == PERIOD_1H:
            t = (t // 3600) * 3600
        elif period == PERIOD_1D:
            t = (t // 86400) * 86400
        c = dict(c)
        c["time"] = t
        by_t[t] = c
    return [by_t[k] for k in sorted(by_t.keys())]


def fetch_candles(
    *,
    asset_id: int | None = None,
    pair: str | None = None,
    hours: int | None = None,
    days: int | None = None,
    period: int = PERIOD_1D,
    token: str | None = None,
) -> list[dict[str, Any]]:
    return asyncio.run(
        fetch_candles_async(
            asset_id=asset_id,
            pair=pair,
            hours=hours,
            days=days,
            period=period,
            token=token,
        )
    )


def fetch_ohlc_1d_rows_for_store(
    *,
    days: int = 120,
    pair: str | None = None,
    asset: str | None = None,
    asset_id: int | None = None,
) -> list[dict[str, Any]]:
    raw = fetch_candles(
        asset_id=asset_id,
        pair=pair or default_pair(),
        days=days,
        period=PERIOD_1D,
    )
    return rows_for_store(raw, asset=asset or default_store_asset(), timeframe="1d")


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
