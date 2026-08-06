"""Cliente nao oficial ExpertOption (WS) → candles OHLC.

Foco: puxar historico 1D (period=86400) para ohlc_candles_expert_1d.
Auth: cookie `token` (ou `auth`) → EXPERTOPTION_AUTH_TOKEN.
"""

from __future__ import annotations

import asyncio
import json
import os
import re
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


def _extract_assets(node: Any) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    if isinstance(node, list):
        for item in node:
            out.extend(_extract_assets(item))
        return out
    if not isinstance(node, dict):
        return out
    aid = node.get("id", node.get("assetId", node.get("asset_id")))
    if aid is not None and any(
        k in node
        for k in ("symbol", "name", "title", "shortName", "short_name", "pair")
    ):
        item = dict(node)
        item["id"] = aid
        out.append(item)
    for key in ("assets", "data", "items", "list", "result", "message"):
        if key in node:
            out.extend(_extract_assets(node[key]))
    return out


def _norm_sym(raw: Any) -> str:
    return re.sub(r"[^A-Z0-9]", "", str(raw or "").upper())


def _asset_symbols(a: dict[str, Any]) -> list[str]:
    keys = (
        "symbol",
        "name",
        "title",
        "shortName",
        "short_name",
        "assetName",
        "pair",
    )
    return [_norm_sym(a.get(k)) for k in keys if a.get(k)]


def _asset_is_active(a: dict[str, Any]) -> bool:
    for k in ("is_active", "isActive", "active", "available", "enabled"):
        v = a.get(k)
        if v is None:
            continue
        if isinstance(v, bool):
            return v
        try:
            return int(v) == 1
        except (TypeError, ValueError):
            s = str(v).strip().lower()
            if s in ("1", "true", "yes", "active"):
                return True
            if s in ("0", "false", "no", "inactive"):
                return False
    return True


def _asset_profit(a: dict[str, Any]) -> float:
    for k in ("profit", "payout", "percentProfit", "percent"):
        try:
            return float(a.get(k) or 0)
        except (TypeError, ValueError):
            continue
    return 0.0


def find_eurusd_otc_candidates(assets: list[dict[str, Any]]) -> list[dict[str, Any]]:
    found: list[dict[str, Any]] = []
    seen: set[int] = set()
    for a in assets:
        try:
            aid = int(a.get("id"))
        except (TypeError, ValueError):
            continue
        syms = _asset_symbols(a)
        joined = "".join(syms)
        has_eurusd = any("EURUSD" in s for s in syms) or any(
            s.startswith("EUR") and "USD" in s for s in syms
        )
        has_otc = any("OTC" in s for s in syms) or bool(
            a.get("isOTC") or a.get("is_otc") or a.get("otc")
        )
        # Alguns payloads marcam OTC só no tipo/categoria
        typ = _norm_sym(a.get("type") or a.get("category") or a.get("group") or "")
        if "OTC" in typ:
            has_otc = True
        if not (has_eurusd and has_otc):
            # Aceitar EURUSD puro se estiver ativo e for o unico caminho
            if has_eurusd and _asset_is_active(a) and not has_otc:
                # so inclui se nenhum OTC existir — tratado no pick
                pass
            else:
                if not (has_eurusd and has_otc):
                    continue
        if not has_eurusd:
            continue
        if aid in seen:
            continue
        # Preferir so OTC quando flag/simbolo OTC; se so EURUSD sem OTC, guarda como fraco
        item = dict(a)
        item["_is_otc"] = bool(has_otc)
        seen.add(aid)
        found.append(item)
    otc_only = [a for a in found if a.get("_is_otc")]
    return otc_only or found


def pick_best_asset(
    candidates: list[dict[str, Any]],
    *,
    prefer_active: bool = True,
) -> dict[str, Any] | None:
    if not candidates:
        return None
    pool = candidates
    if prefer_active:
        active = [a for a in candidates if _asset_is_active(a)]
        if active:
            pool = active

    def score(a: dict[str, Any]) -> tuple:
        return (
            1 if _asset_is_active(a) else 0,
            _asset_profit(a),
            -int(a.get("id") or 0),
        )

    return max(pool, key=score)


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
            self.assets = _extract_assets(assets_msg)
            # dedup by id
            by_id: dict[int, dict[str, Any]] = {}
            for a in self.assets:
                try:
                    by_id[int(a["id"])] = a
                except (KeyError, TypeError, ValueError):
                    continue
            self.assets = list(by_id.values())
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

    def resolve_asset_id(self, pair: str, fallback: int) -> tuple[int, list[dict[str, Any]]]:
        """Escolhe asset_id ativo. Ignora fallback inativo se houver candidato ativo."""
        want = _norm_sym(pair)
        candidates = find_eurusd_otc_candidates(self.assets)
        # Se o par nao e EURUSD_OTC, filtra pelo simbolo pedido
        if want and want not in ("EURUSDOTC", "EURUSD_OTC"):
            narrowed: list[dict[str, Any]] = []
            for a in self.assets:
                syms = _asset_symbols(a)
                if any(want == s or want in s or s in want for s in syms):
                    narrowed.append(a)
            if narrowed:
                candidates = narrowed

        best = pick_best_asset(candidates, prefer_active=True)
        summary = []
        for a in candidates[:12]:
            summary.append(
                {
                    "id": a.get("id"),
                    "symbol": (a.get("symbol") or a.get("name") or a.get("title")),
                    "active": _asset_is_active(a),
                    "profit": _asset_profit(a),
                }
            )
        if best is not None:
            try:
                return int(best["id"]), summary
            except (TypeError, ValueError, KeyError):
                pass
        return int(fallback), summary

    async def fetch_timeframes(self, asset_id: int) -> list[int]:
        try:
            resp = await self.send_action(
                "getCandlesTimeframes",
                {"assetId": asset_id},
                wait=True,
                timeout=6.0,
            )
        except Exception:  # noqa: BLE001
            return []
        body = (resp or {}).get("message") or {}
        raw = body.get("timeframes") or body.get("data") or body.get("tfs") or []
        out: list[int] = []
        if isinstance(raw, list):
            for x in raw:
                try:
                    out.append(int(x))
                except (TypeError, ValueError):
                    continue
        return sorted(set(out))

    def choose_period(self, wanted: int, available: list[int]) -> int:
        if not available:
            # 86400 costuma falhar; 3600 e o caminho seguro para agregar D1
            return PERIOD_1H if wanted == PERIOD_1D else wanted
        if wanted in available:
            return wanted
        # maior TF <= wanted
        below = [t for t in available if t <= wanted]
        if below:
            return max(below)
        return min(available)

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
    period: int = PERIOD_1H,
    token: str | None = None,
) -> list[dict[str, Any]]:
    ok, msg = expertoption_available()
    tok = (token or _auth_token()).strip()
    if not tok:
        raise RuntimeError(msg if not ok else "Token ExpertOption vazio.")

    p = (pair or default_pair()).upper()
    # Nao forcar ASSET_ID inativo do env se o WS listar um ativo.
    env_forced = _env("EXPERTOPTION_ASSET_ID").isdigit()
    fallback_id = (
        int(asset_id)
        if asset_id is not None
        else (default_asset_id() if env_forced else 0)
    )

    # Pedidos "1D" passam a puxar 1h (TF mais estavel) — agregacao fica no caller.
    wanted_period = PERIOD_1H if period == PERIOD_1D else int(period)
    lookback_hours = max(
        24,
        int(hours or (days or 120) * 24),
    )
    lookback_sec = lookback_hours * 3600
    chunk_sec = max(
        6 * 3600,
        min(72 * 3600, int(_env("EXPERTOPTION_CHUNK_HOURS", "48") or "48") * 3600),
    )

    end = int(time.time())
    start = end - lookback_sec

    client = _ExpertWs(tok, demo=is_demo(), url=ws_url())
    collected: list[dict[str, Any]] = []
    resolved_id = fallback_id or 179
    used_period = wanted_period
    candidates_summary: list[dict[str, Any]] = []
    try:
        await client.connect()
        resolved_id, candidates_summary = client.resolve_asset_id(
            p, fallback_id or default_asset_id()
        )
        tfs = await client.fetch_timeframes(resolved_id)
        used_period = client.choose_period(wanted_period, tfs)
        await client.subscribe(resolved_id, used_period)

        cursor_end = end
        empty_streak = 0
        while cursor_end > start:
            cursor_start = max(start, cursor_end - chunk_sec)
            batch = await client.history(
                resolved_id,
                period=used_period,
                start_ts=cursor_start,
                end_ts=cursor_end,
            )
            if batch:
                collected.extend(batch)
                empty_streak = 0
            else:
                empty_streak += 1
                if empty_streak >= 3 and not collected:
                    break
            cursor_end = cursor_start
            await asyncio.sleep(0.12)

        if not collected:
            err = "; ".join(client._errors[-3:]) if client._errors else ""
            actions = ",".join(list(client._last_actions)[-8:])
            raise RuntimeError(
                "ExpertOption devolveu 0 candles. "
                f"pair={p} asset_id={resolved_id} period={used_period} "
                f"(pedido={period}) demo={is_demo()} assets_n={len(client.assets)} "
                f"tfs={tfs[:12]} candidates={candidates_summary[:6]} "
                f"actions=[{actions}] errors=[{err or 'nenhum'}]. "
                "Remova EXPERTOPTION_ASSET_ID=179 se estiver no Railway "
                "(179 esta inativo). Deixe o bot escolher o OTC ativo."
            )
    finally:
        await client.close()

    by_t: dict[int, dict[str, Any]] = {}
    step = used_period if used_period > 0 else 3600
    for c in collected:
        try:
            t = int(c["time"])
        except (KeyError, TypeError, ValueError):
            continue
        t = (t // step) * step
        c = dict(c)
        c["time"] = t
        c["timeframe"] = step
        by_t[t] = c
    return [by_t[k] for k in sorted(by_t.keys())]


def fetch_candles(
    *,
    asset_id: int | None = None,
    pair: str | None = None,
    hours: int | None = None,
    days: int | None = None,
    period: int = PERIOD_1H,
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
    """Puxa 1h via WS e agrega para D1 (TF 86400 costuma ser rejeitado)."""
    from bot.ohlc_collector_1d import aggregate_hourly_to_daily

    lookback_days = max(1, min(int(days), 800))
    raw_1h = fetch_candles(
        asset_id=asset_id,
        pair=pair or default_pair(),
        days=lookback_days,
        period=PERIOD_1H,
    )
    hourly_rows = rows_for_store(
        raw_1h, asset=asset or default_store_asset(), timeframe="1h"
    )
    if not hourly_rows:
        return []
    daily = aggregate_hourly_to_daily(
        hourly_rows,
        asset=asset or default_store_asset(),
        include_today=True,
    )
    for row in daily:
        row["source"] = SOURCE
        row["timeframe"] = "1d"
    return daily


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
