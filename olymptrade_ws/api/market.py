# api/market.py
"""Market API Olymptrade — candles via e:10 / e:18 (DevTools).

Formato real (WS Messages):
  {"e":10,"t":3,"d":[{"p":"EURUSD_OTC","t":3600,"candles":[
      {"t":...,"open":...,"high":...,"low":...,"close":...}, ...
  ]}]}

No envelope, o timeframe e o campo `t` (segundos: 60/300/3600), NAO `tf`.
Pedido tipico: e:10 com {"p","t","to","solid":true}.
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

if TYPE_CHECKING:
    from olymptrade_ws.core.client import OlympTradeClient

logger = logging.getLogger(__name__)

# Respostas com historico OHLC (DevTools 2026-08): e:10 e e:18.
E_CANDLES_RESP = (10, 18, 1003)
# Pedido de historico
E_CANDLES_REQ = (10, 282)

_OHLC_KEYS = ("open", "high", "low", "close", "o", "h", "l", "c")


def _pair_aliases(pair: str) -> List[str]:
    """DevTools: EURUSD_OTC (OTC maiusculo)."""
    p = (pair or "").strip()
    if not p:
        return ["EURUSD_OTC", "EURUSD_otc", "EURUSD"]
    base = p
    if base.lower().endswith("_otc"):
        root = base[: -len("_otc")]
    else:
        root = base
    out: List[str] = []
    for cand in (
        p,
        f"{root}_OTC",
        f"{root}_otc",
        (root.upper() + "_OTC") if root else "",
        root,
        "EURUSD_OTC",
        "EURUSD_otc",
        "EURUSD",
    ):
        c = (cand or "").strip()
        if c and c not in out:
            out.append(c)
    return out


def _looks_like_candle(item: Any) -> bool:
    if not isinstance(item, dict):
        return False
    has_ohlc = sum(1 for k in _OHLC_KEYS if k in item) >= 2
    has_time = any(k in item for k in ("t", "time", "timestamp", "from", "ts"))
    return bool(has_ohlc and has_time)


def _envelope_tf(item: dict) -> Optional[int]:
    """Timeframe do envelope: campo t pequeno OU tf."""
    for key in ("tf", "size", "period"):
        if key in item and item[key] is not None:
            try:
                return int(item[key])
            except (TypeError, ValueError):
                pass
    # Em DevTools o TF e `t` (ex.: 3600). Timestamps unix sao >> 1e9.
    if "t" in item and item["t"] is not None:
        try:
            t_val = int(float(item["t"]))
        except (TypeError, ValueError):
            return None
        if 1 <= t_val <= 86400 * 7:  # ate 1 semana em segundos = TF
            return t_val
    return None


def _extract_candles(payload: Any) -> Optional[List[Dict[str, Any]]]:
    """Extrai lista OHLC do envelope Olymp: d[].candles[] ou lista direta."""
    if isinstance(payload, list) and payload:
        bars: List[Dict[str, Any]] = []
        for x in payload:
            if not isinstance(x, dict):
                continue
            nested = x.get("candles")
            env_tf = _envelope_tf(x)
            env_p = x.get("p") or x.get("pair")
            if isinstance(nested, list) and nested:
                for c in nested:
                    if not _looks_like_candle(c):
                        continue
                    row = dict(c)
                    if "p" not in row and env_p:
                        row["p"] = env_p
                    if "pair" not in row and x.get("pair"):
                        row["pair"] = x.get("pair")
                    if "tf" not in row and env_tf is not None:
                        row["tf"] = env_tf
                    bars.append(row)
                continue
            if _looks_like_candle(x):
                row = dict(x)
                if "tf" not in row and env_tf is not None:
                    row["tf"] = env_tf
                bars.append(row)
        if bars:
            return bars
    if isinstance(payload, dict):
        nested = payload.get("candles")
        if isinstance(nested, list) and nested:
            return _extract_candles(
                [
                    {
                        "p": payload.get("p"),
                        "pair": payload.get("pair"),
                        "t": payload.get("t"),
                        "tf": payload.get("tf"),
                        "candles": nested,
                    }
                ]
            )
        for key in ("d", "data", "history", "bars", "ohlc"):
            inner = payload.get(key)
            if inner is not None:
                got = _extract_candles(inner)
                if got:
                    return got
        if _looks_like_candle(payload):
            return [payload]
    return None


class MarketAPI:
    def __init__(self, client: "OlympTradeClient"):
        self._client = client

    async def subscribe_ticks(self, pair: str) -> None:
        logger.info(f"Subscribing to ticks for {pair}...")
        try:
            await self._client.send_request(12, [{"pair": pair}], requires_response=True)
            await self._client.send_request(280, [{"pair": pair}], requires_response=True)
            logger.info(f"Successfully sent tick subscription requests for {pair}.")
        except Exception as e:
            logger.error(f"Failed to subscribe to ticks for {pair}: {e}")
            raise

    async def unsubscribe_ticks(self, pair: str) -> None:
        logger.info(f"Unsubscribing from ticks for {pair}...")
        try:
            await self._client.send_request(13, [{"pair": pair}], requires_response=True)
            await self._client.send_request(281, [{"pair": pair}], requires_response=True)
            logger.info(f"Successfully sent tick unsubscription requests for {pair}.")
        except Exception as e:
            logger.error(f"Failed to unsubscribe from ticks for {pair}: {e}")
            raise

    async def get_candles(
        self,
        pair: str,
        size: int,
        count: int,
        end_time: Optional[Union[datetime, int]] = None,
    ) -> Optional[List[Dict[str, Any]]]:
        """Pede historico OHLC; escuta e:10 / e:18 com campo t=TF (DevTools)."""
        if end_time is None:
            to_ts = int(time.time())
        elif isinstance(end_time, datetime):
            if end_time.tzinfo is None:
                end_time = end_time.replace(tzinfo=timezone.utc)
            to_ts = int(end_time.timestamp())
        else:
            to_ts = int(end_time)

        pairs = _pair_aliases(pair)
        primary = next(
            (x for x in pairs if x.upper().endswith("_OTC")),
            pairs[0],
        )
        logger.info(
            f"Requesting candles pair={primary} aliases={pairs} "
            f"tf={size}s count={count} to={to_ts}"
        )

        loop = asyncio.get_running_loop()
        future: asyncio.Future = loop.create_future()
        seen_events: Dict[int, int] = {}
        sample_by_e: Dict[int, str] = {}

        def _pair_ok(pval: Any) -> bool:
            if pval is None:
                return False
            s = str(pval)
            wanted = {x.upper() for x in pairs}
            wanted.add(str(pair).upper())
            return s.upper() in wanted

        async def _on_any(message: Dict[str, Any]) -> None:
            if future.done():
                return
            ev = message.get("e")
            try:
                ev_i = int(ev) if ev is not None else -1
            except (TypeError, ValueError):
                ev_i = -1
            if ev_i >= 0:
                seen_events[ev_i] = seen_events.get(ev_i, 0) + 1
                if ev_i not in sample_by_e:
                    sample_by_e[ev_i] = repr(message.get("d"))[:240]

            extracted = _extract_candles(message.get("d"))
            if not extracted:
                extracted = _extract_candles(message)
            if not extracted:
                return

            filtered = [
                c
                for c in extracted
                if isinstance(c, dict) and _pair_ok(c.get("p") or c.get("pair"))
            ]
            if not filtered:
                return

            same_tf = [
                c
                for c in filtered
                if c.get("tf") is None or int(c.get("tf") or 0) == int(size)
            ]
            if not same_tf:
                tfs = sorted(
                    {
                        int(c.get("tf"))
                        for c in filtered
                        if c.get("tf") is not None
                    }
                )
                logger.debug(f"OHLC ignorado (tf {tfs} != {size}) e:{ev_i}")
                return

            logger.info(
                f"OHLC hit via e:{ev_i} pair={primary} tf={size} "
                f"({len(same_tf)} bars)"
            )
            future.set_result(same_tf)

        # Escuta respostas com OHLC em qualquer evento (e:10 e e:18 no DevTools).
        for ev in E_CANDLES_RESP:
            self._client.register_callback(ev, _on_any)
        _has_global = hasattr(self._client, "register_global_callback")
        if _has_global:
            self._client.register_global_callback(_on_any)
        else:
            for _ev in (11, 282, 283, 2223, 1097):
                self._client.register_callback(_ev, _on_any)
        try:
            try:
                await self._client.send_request(
                    98, [10, 18, 11, 282, 283], requires_response=False
                )
            except Exception:
                pass

            for p in (primary,):
                try:
                    await self._client.send_request(
                        95,
                        [{"cat": "digital", "pair": p}],
                        requires_response=False,
                    )
                except Exception:
                    pass
                try:
                    await self._client.send_request(
                        12, [{"pair": p}], requires_response=False
                    )
                    await self._client.send_request(
                        280, [{"pair": p}], requires_response=False
                    )
                except Exception:
                    pass

            from_ts = to_ts - max(size, 1) * max(count, 1)
            p = primary
            # DevTools: timeframe no campo `t` (nao `tf`).
            payloads = [
                [{"p": p, "t": size, "to": to_ts, "solid": True}],
                [{"p": p, "t": size, "from": from_ts, "to": to_ts, "solid": True}],
                [{"p": p, "t": size, "to": to_ts}],
                [{"pair": p, "size": size, "to": to_ts, "solid": True}],
                [{"pair": p, "size": size, "from": from_ts, "to": to_ts, "solid": True}],
                [{"p": p, "tf": size, "to": to_ts, "solid": True}],
            ]
            for data in payloads:
                if future.done():
                    break
                for ev in E_CANDLES_REQ:
                    try:
                        await self._client.send_request(
                            ev, data, requires_response=False
                        )
                    except Exception as e:
                        logger.debug(f"candle send e:{ev} failed: {e}")
                await asyncio.sleep(0.35)

            timeout = float(os.environ.get("OLYMPTRADE_CANDLE_TIMEOUT", "30") or 30)
            try:
                candles_data = await asyncio.wait_for(future, timeout=timeout)
            except asyncio.TimeoutError:
                top = sorted(seen_events.items(), key=lambda x: -x[1])[:12]
                samples = "; ".join(
                    f"e:{e}={sample_by_e.get(e, '')}" for e, _ in top[:5]
                )
                logger.error(
                    f"Timeout candles {pair}: sem OHLC e:10/e:18. Eventos: {top}. "
                    f"Amostras: {samples}"
                )
                return None

            if isinstance(candles_data, list):
                logger.info(f"Received {len(candles_data)} candles for {pair}.")
                return candles_data
            return None
        except Exception as e:
            logger.error(f"Failed to get candles for {pair}: {e!r}")
            return None
        finally:
            for ev in E_CANDLES_RESP:
                self._client.unregister_callback(ev, _on_any)
            if _has_global and hasattr(self._client, "unregister_global_callback"):
                self._client.unregister_global_callback(_on_any)
            else:
                for _ev in (11, 282, 283, 2223, 1097):
                    self._client.unregister_callback(_ev, _on_any)

    async def get_profitability(self, account_id: int) -> Optional[List[Dict[str, Any]]]:
        logger.info(f"Requesting asset profitability for account {account_id}...")
        try:
            response = await self._client.send_request(
                182, [{"account_id": account_id}], requires_response=True
            )
            if response and response.get("e") == 182:
                profit_data = response.get("d")
                if isinstance(profit_data, list):
                    return profit_data
            return None
        except Exception as e:
            logger.error(f"Failed to get profitability: {e}")
            return None
