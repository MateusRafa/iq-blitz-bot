# api/market.py
"""Market API Olymptrade — candles via e:18 (confirmado no DevTools).

Formato real (WS Messages):
  {"e":18,"t":3,"d":[{"p":"EURUSD_OTC","tf":3600,"candles":[
      {"t":...,"open":...,"high":...,"low":...,"close":...}, ...
  ]}]}

Pedido tipico (Chipa / browser): e:10 com pair/size/to ou p/tf/to.
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

# Resposta real de historico OHLC (DevTools 2026-08).
E_CANDLES = 18
# Pedidos / legado Chipa
E_CANDLES_REQ = (10, 282, 18)

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


def _extract_candles(payload: Any) -> Optional[List[Dict[str, Any]]]:
    """Extrai lista OHLC do envelope Olymp: d[].candles[] ou lista direta."""
    if isinstance(payload, list) and payload:
        # Envelope: [{"p":"...","tf":3600,"candles":[...]}]
        bars: List[Dict[str, Any]] = []
        for x in payload:
            if not isinstance(x, dict):
                continue
            nested = x.get("candles")
            if isinstance(nested, list) and nested:
                for c in nested:
                    if _looks_like_candle(c):
                        # Propaga par/tf do envelope se a vela nao tiver.
                        row = dict(c)
                        if "p" not in row and x.get("p"):
                            row["p"] = x.get("p")
                        if "pair" not in row and x.get("pair"):
                            row["pair"] = x.get("pair")
                        if "tf" not in row and x.get("tf") is not None:
                            row["tf"] = x.get("tf")
                        bars.append(row)
                continue
            if _looks_like_candle(x):
                bars.append(x)
        if bars:
            return bars
    if isinstance(payload, dict):
        nested = payload.get("candles")
        if isinstance(nested, list) and nested:
            return _extract_candles(
                [{"p": payload.get("p"), "tf": payload.get("tf"), "candles": nested}]
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
        """Pede historico OHLC; escuta e:18 (formato DevTools)."""
        if end_time is None:
            to_ts = int(time.time())
        elif isinstance(end_time, datetime):
            if end_time.tzinfo is None:
                end_time = end_time.replace(tzinfo=timezone.utc)
            to_ts = int(end_time.timestamp())
        else:
            to_ts = int(end_time)

        pairs = _pair_aliases(pair)
        primary = pairs[0]
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
                return True
            s = str(pval)
            return s in pairs or s == str(pair) or s.upper() in {x.upper() for x in pairs}

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

            # Prioriza e:18; aceita outros se tiverem candles[].
            extracted = _extract_candles(message.get("d"))
            if not extracted:
                extracted = _extract_candles(message)
            if not extracted:
                return
            if ev_i not in (E_CANDLES, 1003, 11, 283, 10, -1) and not any(
                "open" in c or "close" in c for c in extracted[:3]
            ):
                return

            filtered = [
                c
                for c in extracted
                if isinstance(c, dict) and _pair_ok(c.get("p") or c.get("pair"))
            ]
            # Se filtro zerou mas veio lote unico de outro formato, aceita.
            result = filtered or extracted
            # Preferir tf pedido quando presente
            if size:
                same_tf = [
                    c
                    for c in result
                    if not c.get("tf") or int(c.get("tf") or 0) == int(size)
                ]
                if same_tf:
                    result = same_tf
            logger.info(f"OHLC hit via e:{ev_i} ({len(result)} bars)")
            future.set_result(result)

        self._client.register_callback(E_CANDLES, _on_any)
        # Compat: client novo tem sniffer global; deploy antigo so tem register_callback.
        _has_global = hasattr(self._client, "register_global_callback")
        if _has_global:
            self._client.register_global_callback(_on_any)
        else:
            for _ev in (10, 11, 1003, 282, 283):
                self._client.register_callback(_ev, _on_any)
        try:
            # Inscreve push de candles (e:98).
            try:
                await self._client.send_request(
                    98, [E_CANDLES, 10, 11, 282, 283], requires_response=False
                )
            except Exception:
                pass

            for p in pairs[:3]:
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

            # Pedidos: nomes pair/size (Chipa) e p/tf (igual ao response DevTools).
            for p in pairs[:3]:
                if future.done():
                    break
                from_ts = to_ts - max(size, 1) * max(count, 1)
                payloads = [
                    [{"pair": p, "size": size, "to": to_ts, "solid": True}],
                    [{"p": p, "tf": size, "to": to_ts, "solid": True}],
                    [{"pair": p, "size": size, "to": to_ts}],
                    [{"p": p, "tf": size, "to": to_ts}],
                    [{"pair": p, "size": size, "to": to_ts, "count": count}],
                    [{"p": p, "tf": size, "from": from_ts, "to": to_ts}],
                    [
                        {
                            "pair": p,
                            "size": size,
                            "from": from_ts,
                            "to": to_ts,
                            "solid": True,
                        }
                    ],
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
                    await asyncio.sleep(0.12)

            timeout = float(os.environ.get("OLYMPTRADE_CANDLE_TIMEOUT", "25") or 25)
            try:
                candles_data = await asyncio.wait_for(future, timeout=timeout)
            except asyncio.TimeoutError:
                top = sorted(seen_events.items(), key=lambda x: -x[1])[:12]
                samples = "; ".join(
                    f"e:{e}={sample_by_e.get(e, '')}" for e, _ in top[:5]
                )
                logger.error(
                    f"Timeout candles {pair}: sem OHLC e:18. Eventos: {top}. "
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
            self._client.unregister_callback(E_CANDLES, _on_any)
            if _has_global and hasattr(self._client, "unregister_global_callback"):
                self._client.unregister_global_callback(_on_any)
            else:
                for _ev in (10, 11, 1003, 282, 283):
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
