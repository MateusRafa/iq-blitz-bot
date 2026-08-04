# api/market.py
import asyncio
import logging
import os
import time
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

if TYPE_CHECKING:
    from olymptrade_ws.core.client import OlympTradeClient

logger = logging.getLogger(__name__)

_OHLC_KEYS = ("open", "high", "low", "close", "o", "h", "l", "c")


def _pair_aliases(pair: str) -> List[str]:
    """DevTools mostra EURUSD_OTC (OTC maiusculo). Aceita variantes do env."""
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
        root.upper() + "_OTC" if root else "",
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
    has_time = any(k in item for k in ("t", "time", "timestamp", "from", "tf", "ts"))
    return has_ohlc and (has_time or "p" in item or "pair" in item)


def _extract_candles(payload: Any) -> Optional[List[Dict[str, Any]]]:
    if isinstance(payload, list) and payload:
        if all(_looks_like_candle(x) for x in payload[: min(5, len(payload))]):
            return [x for x in payload if isinstance(x, dict)]
        if all(isinstance(x, dict) for x in payload):
            if any(_looks_like_candle(x) for x in payload):
                return [x for x in payload if _looks_like_candle(x)]
            for x in payload:
                nested = x.get("candles") if isinstance(x, dict) else None
                if isinstance(nested, list) and nested:
                    got = _extract_candles(nested)
                    if got:
                        return got
    if isinstance(payload, dict):
        for key in ("candles", "d", "data", "history", "bars", "ohlc"):
            nested = payload.get(key)
            if isinstance(nested, list) and nested:
                got = _extract_candles(nested)
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
        """Pede historico OHLC. Sniffa TODOS os eventos (protocolo Chipa e:10→1003 e incerto)."""
        if end_time is None:
            to_ts = int(time.time())
        elif isinstance(end_time, datetime):
            if end_time.tzinfo is None:
                end_time = end_time.replace(tzinfo=timezone.utc)
            to_ts = int(end_time.timestamp())
        else:
            to_ts = int(end_time)

        pairs = _pair_aliases(pair)
        sizes = [size]
        if size != 60:
            sizes.append(60)
        if size != 3600:
            sizes.append(3600)

        logger.info(
            f"Requesting candles pair={pair} aliases={pairs} size={size}s "
            f"count={count} to={to_ts}"
        )

        loop = asyncio.get_running_loop()
        future: asyncio.Future = loop.create_future()
        seen_events: Dict[int, int] = {}
        sample_by_e: Dict[int, str] = {}

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
                    d = message.get("d")
                    sample_by_e[ev_i] = repr(d)[:220]

            extracted = _extract_candles(message.get("d"))
            if not extracted:
                extracted = _extract_candles(message)
            if not extracted:
                return

            filtered: List[Dict[str, Any]] = []
            for c in extracted:
                if not isinstance(c, dict):
                    continue
                pval = c.get("p") or c.get("pair")
                if pval and str(pval) not in pairs and str(pval) != str(pair):
                    # Aceita mesmo assim se so veio um lote (par pode ser id numerico).
                    filtered.append(c)
                    continue
                filtered.append(c)
            logger.info(
                f"OHLC hit via e:{ev_i} ({len(filtered or extracted)} bars)"
            )
            future.set_result(filtered or extracted)

        self._client.register_global_callback(_on_any)
        try:
            for p in pairs:
                try:
                    await self._client.send_request(
                        95,
                        [{"cat": "digital", "pair": p}],
                        requires_response=False,
                    )
                    await self._client.send_request(
                        95,
                        [{"cat": "forex", "pair": p}],
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

            for p in pairs:
                if future.done():
                    break
                for sz in sizes:
                    if future.done():
                        break
                    from_ts = to_ts - max(sz, 1) * max(count, 1)
                    payloads = [
                        [{"pair": p, "size": sz, "to": to_ts, "solid": True}],
                        [{"pair": p, "size": sz, "to": to_ts}],
                        [{"pair": p, "size": sz, "to": to_ts, "count": count}],
                        [{"pair": p, "size": sz, "from": from_ts, "to": to_ts}],
                        [
                            {
                                "pair": p,
                                "size": sz,
                                "from": from_ts,
                                "to": to_ts,
                                "solid": True,
                            }
                        ],
                    ]
                    for data in payloads:
                        if future.done():
                            break
                        for ev in (10, 282, 11, 283, 1003):
                            try:
                                await self._client.send_request(
                                    ev, data, requires_response=False
                                )
                            except Exception as e:
                                logger.debug(f"candle send e:{ev} failed: {e}")
                        await asyncio.sleep(0.15)

            timeout = float(os.environ.get("OLYMPTRADE_CANDLE_TIMEOUT", "20") or 20)
            try:
                candles_data = await asyncio.wait_for(future, timeout=timeout)
            except asyncio.TimeoutError:
                top = sorted(seen_events.items(), key=lambda x: -x[1])[:12]
                samples = "; ".join(
                    f"e:{e}={sample_by_e.get(e, '')}" for e, _ in top[:5]
                )
                logger.error(
                    f"Timeout candles {pair}: sem OHLC. Eventos vistos: {top}. "
                    f"Amostras: {samples}. "
                    "Defina OLYMPTRADE_PAIR=EURUSD_OTC e capture no DevTools "
                    "o frame WS com open/high/low/close (campo e=)."
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
            self._client.unregister_global_callback(_on_any)

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
