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
        """Pede historico OHLC. Escuta e:1003/11/283 (nao e:10 — e:10 e so o pedido/ACK)."""
        if end_time is None:
            to_ts = int(time.time())
        elif isinstance(end_time, datetime):
            if end_time.tzinfo is None:
                end_time = end_time.replace(tzinfo=timezone.utc)
            to_ts = int(end_time.timestamp())
        else:
            to_ts = int(end_time)

        logger.info(
            f"Requesting candles pair={pair} size={size}s count={count} to={to_ts}"
        )

        payloads = [
            [{"pair": pair, "size": size, "to": to_ts, "solid": True}],
            [{"pair": pair, "size": size, "to": to_ts}],
            [{"pair": pair, "size": size, "to": to_ts, "count": count}],
            [
                {
                    "pair": pair,
                    "size": size,
                    "from": to_ts - max(size, 1) * max(count, 1),
                    "to": to_ts,
                }
            ],
        ]

        loop = asyncio.get_running_loop()
        future: asyncio.Future = loop.create_future()
        listen_events = (1003, 11, 283)

        def _extract(payload: Any) -> Optional[List[Dict[str, Any]]]:
            if isinstance(payload, list) and payload:
                if all(isinstance(x, dict) for x in payload):
                    if any(
                        ("open" in x or "o" in x or "close" in x or "c" in x)
                        for x in payload
                    ):
                        return payload  # type: ignore[return-value]
                    for x in payload:
                        nested = x.get("candles") if isinstance(x, dict) else None
                        if isinstance(nested, list) and nested:
                            return nested
            if isinstance(payload, dict):
                for key in ("candles", "d", "data", "history"):
                    nested = payload.get(key)
                    if isinstance(nested, list) and nested:
                        return nested
            return None

        async def _on_msg(message: Dict[str, Any]) -> None:
            if future.done():
                return
            extracted = _extract(message.get("d"))
            if not extracted:
                return
            filtered: List[Dict[str, Any]] = []
            for c in extracted:
                if not isinstance(c, dict):
                    continue
                pval = c.get("p") or c.get("pair")
                if pval and str(pval) != str(pair):
                    continue
                filtered.append(c)
            future.set_result(filtered or extracted)

        for ev in listen_events:
            self._client.register_callback(ev, _on_msg)

        try:
            try:
                await self._client.send_request(
                    95,
                    [{"cat": "digital", "pair": pair}],
                    requires_response=False,
                )
            except Exception:
                pass

            for data in payloads:
                if future.done():
                    break
                try:
                    await self._client.send_request(10, data, requires_response=False)
                    await self._client.send_request(282, data, requires_response=False)
                except Exception as e:
                    logger.warning(f"candle request send failed: {e}")
                await asyncio.sleep(0.25)

            timeout = float(os.environ.get("OLYMPTRADE_CANDLE_TIMEOUT", "15") or 15)
            try:
                candles_data = await asyncio.wait_for(future, timeout=timeout)
            except asyncio.TimeoutError:
                logger.error(
                    f"Timeout candles {pair}: e:10/282 enviados, sem OHLC em "
                    f"e:1003/11/283. Capture no DevTools o frame com open/close."
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
            for ev in listen_events:
                self._client.unregister_callback(ev, _on_msg)

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
