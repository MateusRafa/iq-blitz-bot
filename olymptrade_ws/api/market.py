# api/market.py
import logging
import time
from typing import TYPE_CHECKING, Dict, Any, Optional, List, Union
from datetime import datetime, timezone

from olymptrade_ws.core.protocol import get_current_timestamp_ms

if TYPE_CHECKING:
    from olymptrade_ws.core.client import OlympTradeClient

logger = logging.getLogger(__name__)

class MarketAPI:
    def __init__(self, client: 'OlympTradeClient'):
        self._client = client

    async def subscribe_ticks(self, pair: str) -> None:
        """Subscribes to live price ticks (Event 1) for a given asset pair."""
        logger.info(f"Subscribing to ticks for {pair}...")
        # Logs show events 12 and 280 are sent when subscribing to a pair
        try:
            # Event 12
            await self._client.send_request(12, [{"pair": pair}], requires_response=True)
             # Event 280
            await self._client.send_request(280, [{"pair": pair}], requires_response=True)
            logger.info(f"Successfully sent tick subscription requests for {pair}.")
        except Exception as e:
            logger.error(f"Failed to subscribe to ticks for {pair}: {e}")
            raise

    async def unsubscribe_ticks(self, pair: str) -> None:
        """Unsubscribes from live price ticks for a given asset pair."""
        logger.info(f"Unsubscribing from ticks for {pair}...")
         # Logs show events 13 and 281 are sent when unsubscribing
        try:
             # Event 13
            await self._client.send_request(13, [{"pair": pair}], requires_response=True)
            # Event 281
            await self._client.send_request(281, [{"pair": pair}], requires_response=True)
            logger.info(f"Successfully sent tick unsubscription requests for {pair}.")
        except Exception as e:
            logger.error(f"Failed to unsubscribe from ticks for {pair}: {e}")
            raise
            
    async def get_candles(self, pair: str, size: int, count: int, end_time: Optional[Union[datetime, int]] = None) -> Optional[List[Dict[str, Any]]]:
        """Pede historico OHLC.

        Protocolo Olymp e pouco documentado: tenta e:10 e escuta e:1003 / e:11 /
        qualquer push com campos OHLC. Se nada vier, retorna None (timeout).
        """
        import asyncio
        import os

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

        # Variantes de payload vistas em clients nao oficiais
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
        future = loop.create_future()
        candle_events = (1003, 11, 283)

        def _extract(payload: Any) -> Optional[List[Dict[str, Any]]]:
            if isinstance(payload, list) and payload:
                if all(isinstance(x, dict) for x in payload):
                    # lista de candles OU lista com wrapper
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

        async def _on_msg(message: Dict[str, Any]):
            if future.done():
                return
            extracted = _extract(message.get("d"))
            if extracted:
                # filtra por par se o campo existir
                filtered = []
                for c in extracted:
                    if not isinstance(c, dict):
                        continue
                    pval = c.get("p") or c.get("pair")
                    if pval and str(pval) != str(pair):
                        continue
                    filtered.append(c)
                future.set_result(filtered or extracted)

        for ev in candle_events:
            self._client.register_callback(ev, _on_msg)
        # Catch-all: qualquer evento com OHLC (descoberta)
        async def _on_any(message: Dict[str, Any]):
            if future.done():
                return
            extracted = _extract(message.get("d"))
            if extracted:
                logger.info(
                    f"Candles via e:{message.get('e')} n={len(extracted)}"
                )
                await _on_msg(message)

        # Registra em eventos comuns de chart se conhecidos; tambem 1 (ticks) nao
        for ev in (1003, 11, 283, 10):
            self._client.register_callback(ev, _on_any)

        try:
            # Seleciona asset (alguns fluxos exigem)
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
                await asyncio.sleep(0.3)

            timeout = float(os.environ.get("OLYMPTRADE_CANDLE_TIMEOUT", "25") or 25)
            try:
                candles_data = await asyncio.wait_for(future, timeout=timeout)
            except asyncio.TimeoutError:
                logger.error(
                    f"Timeout aguardando candles OHLC para {pair} "
                    f"(e:10 enviado; sem e:1003/11). "
                    f"Abra o grafico no browser e capture o evento no DevTools."
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
            for ev in (1003, 11, 283, 10):
                try:
                    self._client.unregister_callback(ev, _on_msg)
                except Exception:
                    pass
                try:
                    self._client.unregister_callback(ev, _on_any)
                except Exception:
                    pass

    async def get_profitability(self, account_id: int) -> Optional[List[Dict[str, Any]]]:
        """Requests current profitability for assets (Event 182)."""
        logger.info(f"Requesting asset profitability for account {account_id}...")
        event_code = 182
        data = [{"account_id": account_id}]
        try:
            response = await self._client.send_request(event_code, data, requires_response=True)
            if response and response.get("e") == event_code:
                profit_data = response.get("d")
                if isinstance(profit_data, list):
                    logger.info(f"Received profitability for {len(profit_data)} assets.")
                    return profit_data
                else:
                     logger.error(f"Unexpected data format in profitability response: {profit_data}")
                     return None
            else:
                logger.error(f"Did not receive expected profitability response (e:{event_code}). Got: {response}")
                return None
        except Exception as e:
            logger.error(f"Failed to get profitability: {e}")
            return None

    async def select_asset(self, pair: str, category: str = "digital") -> Optional[Dict[str, Any]]:
         """Selects an asset, potentially retrieving strike/payout info (Events 95, 80)."""
         logger.info(f"Selecting asset {pair} (category: {category})...")
         event_code_select = 95
         event_code_strikes = 80 # Often follows e:95 in logs
         data = [{"cat": category, "pair": pair}]
         try:
             # Send e:95 request
             response_select = await self._client.send_request(event_code_select, data, requires_response=True)
             if not (response_select and response_select.get("e") == event_code_select):
                 logger.error(f"Failed to get confirmation for asset selection (e:{event_code_select}).")
                 # Decide if we should proceed to wait for strikes anyway
             
             logger.info(f"Asset {pair} selected. Waiting for strike/payout info (e:{event_code_strikes})...")
             # Event 80 seems to be pushed after 95, not a direct response.
             # We need a way to wait for a specific *unsolicited* event.
             # Option 1: Register a temporary callback for e:80 with a filter for the pair.
             # Option 2: Have a general e:80 callback update internal state, then retrieve it.
             
             # Using Option 1 (temporary callback) for demonstration:
             future = asyncio.get_running_loop().create_future()

             async def temp_strike_callback(message: Dict[str, Any]):
                 strike_data_list = message.get("d", [])
                 if isinstance(strike_data_list, list):
                     for item in strike_data_list:
                         # Check if this strike data is for the requested pair
                         if isinstance(item, dict) and item.get("p") == pair:
                              if not future.done():
                                   future.set_result(item) # Return the specific strike data for the pair
                              break # Found our pair

             self._client.register_callback(event_code_strikes, temp_strike_callback)
             
             try:
                 # Wait for the callback to set the future's result
                 strike_info = await asyncio.wait_for(future, timeout=settings.DEFAULT_RESPONSE_TIMEOUT)
                 logger.info(f"Received strike info for {pair}: {strike_info}")
                 return strike_info
             except asyncio.TimeoutError:
                  logger.error(f"Timeout waiting for strike info (e:{event_code_strikes}) for {pair}.")
                  return None
             finally:
                  # Always unregister the temporary callback
                  self._client.unregister_callback(event_code_strikes, temp_strike_callback)

         except Exception as e:
             logger.error(f"Failed during asset selection/strike retrieval for {pair}: {e}")
             return None
