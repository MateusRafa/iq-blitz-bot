# api/balance.py
from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Dict, Optional

if TYPE_CHECKING:
    from olymptrade_ws.core.client import OlympTradeClient

logger = logging.getLogger(__name__)


class BalanceAPI:
    def __init__(self, client: "OlympTradeClient"):
        self._client = client

    async def subscribe_balance_updates(self) -> None:
        event_code_subscribe = 98
        event_code_balance = 55
        data = [event_code_balance]
        logger.info(
            f"Attempting to subscribe to balance updates (event {event_code_balance})..."
        )
        try:
            await self._client.send_request(
                event_code_subscribe, [data], requires_response=False
            )
        except Exception as e:
            logger.error(f"Failed to send subscription request for balance updates: {e}")
            raise

    def get_last_balance(self) -> Dict[str, Any]:
        balance_data = self._client.current_balance
        return balance_data or {}

    async def request_balance(
        self, account_id: int, group: str = "real"
    ) -> Optional[Dict[str, Any]]:
        event_code = 1068
        data = [{"account_id": account_id, "group": group}]
        try:
            return await self._client.send_request(
                event_code, data, requires_response=True
            )
        except Exception as e:
            logger.error(f"Failed to request balance using event {event_code}: {e}")
            return None

    async def get_balance(
        self, timeout: float = 10.0, poll_interval: float = 0.5
    ) -> dict:
        if not getattr(self._client, "_session_initialized", False):
            await self._client.initialize_session()
            self._client._session_initialized = True
        try:
            await self.subscribe_balance_updates()
        except Exception:
            pass
        balance = await self._client.wait_for_balance(
            timeout=timeout, poll_interval=poll_interval
        )
        return balance or {}
