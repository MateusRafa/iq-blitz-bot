"""Broker simulado para validar a FSM sem Pocket Option."""

from __future__ import annotations

from datetime import datetime

from bot.assets import choose_asset
from bot.position import Direction


class MockBroker:
    def __init__(self, price: float = 505.0) -> None:
        self.price = price
        self.orders: list[dict] = []
        self.limits: list[dict] = []
        self._pending_ids: set[str] = set()
        self.payout = 0.85
        self.payouts: dict[str, float] = {}
        self.feed_asset: str | None = None
        self.switched: list[str] = []

    def set_price(self, price: float) -> None:
        self.price = price

    def get_price(self, asset: str) -> float:
        return self.price

    def get_payout(self, asset: str) -> float:
        if asset in self.payouts:
            return self.payouts[asset]
        return getattr(self, "payout", 0.85)

    def set_payout(self, payout: float) -> None:
        self.payout = payout

    def list_payouts(self) -> dict[str, float]:
        if self.payouts:
            return dict(self.payouts)
        return {}

    def select_asset(
        self,
        current: str,
        *,
        target: float = 0.92,
        otc_only: bool | None = None,
    ) -> tuple[str, float, str]:
        table = self.list_payouts()
        if current not in table:
            table[current] = self.get_payout(current)
        return choose_asset(
            table, current=current, target=target, otc_only=otc_only
        )

    def switch_asset(self, asset: str, *, wait_seconds: float = 8.0) -> float | None:
        self.feed_asset = asset
        self.switched.append(asset)
        return self.price

    def place_pending(
        self,
        asset: str,
        direction: Direction,
        stake: float,
        price: float,
        expiration_seconds: int,
        *,
        opened_at: datetime | None = None,
    ) -> str | None:
        if getattr(self, "block_pending", False):
            # Simula API que cria/timeout sem devolver id
            self.limits.append(
                {
                    "id": "ghost",
                    "kind": "pending",
                    "asset": asset,
                    "direction": direction,
                    "stake": stake,
                    "limit_price": price,
                    "expiration_seconds": expiration_seconds,
                }
            )
            return None
        order_id = f"mock-pend-{len(self.limits) + 1}"
        self.limits.append(
            {
                "id": order_id,
                "kind": "pending",
                "asset": asset,
                "direction": direction,
                "stake": stake,
                "limit_price": price,
                "expiration_seconds": expiration_seconds,
            }
        )
        self._pending_ids.add(order_id)
        return order_id

    def cancel_pending(self, order_id: str) -> bool:
        self._pending_ids.discard(str(order_id))
        self.limits = [x for x in self.limits if x.get("id") != order_id]
        return True

    def pending_still_open(self, order_id: str) -> bool:
        return str(order_id) in self._pending_ids

    def fill_pending(self, order_id: str) -> None:
        """Simula fill do seguro."""
        self._pending_ids.discard(str(order_id))

    def load_allowed_durations(self, asset: str) -> tuple[int, ...]:
        return (5, 10, 15, 20, 30, 60)

    def open_order(
        self,
        asset: str,
        direction: Direction,
        stake: float,
        expiration_seconds: int,
        *,
        opened_at: datetime | None = None,
    ) -> tuple[str, float, datetime]:
        opened = opened_at or datetime.now().astimezone()
        order_id = f"mock-{len(self.orders) + len(self.limits) + 1}"
        self.orders.append(
            {
                "id": order_id,
                "kind": "market",
                "asset": asset,
                "direction": direction,
                "stake": stake,
                "entry": self.price,
                "opened_at": opened,
                "expiration_seconds": expiration_seconds,
            }
        )
        return order_id, self.price, opened

    def place_limit(
        self,
        asset: str,
        direction: Direction,
        stake: float,
        price: float,
        expiration_seconds: int,
        *,
        opened_at: datetime | None = None,
    ) -> tuple[str, float, datetime]:
        if getattr(self, "limit_falls_to_market", False):
            return self.open_order(
                asset,
                direction,
                stake,
                expiration_seconds,
                opened_at=opened_at,
            )
        opened = opened_at or datetime.now().astimezone()
        order_id = f"mock-lim-{len(self.orders) + len(self.limits) + 1}"
        self.limits.append(
            {
                "id": order_id,
                "kind": "limit",
                "asset": asset,
                "direction": direction,
                "stake": stake,
                "limit_price": price,
                "opened_at": opened,
                "expiration_seconds": expiration_seconds,
            }
        )
        self._pending_ids.add(order_id)
        return order_id, price, opened

    def wait_limit_fill(self, order_id: str, *, timeout: float = 0.5) -> bool:
        """No mock a 1ª pendente 'executa' no proximo poll."""
        if order_id in self._pending_ids:
            self._pending_ids.discard(order_id)
            return True
        return True
