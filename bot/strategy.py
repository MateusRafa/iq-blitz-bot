"""Decisão de abertura: 1ª a mercado; ajustes na âncora (limite) com fallback."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Literal

from bot.clock import PocketClock
from bot.position import Cycle, Direction
from bot.risk import RiskManager

OrderKind = Literal["market", "limit"]


def add_points(price: float, points: int = 5) -> float:
    """Soma `points` unidades no último decimal visível do preço.

    Ex.: 0.55652 + 5 pontos → 0.55657
    """
    text = format(price, ".10f").rstrip("0").rstrip(".")
    if "." in text:
        decimals = len(text.split(".")[1])
    else:
        decimals = 0
    step = 10 ** (-decimals) if decimals else 1.0
    return round(price + points * step, decimals if decimals else 0)


@dataclass
class TradePlan:
    direction: Direction
    stake: float
    duration_seconds: int
    reason: str
    order_kind: OrderKind = "market"
    limit_price: float | None = None


@dataclass
class Strategy:
    initial_direction: Direction = "above"
    risk: RiskManager | None = None
    clock: PocketClock | None = None
    initial_duration_seconds: int = 10
    limit_points: int = 0
    # Pendente cola na âncora, mas a API Pocket pode falhar (_placeholder).
    # Default mercado; ative com POCKET_USE_LIMIT=1 quando pendente funcionar.
    use_limit_for_adjust: bool = False

    def decide(
        self,
        cycle: Cycle,
        price: float,
        now: datetime,
    ) -> TradePlan | None:
        assert self.risk is not None
        assert self.clock is not None
        risk = self.risk
        clock = self.clock

        if risk.hit_daily_stop():
            return None

        # Nova ciclo — 1ª ordem a mercado
        if not cycle.positions:
            stake = risk.clamp_stake(risk.config.base_stake)
            if stake is None:
                return None
            duration = clock.clamp_duration(self.initial_duration_seconds)
            if duration is None:
                return None
            return TradePlan(
                direction=self.initial_direction,
                stake=stake,
                duration_seconds=duration,
                reason="base_open",
                order_kind="market",
            )

        if not risk.can_open_level(cycle.level):
            return None

        anchor = cycle.anchor_expires_at
        if anchor is None:
            return None

        duration = clock.duration_to_anchor(now, anchor)
        if duration is None:
            return None  # resto < min → HOLD na FSM

        if not cycle.needs_adjustment(price, risk.config.buffer):
            return None

        target = cycle.target_direction(price)
        if target is None:
            return None

        sa = cycle.stake_above()
        sb = cycle.stake_below()
        payout = risk.config.payout
        # Mesmo lado: OPEN+PENDING cobre. Lado oposto: so OPEN (pendente
        # oposto e seguro de reversao, nao perda ate o fill).
        if target == "above":
            delta = risk.delta_to_cover(
                target_win_pool=cycle.win_pool_active("above"),
                other_stake=cycle.stake_of("below"),
                new_payout=payout,
            )
        else:
            delta = risk.delta_to_cover(
                target_win_pool=cycle.win_pool_active("below"),
                other_stake=cycle.stake_of("above"),
                new_payout=payout,
            )

        if delta <= 0:
            return None

        clamped = risk.clamp_stake(delta)
        if clamped is None:
            return None

        order_kind: OrderKind = "market"
        limit_price: float | None = None
        if self.use_limit_for_adjust:
            first_entry = cycle.positions[0].entry_price
            order_kind = "limit"
            limit_price = add_points(first_entry, self.limit_points)

        return TradePlan(
            direction=target,
            stake=clamped,
            duration_seconds=duration,
            reason=(
                f"adjust {target} Sa={sa:.2f} Sb={sb:.2f} "
                f"payout={payout * 100:.0f}%"
            ),
            order_kind=order_kind,
            limit_price=limit_price,
        )
