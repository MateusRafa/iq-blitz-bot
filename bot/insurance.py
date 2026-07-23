"""Seguro: pedido pendente apos lucro sustentado (nao substitui entradas a mercado)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from bot.clock import PocketClock
from bot.position import Cycle, Direction, PositionStatus
from bot.risk import RiskManager


@dataclass
class InsurancePlan:
    direction: Direction
    stake: float
    limit_price: float
    duration_seconds: int
    reason: str


def winning_side(cycle: Cycle, price: float) -> Direction | None:
    if cycle.primary_is_winning(price):
        return cycle.primary_direction()
    if cycle.primary_is_losing(price):
        return cycle.hedge_direction()
    return None


def is_favorable_for_insurance(cycle: Cycle, price: float, buffer: float) -> bool:
    """True se o lado vencedor ja projeta PnL >= buffer."""
    side = winning_side(cycle, price)
    if side is None:
        return False
    return cycle.projected_pnl_if(side) >= buffer


def build_insurance_plan(
    cycle: Cycle,
    risk: RiskManager,
    clock: PocketClock,
    price: float,
    now: datetime,
) -> InsurancePlan | None:
    """Calcula hedge/reforco oposto na entry da 1a (pedido pendente)."""
    if not cycle.open_positions:
        return None
    entry = cycle.primary_entry()
    primary = cycle.primary_direction()
    hedge = cycle.hedge_direction()
    if entry is None or primary is None or hedge is None:
        return None

    win = winning_side(cycle, price)
    if win is None:
        return None
    if cycle.projected_pnl_if(win) < risk.config.buffer:
        return None

    # Reversao: lado oposto ao que esta ganhando agora
    target: Direction = hedge if win == primary else primary

    anchor = cycle.anchor_expires_at
    if anchor is None:
        return None
    duration = clock.duration_to_anchor(now, anchor)
    if duration is None:
        return None

    payout = risk.config.payout
    # Conta OPEN + PENDING no livro para nao super-armar
    if target == "above":
        delta = risk.delta_to_cover(
            target_win_pool=cycle.win_pool_active("above"),
            other_stake=cycle.stake_active("below"),
            new_payout=payout,
        )
    else:
        delta = risk.delta_to_cover(
            target_win_pool=cycle.win_pool_active("below"),
            other_stake=cycle.stake_active("above"),
            new_payout=payout,
        )

    if delta <= 0:
        return None
    clamped = risk.clamp_stake(delta)
    if clamped is None:
        return None

    return InsurancePlan(
        direction=target,
        stake=clamped,
        limit_price=float(entry),
        duration_seconds=duration,
        reason=(
            f"seguro {target} @{entry} stake={clamped:.2f} "
            f"payout={payout * 100:.0f}%"
        ),
    )


def pending_covers_market_plan(cycle: Cycle, direction: Direction, stake: float) -> bool:
    """Se ja ha pendente no mesmo lado com stake suficiente, skip mercado."""
    pending = [
        p
        for p in cycle.pending_positions
        if p.direction == direction
    ]
    if not pending:
        return False
    return sum(p.stake for p in pending) + 1e-9 >= stake
