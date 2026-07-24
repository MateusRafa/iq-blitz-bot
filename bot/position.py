"""Modelo de ordem / ciclo âncora-T (Pocket)."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Literal


Direction = Literal["above", "below"]


class PositionStatus(str, Enum):
    PENDING = "pending"
    OPEN = "open"
    WON = "won"
    LOST = "lost"


@dataclass
class Position:
    id: str
    direction: Direction
    stake: float
    entry_price: float
    opened_at: datetime
    expires_at: datetime
    payout: float  # ex.: 0.85
    status: PositionStatus = PositionStatus.OPEN
    pnl: float = 0.0

    def is_favorable(self, price: float, tie_as_against: bool = False) -> bool:
        """Empate NÃO conta como contra (só preço estritamente do outro lado)."""
        if self.direction == "above":
            if price > self.entry_price:
                return True
            if price < self.entry_price:
                return False
            return not tie_as_against
        if price < self.entry_price:
            return True
        if price > self.entry_price:
            return False
        return not tie_as_against

    def settle(self, exit_price: float) -> float:
        won = self.is_favorable(exit_price, tie_as_against=True)
        if won:
            self.status = PositionStatus.WON
            self.pnl = self.stake * self.payout
        else:
            self.status = PositionStatus.LOST
            self.pnl = -self.stake
        return self.pnl


@dataclass
class Cycle:
    """Ciclo de gestão com vencimento âncora T compartilhado.

    A direção de ajuste é ancorada na **1ª ordem** (entrada + direção),
    não em marcas misturadas de cada ordem com entry diferente.
    """

    positions: list[Position] = field(default_factory=list)
    realized_pnl: float = 0.0
    anchor_expires_at: datetime | None = None
    ladder_placed: bool = False

    @property
    def open_positions(self) -> list[Position]:
        return [p for p in self.positions if p.status == PositionStatus.OPEN]

    @property
    def pending_positions(self) -> list[Position]:
        return [p for p in self.positions if p.status == PositionStatus.PENDING]

    @property
    def level(self) -> int:
        return len(self.positions)

    def primary_direction(self) -> Direction | None:
        if not self.positions:
            return None
        return self.positions[0].direction

    def primary_entry(self) -> float | None:
        if not self.positions:
            return None
        return self.positions[0].entry_price

    def hedge_direction(self) -> Direction | None:
        primary = self.primary_direction()
        if primary is None:
            return None
        return "below" if primary == "above" else "above"

    def primary_is_winning(self, price: float) -> bool:
        """True se o preço está do lado vencedor da 1ª ordem (estrito)."""
        direction = self.primary_direction()
        entry = self.primary_entry()
        if direction is None or entry is None:
            return False
        if direction == "above":
            return price > entry
        return price < entry

    def primary_is_losing(self, price: float) -> bool:
        direction = self.primary_direction()
        entry = self.primary_entry()
        if direction is None or entry is None:
            return False
        if direction == "above":
            return price < entry
        return price > entry

    def stake_above(self) -> float:
        return sum(p.stake for p in self.open_positions if p.direction == "above")

    def stake_below(self) -> float:
        return sum(p.stake for p in self.open_positions if p.direction == "below")

    def stake_of(self, direction: Direction) -> float:
        return self.stake_above() if direction == "above" else self.stake_below()

    def stake_active(self, direction: Direction) -> float:
        """OPEN + PENDING (seguro armado conta na exposicao)."""
        return sum(
            p.stake
            for p in self.positions
            if p.direction == direction
            and p.status in (PositionStatus.OPEN, PositionStatus.PENDING)
        )

    def win_pool(self, direction: Direction) -> float:
        """Soma stake*payout do lado (cada ordem guarda o payout do momento da abertura)."""
        return sum(
            p.stake * p.payout
            for p in self.open_positions
            if p.direction == direction
        )

    def win_pool_active(self, direction: Direction) -> float:
        return sum(
            p.stake * p.payout
            for p in self.positions
            if p.direction == direction
            and p.status in (PositionStatus.OPEN, PositionStatus.PENDING)
        )

    def itm_win_pool(self, price: float) -> float:
        """Lucro potencial das ordens que ganhariam se liquidasse agora."""
        return sum(
            p.stake * p.payout
            for p in self.open_positions
            if p.is_favorable(price, tie_as_against=False)
        )

    def otm_lose_stake(self, price: float) -> float:
        """Stake das ordens que perderiam se liquidasse agora."""
        return sum(
            p.stake
            for p in self.open_positions
            if not p.is_favorable(price, tie_as_against=False)
        )

    def projected_pnl_if(self, winner: Direction) -> float:
        """PnL se o lado `winner` liquidar a favor e o oposto contra (modelo direcional)."""
        loser: Direction = "below" if winner == "above" else "above"
        return self.win_pool(winner) - self.stake_of(loser)

    def projected_pnl_if_active(self, winner: Direction) -> float:
        """Como projected_pnl_if, contando PENDING no win_pool do vencedor."""
        loser: Direction = "below" if winner == "above" else "above"
        return self.win_pool_active(winner) - self.stake_of(loser)

    def projected_mark_pnl(self, price: float) -> float:
        """PnL se todas as abertas liquidassem no preco atual (cada entry propria)."""
        return self.itm_win_pool(price) - self.otm_lose_stake(price)

    def favored_direction(self, price: float) -> Direction | None:
        """Lado que o mercado esta favorecendo agora (ancorado na 1ª)."""
        if self.primary_is_winning(price):
            return self.primary_direction()
        if self.primary_is_losing(price):
            return self.hedge_direction()
        return None

    def in_dead_zone(self, price: float) -> bool:
        """True se Acima e Abaixo abertos e ambos perderiam no preco atual."""
        if self.stake_above() <= 0 or self.stake_below() <= 0:
            return False
        return self.itm_win_pool(price) <= 0 and self.otm_lose_stake(price) > 0

    def target_direction(self, price: float) -> Direction | None:
        """Lado a reforçar: o que o preço esta favorecendo (gate de lucro)."""
        return self.favored_direction(price)

    def needs_adjustment(self, price: float, buffer: float) -> bool:
        """True se o lado favorecido ainda nao projeta PnL >= buffer.

        Gate de lucro (Sa/Sb). Fora da zona morta, tambem exige mark PnL >= buffer
        quando os dois lados estao abertos.
        """
        fav = self.favored_direction(price)
        if fav is None:
            return False

        hedge = self.hedge_direction()
        primary = self.primary_direction()
        # Preco a favor sem hedge: 1ª sozinha — so ajusta se win_pool < buffer
        if (
            primary is not None
            and hedge is not None
            and fav == primary
            and self.stake_of(hedge) <= 0
        ):
            return self.win_pool_active(primary) < buffer

        if self.projected_pnl_if_active(fav) < buffer:
            return True

        # Mark gate: livro Sa/Sb ok, mas entries divergentes deixam mark negativo
        if (
            not self.in_dead_zone(price)
            and self.stake_above() > 0
            and self.stake_below() > 0
            and self.projected_mark_pnl(price) < buffer
        ):
            return True

        return False

    def apply_settlement(self, position: Position, exit_price: float) -> float:
        pnl = position.settle(exit_price)
        self.realized_pnl += pnl
        return pnl

    def is_cleared(self) -> bool:
        return not self.open_positions and self.realized_pnl >= 0


# Alias legado para imports antigos em transição
Chain = Cycle
