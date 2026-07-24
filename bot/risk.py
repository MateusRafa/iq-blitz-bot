"""Gestão de risco e cálculo de stake (Sa/Sb)."""

from __future__ import annotations

from dataclasses import dataclass
from math import ceil


@dataclass
class RiskConfig:
    base_stake: float = 1.0
    min_stake: float = 1.0
    buffer: float = 0.30
    payout: float = 0.85
    max_stake: float = 50.0
    max_levels: int = 8
    daily_loss_limit: float = 20.0


@dataclass
class RiskManager:
    config: RiskConfig
    daily_pnl: float = 0.0
    wins: int = 0
    losses: int = 0

    def set_payout(self, payout: float) -> None:
        """Atualiza payout vigente (fração, ex. 0.85). Usado no próximo ajuste."""
        if payout <= 0:
            return
        self.config.payout = float(payout)

    def stake_needed(self, other_side_stake: float) -> float:
        """S_need = (S_other + buffer) / payout para PnL >= buffer se o lado ganhar."""
        raw = (other_side_stake + self.config.buffer) / self.config.payout
        return round(raw, 2)

    def delta_stake(self, current_side_stake: float, other_side_stake: float) -> float:
        """Delta mínimo (>= min_stake) para cobrir o lado oposto + buffer (payout uniforme)."""
        need = self.stake_needed(other_side_stake)
        raw_delta = need - current_side_stake
        if raw_delta <= 0:
            return 0.0
        cents = ceil(raw_delta * 100)
        delta = cents / 100.0
        return max(delta, self.config.min_stake)

    def delta_to_cover(
        self,
        *,
        target_win_pool: float,
        other_stake: float,
        new_payout: float | None = None,
    ) -> float:
        """Delta na direção alvo para PnL projetado >= buffer com o payout da nova ordem.

        target_win_pool = soma(stake * payout) já aberta no lado que queremos vencedor
        other_stake     = soma(stake) do lado perdedor
        new_payout      = payout atual do mercado (default = config)
        """
        p = self.config.payout if new_payout is None else float(new_payout)
        if p <= 0:
            return 0.0
        # target_win_pool + delta*p - other_stake >= buffer
        raw = (self.config.buffer - target_win_pool + other_stake) / p
        if raw <= 0:
            return 0.0
        cents = ceil(raw * 100)
        delta = cents / 100.0
        return max(delta, self.config.min_stake)

    def clamp_stake(self, stake: float) -> float | None:
        if stake > self.config.max_stake:
            return None
        return max(stake, self.config.min_stake)

    def can_open_level(self, level: int) -> bool:
        return level < self.config.max_levels

    def hit_daily_stop(self) -> bool:
        return self.daily_pnl <= -abs(self.config.daily_loss_limit)

    def register_pnl(self, pnl: float) -> None:
        self.daily_pnl += pnl
        if pnl > 0:
            self.wins += 1
        elif pnl < 0:
            self.losses += 1

    def reset_session_stats(self) -> None:
        self.daily_pnl = 0.0
        self.wins = 0
        self.losses = 0

