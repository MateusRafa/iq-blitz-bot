"""Relógio do ciclo âncora-T (Pocket)."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from math import floor
from typing import Sequence


# Tempos comuns de NEGOCIACAO (UI Pocket). allowed_candles da API e de
# grafico e muitas vezes NAO inclui 10/20/30s — nao usar sozinho.
POCKET_TRADE_DURATIONS: tuple[int, ...] = (
    5,
    10,
    15,
    20,
    30,
    45,
    60,
    90,
    120,
    180,
    300,
    600,
    900,
    1800,
    3600,
)

# Alias legado
POCKET_COMMON_DURATIONS = POCKET_TRADE_DURATIONS


def snap_to_allowed(
    seconds: float,
    allowed: Sequence[int],
    *,
    min_duration: int = 5,
    max_duration: int = 4 * 60 * 60,
) -> int | None:
    """Maior duration da lista <= seconds (e >= min)."""
    if seconds < min_duration:
        return None
    capped = min(int(floor(seconds)), max_duration)
    chosen: int | None = None
    for d in sorted(int(x) for x in allowed):
        if d < min_duration:
            continue
        if d > capped:
            break
        chosen = d
    return chosen


@dataclass
class ClockConfig:
    min_duration_seconds: int = 5
    max_duration_seconds: int = 4 * 60 * 60
    initial_duration_seconds: int = 10


@dataclass
class PocketClock:
    config: ClockConfig = field(default_factory=ClockConfig)
    # Nao restringir ao allowed_candles da API (bloqueava 10/20/30s indevidamente).
    allowed_durations: tuple[int, ...] = POCKET_TRADE_DURATIONS

    def set_allowed_durations(self, durations: Sequence[int]) -> None:
        """Mescla tempos da API com tempos de trade da UI (5/10/15/20/30...)."""
        merged = set(POCKET_TRADE_DURATIONS)
        for d in durations:
            try:
                v = int(d)
            except (TypeError, ValueError):
                continue
            if self.config.min_duration_seconds <= v <= self.config.max_duration_seconds:
                merged.add(v)
        self.allowed_durations = tuple(sorted(merged))

    def expires_at(self, opened_at: datetime, duration_seconds: int) -> datetime:
        return opened_at + timedelta(seconds=duration_seconds)

    def remaining_to_anchor(self, now: datetime, anchor: datetime) -> float:
        return (anchor - now).total_seconds()

    def clamp_duration(self, seconds: float) -> int | None:
        """Floor do resto; se cair em valor 'estranho', snap para 30/20/10/5..."""
        if seconds < self.config.min_duration_seconds:
            return None
        raw = int(floor(seconds))
        if raw < self.config.min_duration_seconds:
            return None
        raw = min(raw, self.config.max_duration_seconds)
        # Preferir o valor exato (como antes, quando 8s/23s abriam no OTC).
        # Snap so se quisermos alinhar — usamos raw; broker faz fallback se API rejeitar.
        return raw

    def min_allowed_duration(self) -> int:
        return self.config.min_duration_seconds

    def duration_to_anchor(self, now: datetime, anchor: datetime) -> int | None:
        return self.clamp_duration(self.remaining_to_anchor(now, anchor))

    def can_open_adjustment(self, now: datetime, anchor: datetime | None) -> bool:
        if anchor is None:
            return True
        return self.duration_to_anchor(now, anchor) is not None

    def hold_reason(self, now: datetime, anchor: datetime | None) -> str | None:
        if anchor is None:
            return None
        rem = self.remaining_to_anchor(now, anchor)
        if rem < self.config.min_duration_seconds:
            return f"resto={rem:.1f}s < min {self.config.min_duration_seconds}s"
        return None
