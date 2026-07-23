"""Demo local: ciclo âncora-T 10s com MockBroker (sem Pocket live)."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from bot.broker_mock import MockBroker
from bot.clock import PocketClock
from bot.fsm import BotConfig, PocketFSM
from bot.risk import RiskConfig, RiskManager
from bot.strategy import Strategy


def main() -> None:
    broker = MockBroker(price=0.55652)
    risk = RiskManager(
        RiskConfig(
            base_stake=1.0,
            min_stake=1.0,
            buffer=0.30,
            payout=0.85,
            max_stake=50.0,
            max_levels=8,
            daily_loss_limit=20.0,
        )
    )
    fsm = PocketFSM(
        clock=PocketClock(),
        risk=risk,
        strategy=Strategy(initial_direction="above"),
        broker=broker,
        config=BotConfig(initial_duration_seconds=10),
    )

    print("Demo Pocket FSM (âncora-T 10s, min 5s, ajuste limite na 1ª)\n")

    t0 = datetime(2026, 7, 21, 13, 50, 0, tzinfo=timezone.utc)
    # t+0: mercado above @ 0.55652 / 10s → T=13:50:10
    # t+3: preço cai → limite below @ 0.55652 (âncora) / 7s
    # t+4: preço sobe → limite above reforço @ 0.55652 / 6s
    # t+10: settle
    steps: list[tuple[int, float]] = [
        (0, 0.55652),
        (3, 0.55640),
        (4, 0.55660),
        (10, 0.55660),
    ]

    for i, (offset_s, px) in enumerate(steps):
        broker.set_price(px)
        now = t0 + timedelta(seconds=offset_s)
        state = fsm.tick(now)
        open_pos = [
            f"{p.direction}:{p.stake}@{p.entry_price}->exp={p.expires_at.strftime('%H:%M:%S')}"
            for p in fsm.cycle.open_positions
        ]
        anchor = fsm.cycle.anchor_expires_at
        anchor_s = anchor.strftime("%H:%M:%S") if anchor else None
        print(
            f"[{i}] t=+{offset_s}s price={px} state={state.value} "
            f"T={anchor_s} levels={fsm.cycle.level} "
            f"Sa={fsm.cycle.stake_above():.2f} Sb={fsm.cycle.stake_below():.2f} "
            f"realized={fsm.cycle.realized_pnl:.2f} open={open_pos}"
        )

    print("stop_reason=", fsm.stop_reason)
    print("ordens mercado=", broker.orders)
    print("ordens limite=", broker.limits)


if __name__ == "__main__":
    main()
