"""Testes do ciclo âncora-T (ordens a mercado, Sa/Sb)."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from bot.broker_mock import MockBroker
from bot.clock import PocketClock
from bot.fsm import BotConfig, PocketFSM
from bot.position import Cycle, Position
from bot.risk import RiskConfig, RiskManager
from bot.strategy import Strategy, add_points


def test_stake_needed_covers_buffer():
    risk = RiskManager(RiskConfig(buffer=0.02, payout=0.85, min_stake=1.0))
    need = risk.stake_needed(1.0)
    assert need == 1.2
    assert round(need * 0.85 - 1.0, 2) == 0.02


def test_delta_stake_respects_min_one_dollar():
    risk = RiskManager(RiskConfig(buffer=0.02, payout=0.85, min_stake=1.0))
    assert risk.delta_stake(1.0, 1.11) == 1.0


def test_delta_stake_when_already_covered():
    risk = RiskManager(RiskConfig(buffer=0.02, payout=0.85, min_stake=1.0))
    assert risk.delta_stake(2.0, 1.11) == 0.0


def test_add_points_five_on_forex_price():
    assert add_points(0.55652, 5) == 0.55657


def test_clamp_duration_min_5s():
    clock = PocketClock()
    assert clock.clamp_duration(4.9) is None
    assert clock.clamp_duration(5.0) == 5
    assert clock.clamp_duration(7.1) == 7


def test_duration_to_anchor_floor():
    clock = PocketClock()
    now = datetime(2026, 7, 21, 13, 50, 0, tzinfo=timezone.utc)
    anchor = now + timedelta(seconds=60)
    assert clock.duration_to_anchor(now + timedelta(seconds=8), anchor) == 52
    assert clock.duration_to_anchor(now + timedelta(seconds=56), anchor) is None


def test_merge_keeps_short_trade_times():
    clock = PocketClock()
    # API so manda >=60 (grafico); merge deve manter 10/20/30
    clock.set_allowed_durations([60, 120, 180, 300])
    assert 10 in clock.allowed_durations
    assert 30 in clock.allowed_durations
    assert clock.clamp_duration(52) == 52


def test_first_market_adjust_limit_at_anchor():
    risk = RiskManager(RiskConfig())
    clock = PocketClock()
    strategy = Strategy(
        risk=risk,
        clock=clock,
        initial_duration_seconds=10,
        use_limit_for_adjust=True,
    )
    now = datetime(2026, 7, 21, 13, 50, 0, tzinfo=timezone.utc)
    plan0 = strategy.decide(Cycle(), price=0.55652, now=now)
    assert plan0 is not None
    assert plan0.order_kind == "market"

    T = now + timedelta(seconds=10)
    cycle = Cycle(anchor_expires_at=T)
    cycle.positions.append(
        Position(
            id="1",
            direction="above",
            stake=1.0,
            entry_price=0.55652,
            opened_at=now,
            expires_at=T,
            payout=0.85,
        )
    )
    plan = strategy.decide(cycle, price=0.55640, now=now + timedelta(seconds=3))
    assert plan is not None
    assert plan.direction == "below"
    assert plan.order_kind == "limit"
    assert plan.limit_price == 0.55652
    assert plan.stake == 1.53
    assert plan.duration_seconds == 7


def test_adjust_can_force_market():
    risk = RiskManager(RiskConfig())
    clock = PocketClock()
    strategy = Strategy(
        risk=risk,
        clock=clock,
        initial_duration_seconds=10,
        use_limit_for_adjust=False,
    )
    now = datetime(2026, 7, 21, 13, 50, 0, tzinfo=timezone.utc)
    T = now + timedelta(seconds=10)
    cycle = Cycle(anchor_expires_at=T)
    cycle.positions.append(
        Position(
            id="1",
            direction="above",
            stake=1.0,
            entry_price=0.55652,
            opened_at=now,
            expires_at=T,
            payout=0.85,
        )
    )
    plan = strategy.decide(cycle, price=0.55640, now=now + timedelta(seconds=3))
    assert plan is not None
    assert plan.order_kind == "market"


def test_no_hedge_while_primary_winning_without_opposite():
    risk = RiskManager(RiskConfig())
    clock = PocketClock()
    strategy = Strategy(risk=risk, clock=clock, initial_duration_seconds=60)
    now = datetime(2026, 7, 21, 13, 50, 0, tzinfo=timezone.utc)
    T = now + timedelta(seconds=120)
    cycle = Cycle(anchor_expires_at=T)
    cycle.positions.append(
        Position(
            id="1",
            direction="above",
            stake=1.0,
            entry_price=1.08110,
            opened_at=now,
            expires_at=T,
            payout=0.85,
        )
    )
    assert strategy.decide(cycle, price=1.08170, now=now + timedelta(seconds=30)) is None


def test_hedge_scales_with_book_not_mark_zone():
    """Entre entries divergentes nao explode stake (evita zona morta ITM/OTM)."""
    risk = RiskManager(RiskConfig(buffer=0.30, payout=0.92, min_stake=1.0))
    clock = PocketClock()
    strategy = Strategy(risk=risk, clock=clock, initial_duration_seconds=60)
    now = datetime(2026, 7, 21, 13, 50, 0, tzinfo=timezone.utc)
    T = now + timedelta(seconds=120)
    cycle = Cycle(anchor_expires_at=T)
    cycle.positions.extend(
        [
            Position(
                id="1",
                direction="above",
                stake=1.0,
                entry_price=0.57005,
                opened_at=now,
                expires_at=T,
                payout=0.92,
            ),
            Position(
                id="2",
                direction="below",
                stake=1.42,
                entry_price=0.56995,
                opened_at=now,
                expires_at=T,
                payout=0.92,
            ),
        ]
    )
    # Preco entre as duas entradas (zona morta na Pocket) — nao pode abrir ~$2.96
    price = 0.57000
    plan = strategy.decide(cycle, price=price, now=now + timedelta(seconds=30))
    # Empate na 1a → sem alvo
    assert plan is None

    # Preco a favor da 1a com hedge aberto → reforco above controlado
    plan = strategy.decide(cycle, price=0.57020, now=now + timedelta(seconds=30))
    assert plan is not None
    assert plan.direction == "above"
    # win_pool above=0.92; other=1.42; delta=(0.30-0.92+1.42)/0.92=0.87→min 1.0
    assert plan.stake == 1.0


def test_hedge_when_primary_losing():
    risk = RiskManager(RiskConfig())
    clock = PocketClock()
    strategy = Strategy(risk=risk, clock=clock, initial_duration_seconds=60)
    now = datetime(2026, 7, 21, 13, 50, 0, tzinfo=timezone.utc)
    T = now + timedelta(seconds=120)
    cycle = Cycle(anchor_expires_at=T)
    cycle.positions.append(
        Position(
            id="1",
            direction="above",
            stake=1.0,
            entry_price=1.08110,
            opened_at=now,
            expires_at=T,
            payout=0.85,
        )
    )
    plan = strategy.decide(cycle, price=1.08080, now=now + timedelta(seconds=30))
    assert plan is not None
    assert plan.direction == "below"
    assert plan.stake == 1.53


def test_lower_payout_increases_adjust_stake():
    """Payout cai no meio do ciclo → cruzamento com stake maior para manter buffer."""
    risk = RiskManager(RiskConfig(buffer=0.02, payout=0.70, min_stake=1.0))
    clock = PocketClock()
    strategy = Strategy(risk=risk, clock=clock, initial_duration_seconds=60)
    now = datetime(2026, 7, 21, 13, 50, 0, tzinfo=timezone.utc)
    T = now + timedelta(seconds=120)
    cycle = Cycle(anchor_expires_at=T)
    cycle.positions.append(
        Position(
            id="1",
            direction="above",
            stake=1.0,
            entry_price=1.08110,
            opened_at=now,
            expires_at=T,
            payout=0.85,  # ordem antiga travada em 85%
        )
    )
    plan = strategy.decide(cycle, price=1.08080, now=now + timedelta(seconds=30))
    assert plan is not None
    assert plan.direction == "below"
    # (1.0 + 0.02) / 0.70 = 1.457... → ceil cents → 1.46, min_stake 1 → 1.46
    assert plan.stake == 1.46
    assert "70%" in plan.reason


def test_fsm_adapts_payout_on_cross():
    broker = MockBroker(price=0.55652)
    broker.set_payout(0.85)
    fsm = PocketFSM(
        clock=PocketClock(),
        risk=RiskManager(RiskConfig(payout=0.85, buffer=0.02)),
        strategy=Strategy(initial_direction="above", use_limit_for_adjust=True),
        broker=broker,
        config=BotConfig(initial_duration_seconds=10, preplace_limit=False),
    )
    t0 = datetime(2026, 7, 21, 13, 50, 0, tzinfo=timezone.utc)
    fsm.tick(t0)
    assert fsm.cycle.positions[0].payout == 0.85

    broker.set_payout(0.70)
    broker.set_price(0.55640)
    fsm.tick(t0 + timedelta(seconds=3))
    assert fsm.cycle.level == 2
    assert abs(fsm.risk.config.payout - 0.70) < 1e-9
    assert fsm.cycle.positions[-1].payout == 0.70
    assert fsm.cycle.positions[-1].stake == 1.46


def test_fsm_same_expires_at_mid_cycle():
    broker = MockBroker(price=0.55652)
    fsm = PocketFSM(
        clock=PocketClock(),
        risk=RiskManager(RiskConfig()),
        strategy=Strategy(initial_direction="above", use_limit_for_adjust=True),
        broker=broker,
        config=BotConfig(
            initial_duration_seconds=10,
            adjust_cooldown_seconds=0,
            preplace_limit=False,
        ),
    )
    t0 = datetime(2026, 7, 21, 13, 50, 0, tzinfo=timezone.utc)
    fsm.tick(t0)
    assert fsm.cycle.level == 1
    T = fsm.cycle.anchor_expires_at
    assert T == t0 + timedelta(seconds=10)
    assert broker.orders[-1]["kind"] == "market"

    broker.set_price(0.55640)
    fsm.tick(t0 + timedelta(seconds=3))
    assert fsm.cycle.level == 2
    assert {p.expires_at for p in fsm.cycle.open_positions} == {T}
    below = next(p for p in fsm.cycle.open_positions if p.direction == "below")
    assert below.entry_price == 0.55652  # colado na 1ª

    broker.set_price(0.55660)
    fsm.tick(t0 + timedelta(seconds=4))
    assert fsm.cycle.level == 3
    assert {p.expires_at for p in fsm.cycle.open_positions} == {T}

    broker.set_price(0.55660)
    fsm.tick(t0 + timedelta(seconds=10))
    assert fsm.state.value == "IDLE"


def test_hold_when_remaining_under_5s():
    broker = MockBroker(price=0.55652)
    fsm = PocketFSM(
        clock=PocketClock(),
        risk=RiskManager(RiskConfig()),
        strategy=Strategy(initial_direction="above"),
        broker=broker,
        config=BotConfig(initial_duration_seconds=10),
    )
    t0 = datetime(2026, 7, 21, 13, 50, 0, tzinfo=timezone.utc)
    fsm.tick(t0)
    broker.set_price(0.55640)
    state = fsm.tick(t0 + timedelta(seconds=6))
    assert state.value == "HOLD"
    assert fsm.cycle.level == 1


def test_choose_asset_keeps_target():
    from bot.assets import choose_asset

    asset, p, reason = choose_asset(
        {"AUDCHF_otc": 0.92, "EURUSD_otc": 0.95},
        current="AUDCHF_otc",
        target=0.92,
    )
    assert asset == "AUDCHF_otc"
    assert reason == "mantem_target"
    assert p == 0.92


def test_choose_asset_switches_to_target():
    from bot.assets import choose_asset

    asset, p, reason = choose_asset(
        {"AUDCHF_otc": 0.75, "EURUSD_otc": 0.92, "GBPUSD_otc": 0.93},
        current="AUDCHF_otc",
        target=0.92,
    )
    assert asset == "GBPUSD_otc"
    assert p == 0.93
    assert reason == "troca_target"


def test_choose_asset_falls_back_to_best():
    from bot.assets import choose_asset

    asset, p, reason = choose_asset(
        {"AUDCHF_otc": 0.70, "EURUSD_otc": 0.88, "GBPUSD_otc": 0.80},
        current="AUDCHF_otc",
        target=0.92,
    )
    assert asset == "EURUSD_otc"
    assert p == 0.88
    assert reason == "melhor_disponivel"


def test_fsm_does_not_switch_mid_cycle():
    broker = MockBroker(price=0.55652)
    broker.payouts = {"AUDCHF_otc": 0.75, "EURUSD_otc": 0.92}
    fsm = PocketFSM(
        clock=PocketClock(),
        risk=RiskManager(RiskConfig(payout=0.75, buffer=0.30)),
        strategy=Strategy(initial_direction="above"),
        broker=broker,
        config=BotConfig(
            asset="AUDCHF_otc",
            initial_duration_seconds=10,
            target_payout=0.92,
            auto_switch_asset=True,
            preplace_limit=False,
        ),
    )
    t0 = datetime(2026, 7, 21, 13, 50, 0, tzinfo=timezone.utc)
    fsm.tick(t0)
    assert fsm.config.asset == "AUDCHF_otc"
    assert fsm.cycle.level == 1
    # payout cai / outro ativo melhor — ainda no meio da rodada
    broker.payouts["AUDCHF_otc"] = 0.60
    fsm.ensure_asset()
    assert fsm.config.asset == "AUDCHF_otc"
    assert broker.switched == []


def test_fsm_switches_after_settle():
    broker = MockBroker(price=0.55652)
    broker.payouts = {"AUDCHF_otc": 0.92, "EURUSD_otc": 0.92}
    fsm = PocketFSM(
        clock=PocketClock(),
        risk=RiskManager(RiskConfig(payout=0.92, buffer=0.30)),
        strategy=Strategy(initial_direction="above"),
        broker=broker,
        config=BotConfig(
            asset="AUDCHF_otc",
            initial_duration_seconds=10,
            target_payout=0.92,
            auto_switch_asset=True,
            preplace_limit=False,
        ),
    )
    t0 = datetime(2026, 7, 21, 13, 50, 0, tzinfo=timezone.utc)
    fsm.ensure_asset()
    assert fsm.config.asset == "AUDCHF_otc"
    fsm.tick(t0)
    assert fsm.cycle.level == 1

    broker.payouts["AUDCHF_otc"] = 0.70
    broker.payouts["EURUSD_otc"] = 0.92
    # settle no T
    fsm.tick(t0 + timedelta(seconds=10))
    assert fsm.state.value == "IDLE"
    assert fsm.config.asset == "EURUSD_otc"
    assert "EURUSD_otc" in broker.switched


def test_insurance_arms_after_sustain_and_skips_market():
    broker = MockBroker(price=0.55660)
    broker.set_payout(0.85)
    fsm = PocketFSM(
        clock=PocketClock(),
        risk=RiskManager(RiskConfig(payout=0.85, buffer=0.30)),
        strategy=Strategy(initial_direction="above"),
        broker=broker,
        config=BotConfig(
            asset="AUDCHF_otc",
            initial_duration_seconds=60,
            auto_switch_asset=False,
            preplace_limit=True,
            preplace_sustain_seconds=10.0,
        ),
    )
    t0 = datetime(2026, 7, 21, 13, 50, 0, tzinfo=timezone.utc)
    fsm.tick(t0)  # 1a above @ 0.55660
    assert fsm.cycle.level == 1
    assert not fsm.cycle.pending_positions

    # Preco a favor (acima da entry)
    broker.set_price(0.55670)

    # Favoravel: inicia contagem
    fsm.tick(t0 + timedelta(seconds=1))
    assert not fsm.cycle.pending_positions

    # Ainda nao sustentou 10s
    fsm.tick(t0 + timedelta(seconds=5))
    assert not fsm.cycle.pending_positions

    # Apos 10s+ a favor → arma seguro below na entry
    fsm.tick(t0 + timedelta(seconds=12))
    assert len(fsm.cycle.pending_positions) == 1
    pend = fsm.cycle.pending_positions[0]
    assert pend.direction == "below"
    assert pend.stake == 1.53
    assert pend.entry_price == 0.55660

    # Preco desaba: pendente preenche na barreira → nao abre mercado extra
    broker.set_price(0.55640)
    orders_before = len(broker.orders)
    fsm.tick(t0 + timedelta(seconds=13))
    assert len(broker.orders) == orders_before
    assert not fsm.cycle.pending_positions
    assert any(
        p.direction == "below" and p.status.value == "open" for p in fsm.cycle.positions
    )


def test_insurance_arms_only_once_per_window():
    """Mesmo sem id da API, nao reenvia seguro a cada tick."""
    broker = MockBroker(price=0.55660)
    broker.set_payout(0.85)
    broker.block_pending = True  # place_pending retorna None
    fsm = PocketFSM(
        clock=PocketClock(),
        risk=RiskManager(RiskConfig(payout=0.85, buffer=0.30)),
        strategy=Strategy(initial_direction="above"),
        broker=broker,
        config=BotConfig(
            asset="AUDCHF_otc",
            initial_duration_seconds=60,
            auto_switch_asset=False,
            preplace_limit=True,
            preplace_sustain_seconds=5.0,
        ),
    )
    t0 = datetime(2026, 7, 21, 13, 50, 0, tzinfo=timezone.utc)
    fsm.tick(t0)
    broker.set_price(0.55670)
    fsm.tick(t0 + timedelta(seconds=1))
    fsm.tick(t0 + timedelta(seconds=6))  # arma 1x (falha id)
    assert fsm.seguro_armed is True
    n1 = len(broker.limits)
    fsm.tick(t0 + timedelta(seconds=7))
    fsm.tick(t0 + timedelta(seconds=8))
    fsm.tick(t0 + timedelta(seconds=9))
    assert len(broker.limits) == n1  # nao floodou


def test_insurance_rearms_after_fill_if_still_favorable():
    """Apos fill do seguro, se continuar no lucro + sustain → novo pendente."""
    broker = MockBroker(price=0.55660)
    broker.set_payout(0.85)
    fsm = PocketFSM(
        clock=PocketClock(),
        risk=RiskManager(RiskConfig(payout=0.85, buffer=0.30)),
        strategy=Strategy(initial_direction="above"),
        broker=broker,
        config=BotConfig(
            asset="AUDCHF_otc",
            initial_duration_seconds=60,
            auto_switch_asset=False,
            preplace_limit=True,
            preplace_sustain_seconds=5.0,
        ),
    )
    t0 = datetime(2026, 7, 21, 13, 50, 0, tzinfo=timezone.utc)
    fsm.tick(t0)
    broker.set_price(0.55670)
    fsm.tick(t0 + timedelta(seconds=1))
    fsm.tick(t0 + timedelta(seconds=6))  # 1o seguro (below)
    assert len(fsm.cycle.pending_positions) == 1
    first_id = fsm.cycle.pending_positions[0].id
    assert fsm.cycle.pending_positions[0].direction == "below"

    # Fill na barreira; preco permanece abaixo → lucro no below
    broker.set_price(0.55640)
    fsm.tick(t0 + timedelta(seconds=7))
    assert fsm.seguro_armed is False
    assert not fsm.cycle.pending_positions

    # Continua a favor do below: inicia novo sustain (ainda sem pendente)
    fsm.tick(t0 + timedelta(seconds=8))
    assert not fsm.cycle.pending_positions

    # Apos sustain → novo seguro (agora above, para a proxima reversao)
    fsm.tick(t0 + timedelta(seconds=14))
    assert len(fsm.cycle.pending_positions) == 1
    assert fsm.cycle.pending_positions[0].id != first_id
    assert fsm.cycle.pending_positions[0].direction == "above"


def test_fast_cross_limit_fills_at_anchor():
    """Cruzamento sem seguro previo → ajuste limite cola na entry da 1ª."""
    broker = MockBroker(price=0.55660)
    broker.set_payout(0.85)
    fsm = PocketFSM(
        clock=PocketClock(),
        risk=RiskManager(RiskConfig(payout=0.85, buffer=0.30)),
        strategy=Strategy(initial_direction="above", use_limit_for_adjust=True),
        broker=broker,
        config=BotConfig(
            asset="AUDCHF_otc",
            initial_duration_seconds=60,
            auto_switch_asset=False,
            preplace_limit=False,
            adjust_cooldown_seconds=0,
        ),
    )
    t0 = datetime(2026, 7, 21, 13, 50, 0, tzinfo=timezone.utc)
    fsm.tick(t0)
    broker.set_price(0.55640)
    fsm.tick(t0 + timedelta(seconds=3))
    assert not fsm.cycle.pending_positions
    assert fsm.cycle.level == 2
    below = next(p for p in fsm.cycle.open_positions if p.direction == "below")
    assert below.entry_price == 0.55660


def test_limit_fallback_to_market_when_api_fails():
    broker = MockBroker(price=0.55660)
    broker.set_payout(0.85)
    broker.block_pending = True
    broker.limit_falls_to_market = True
    fsm = PocketFSM(
        clock=PocketClock(),
        risk=RiskManager(RiskConfig(payout=0.85, buffer=0.30)),
        strategy=Strategy(initial_direction="above", use_limit_for_adjust=True),
        broker=broker,
        config=BotConfig(
            asset="AUDCHF_otc",
            initial_duration_seconds=60,
            auto_switch_asset=False,
            preplace_limit=False,
            adjust_cooldown_seconds=0,
        ),
    )
    t0 = datetime(2026, 7, 21, 13, 50, 0, tzinfo=timezone.utc)
    fsm.tick(t0)
    broker.set_price(0.55640)
    fsm.tick(t0 + timedelta(seconds=3))
    assert fsm.cycle.level == 2
    below = next(p for p in fsm.cycle.open_positions if p.direction == "below")
    assert below.entry_price == 0.55640
    assert broker.orders[-1]["kind"] == "market"


def test_insurance_arms_immediately_when_sustain_zero():
    broker = MockBroker(price=0.55660)
    broker.set_payout(0.85)
    fsm = PocketFSM(
        clock=PocketClock(),
        risk=RiskManager(RiskConfig(payout=0.85, buffer=0.30)),
        strategy=Strategy(initial_direction="above"),
        broker=broker,
        config=BotConfig(
            asset="AUDCHF_otc",
            initial_duration_seconds=60,
            auto_switch_asset=False,
            preplace_limit=True,
            preplace_sustain_seconds=0.0,
        ),
    )
    t0 = datetime(2026, 7, 21, 13, 50, 0, tzinfo=timezone.utc)
    fsm.tick(t0)
    broker.set_price(0.55670)
    fsm.tick(t0 + timedelta(seconds=1))
    assert len(fsm.cycle.pending_positions) == 1
    assert fsm.cycle.pending_positions[0].direction == "below"
    assert fsm.cycle.pending_positions[0].entry_price == 0.55660


def test_adjust_cooldown_blocks_whipsaw():
    """Inversao em < cooldown nao abre cascata a mercado."""
    broker = MockBroker(price=0.55660)
    broker.set_payout(0.85)
    fsm = PocketFSM(
        clock=PocketClock(),
        risk=RiskManager(RiskConfig(payout=0.85, buffer=0.30)),
        strategy=Strategy(initial_direction="above", use_limit_for_adjust=False),
        broker=broker,
        config=BotConfig(
            asset="AUDCHF_otc",
            initial_duration_seconds=60,
            auto_switch_asset=False,
            preplace_limit=False,
            adjust_cooldown_seconds=8.0,
        ),
    )
    t0 = datetime(2026, 7, 21, 13, 50, 0, tzinfo=timezone.utc)
    fsm.tick(t0)
    broker.set_price(0.55640)
    fsm.tick(t0 + timedelta(seconds=3))  # below
    assert fsm.cycle.level == 2
    broker.set_price(0.55680)
    fsm.tick(t0 + timedelta(seconds=4))  # ainda em cooldown
    assert fsm.cycle.level == 2
    fsm.tick(t0 + timedelta(seconds=12))  # passou cooldown
    assert fsm.cycle.level == 3
    assert broker.orders[-1]["direction"] == "above"


if __name__ == "__main__":
    test_stake_needed_covers_buffer()
    test_delta_stake_respects_min_one_dollar()
    test_delta_stake_when_already_covered()
    test_add_points_five_on_forex_price()
    test_clamp_duration_min_5s()
    test_duration_to_anchor_floor()
    test_merge_keeps_short_trade_times()
    test_first_market_adjust_limit_at_anchor()
    test_adjust_can_force_market()
    test_no_hedge_while_primary_winning_without_opposite()
    test_hedge_when_primary_losing()
    test_lower_payout_increases_adjust_stake()
    test_hedge_scales_with_book_not_mark_zone()
    test_fsm_adapts_payout_on_cross()
    test_fsm_same_expires_at_mid_cycle()
    test_hold_when_remaining_under_5s()
    test_choose_asset_keeps_target()
    test_choose_asset_switches_to_target()
    test_choose_asset_falls_back_to_best()
    test_fsm_does_not_switch_mid_cycle()
    test_fsm_switches_after_settle()
    test_insurance_arms_after_sustain_and_skips_market()
    test_insurance_arms_only_once_per_window()
    test_insurance_rearms_after_fill_if_still_favorable()
    test_fast_cross_limit_fills_at_anchor()
    test_limit_fallback_to_market_when_api_fails()
    test_insurance_arms_immediately_when_sustain_zero()
    test_adjust_cooldown_blocks_whipsaw()
    print("ok")
