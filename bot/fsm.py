"""Máquina de estados do bot Pocket (ciclo âncora-T, limite na 1ª + mercado)."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Protocol
from uuid import uuid4

from bot.clock import PocketClock
from bot.position import Cycle, Direction, Position, PositionStatus
from bot.risk import RiskManager
from bot.strategy import Strategy, TradePlan


class State(str, Enum):
    IDLE = "IDLE"
    EVALUATE = "EVALUATE"
    OPEN = "OPEN"
    TRACK = "TRACK"
    HOLD = "HOLD"
    SETTLE = "SETTLE"
    STOP = "STOP"


class Broker(Protocol):
    def get_price(self, asset: str) -> float: ...

    def open_order(
        self,
        asset: str,
        direction: Direction,
        stake: float,
        expiration_seconds: int,
        *,
        opened_at: datetime | None = None,
    ) -> tuple[str, float, datetime]:
        """Retorna (order_id, entry_price, opened_at)."""
        ...

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
        """Arma ordem limite; retorna (order_id, limit_price, opened_at)."""
        ...

    def get_payout(self, asset: str) -> float:
        """Payout vigente em fração (ex. 0.85)."""
        ...


@dataclass
class BotConfig:
    asset: str = "EURUSD_otc"
    initial_duration_seconds: int = 10
    target_payout: float = 0.92
    auto_switch_asset: bool = True
    otc_only: bool = True
    # Seguro: so se a API de pendente responder ticket valido.
    preplace_limit: bool = False
    preplace_sustain_seconds: float = 0.0
    # Anti-whipsaw: nao inverte lado a mercado antes deste intervalo.
    adjust_cooldown_seconds: float = 8.0
    # Gate de lucro: reforca lado favorecido ate book/mark >= buffer.
    profit_guard: bool = True


@dataclass
class PocketFSM:
    clock: PocketClock
    risk: RiskManager
    strategy: Strategy
    broker: Broker
    config: BotConfig = field(default_factory=BotConfig)
    state: State = State.IDLE
    cycle: Cycle = field(default_factory=Cycle)
    pending_plan: TradePlan | None = None
    stop_reason: str | None = None
    last_adjust_at: datetime | None = None
    favorable_since: datetime | None = None
    # Max 1 seguro por janela favoravel (evita flood se a API nao devolver id).
    seguro_armed: bool = False

    def __post_init__(self) -> None:
        self.strategy.risk = self.risk
        self.strategy.clock = self.clock
        self.strategy.initial_duration_seconds = self.config.initial_duration_seconds
        self.strategy.profit_guard = self.config.profit_guard

    def set_initial_duration(self, seconds: int) -> None:
        """Atualiza duração da próxima 1ª ordem (ciclo em andamento mantém o T atual)."""
        self.config.initial_duration_seconds = seconds
        self.strategy.initial_duration_seconds = seconds

    @property
    def chain(self) -> Cycle:
        return self.cycle

    def ensure_asset(self) -> str:
        """Entre ciclos: confirma target payout ou troca de ativo.

        Nunca chama no meio de uma rodada (com posicoes abertas).
        """
        if self.cycle.positions or self.cycle.open_positions:
            return self.config.asset
        if not self.config.auto_switch_asset:
            self._refresh_payout()
            return self.config.asset

        select = getattr(self.broker, "select_asset", None)
        if select is None:
            self._refresh_payout()
            return self.config.asset

        try:
            asset, payout, reason = select(
                self.config.asset,
                target=self.config.target_payout,
                otc_only=self.config.otc_only,
            )
        except Exception as exc:
            print(f"  !! select_asset falhou: {exc}")
            self._refresh_payout()
            return self.config.asset

        prev = self.config.asset
        if asset != prev:
            switch = getattr(self.broker, "switch_asset", None)
            if switch is not None:
                px = switch(asset)
                if px is None:
                    print(f"  !! feed novo ativo {asset} falhou; mantendo {prev}")
                    self._refresh_payout()
                    return self.config.asset
            self.config.asset = asset
            try:
                self.clock.set_allowed_durations(
                    self.broker.load_allowed_durations(asset)  # type: ignore[attr-defined]
                )
            except Exception:
                pass
            print(
                f"  (asset) {prev} -> {asset} | payout={payout * 100:.0f}% "
                f"({reason}, alvo={self.config.target_payout * 100:.0f}%)"
            )
        else:
            print(
                f"  (asset) {asset} payout={payout * 100:.0f}% ({reason}, "
                f"alvo={self.config.target_payout * 100:.0f}%)"
            )

        if payout > 0:
            self.risk.set_payout(payout)
        else:
            self._refresh_payout()
        return self.config.asset

    def tick(self, now: datetime) -> State:
        if self.state == State.STOP:
            return self.state

        if self.risk.hit_daily_stop():
            self._cancel_all_insurance()
            return self._stop("daily_loss_limit")

        if self._settle_due(now):
            self.state = State.SETTLE
            self._cancel_all_insurance()
            if self.cycle.is_cleared() or not self.cycle.open_positions:
                self.cycle = Cycle()
                self.favorable_since = None
                self.seguro_armed = False
                self.last_adjust_at = None
                self.state = State.IDLE
                # Fim da rodada: so agora pode trocar de ativo.
                self.ensure_asset()
                if self.risk.hit_daily_stop():
                    return self._stop("daily_loss_limit")
            return self.state

        if self.state == State.IDLE:
            self.state = State.EVALUATE

        if self.state == State.HOLD:
            self._cancel_all_insurance()
            anchor = self.cycle.anchor_expires_at
            if anchor is not None and now < anchor:
                return self.state
            return self.state

        if self.state in (State.TRACK, State.EVALUATE, State.IDLE):
            if self.state == State.TRACK:
                self.state = State.EVALUATE

        if self.state == State.EVALUATE:
            self._refresh_payout()
            price = self.broker.get_price(self.config.asset)
            anchor = self.cycle.anchor_expires_at

            self._sync_insurance_fills(price)

            if (
                self.cycle.positions
                and anchor is not None
                and not self.clock.can_open_adjustment(now, anchor)
            ):
                if self.cycle.needs_adjustment(price, self.risk.config.buffer):
                    reason = self.clock.hold_reason(now, anchor)
                    fav = self.cycle.favored_direction(price)
                    book = (
                        self.cycle.projected_pnl_if_active(fav)
                        if fav is not None
                        else 0.0
                    )
                    mark = self.cycle.projected_mark_pnl(price)
                    if reason and self.state != State.HOLD:
                        print(
                            f"  !! HOLD (lucro descoberto, sem tempo): {reason} "
                            f"fav={fav} book={book:+.2f} mark={mark:+.2f}"
                        )
                    self._cancel_all_insurance()
                    self.state = State.HOLD
                else:
                    self.state = State.TRACK
                return self.state

            # Seguro cedo + ajuste limite; mercado so se pendente nao cobrir.
            self._manage_insurance(price, now)

            plan = self.strategy.decide(self.cycle, price, now)
            if plan is None:
                if not self.cycle.positions:
                    self.state = State.IDLE
                elif self.cycle.needs_adjustment(
                    price, self.risk.config.buffer
                ) and not self.risk.can_open_level(
                    self.cycle.level, repair=True
                ):
                    fav = self.cycle.favored_direction(price)
                    book = (
                        self.cycle.projected_pnl_if_active(fav)
                        if fav is not None
                        else 0.0
                    )
                    mark = self.cycle.projected_mark_pnl(price)
                    if self.state != State.HOLD:
                        print(
                            "  !! HOLD (lucro descoberto; max_levels+"
                            f"repair esgotados) fav={fav} "
                            f"book={book:+.2f} mark={mark:+.2f} "
                            f"levels={self.cycle.level}"
                        )
                    self._cancel_all_insurance()
                    self.state = State.HOLD
                else:
                    self.state = State.TRACK
            else:
                if plan.reason != "base_open":
                    if self._blocked_by_adjust_cooldown(plan.direction, now):
                        self.state = State.TRACK
                        return self.state
                    from bot.insurance import pending_covers_market_plan

                    if pending_covers_market_plan(self.cycle, plan.direction, plan.stake):
                        print(
                            f"  (seguro) pendente cobre {plan.direction} "
                            f"stake>={plan.stake:.2f}; skip mercado"
                        )
                        self.state = State.TRACK
                        return self.state
                    # Mercado vai entrar: cancela seguro e recalcula stake limpa
                    if self.cycle.pending_positions:
                        self._cancel_all_insurance()
                        plan = self.strategy.decide(self.cycle, price, now)
                        if plan is None or plan.reason == "base_open":
                            self.state = State.TRACK
                            return self.state
                        if self._blocked_by_adjust_cooldown(plan.direction, now):
                            self.state = State.TRACK
                            return self.state
                    print(
                        f"  !! CRUZAMENTO price={price} -> {plan.direction} "
                        f"({plan.reason}) @{now.strftime('%H:%M:%S.%f')[:-3]}"
                    )
                self.pending_plan = plan
                self.state = State.OPEN

        if self.state == State.OPEN:
            self._execute_open(now)
            self.state = State.TRACK

        return self.state

    def _blocked_by_adjust_cooldown(self, direction: Direction, now: datetime) -> bool:
        """True se ainda nao pode inverter o lado (ruido em torno da entry)."""
        cd = self.config.adjust_cooldown_seconds
        if cd <= 0 or self.last_adjust_at is None:
            return False
        elapsed = (now - self.last_adjust_at).total_seconds()
        if elapsed >= cd:
            return False
        opens = self.cycle.open_positions
        if not opens:
            return False
        last_dir = opens[-1].direction
        if last_dir == direction:
            return False  # reforco no mesmo lado ok
        return True

    def _manage_insurance(self, price: float, now: datetime) -> None:
        from bot.insurance import build_insurance_plan, is_favorable_for_insurance

        if not self.config.preplace_limit:
            return
        if getattr(self.broker, "pending_api_ok", True) is False:
            return
        if not self.cycle.open_positions:
            self.favorable_since = None
            self.seguro_armed = False
            return

        # Ja existe pendente local → no maximo 1; nao recria.
        pending = list(self.cycle.pending_positions)
        if len(pending) > 1:
            for extra in pending[1:]:
                self._cancel_insurance(extra)
            pending = list(self.cycle.pending_positions)
        if pending:
            self.seguro_armed = True
            return

        favorable = is_favorable_for_insurance(
            self.cycle, price, self.risk.config.buffer
        )
        if not favorable:
            self.favorable_since = None
            if self.seguro_armed:
                self._cancel_all_insurance()
            self.seguro_armed = False
            return

        if self.seguro_armed:
            # Ja tentou armar nesta janela (ok ou falha de parse) — nao floodar.
            # Apos PREENCHIDO a trava cai e o sustain reinicia.
            return

        if self.favorable_since is None:
            self.favorable_since = now
            # sustain=0 → arma no mesmo tick (fecha gap Acima/Abaixo cedo).
            if self.config.preplace_sustain_seconds > 0:
                return

        sustained = (now - self.favorable_since).total_seconds()
        if sustained < self.config.preplace_sustain_seconds:
            return

        plan = build_insurance_plan(
            self.cycle, self.risk, self.clock, price, now
        )
        if plan is None:
            return

        self._arm_insurance(plan, now)
        # Trava mesmo se a API nao devolver id (pedido pode ter sido criado).
        self.seguro_armed = True

    def _arm_insurance(self, plan, now: datetime) -> None:
        place = getattr(self.broker, "place_pending", None)
        if place is None:
            print("  !! seguro: broker sem place_pending; skip")
            return
        if self.cycle.pending_positions:
            print("  !! seguro: ja existe 1 pendente; skip")
            return
        try:
            order_id = place(
                self.config.asset,
                plan.direction,
                plan.stake,
                plan.limit_price,
                plan.duration_seconds,
                opened_at=now,
            )
        except Exception as exc:
            print(f"  !! seguro falhou ao armar: {exc}")
            return
        if not order_id:
            print(
                "  !! seguro: sem id na resposta; nao reenvia "
                "(evita multiplos pedidos). Mercado segue normal."
            )
            return

        expires = self.cycle.anchor_expires_at or self.clock.expires_at(
            now, plan.duration_seconds
        )
        pos = Position(
            id=str(order_id),
            direction=plan.direction,
            stake=plan.stake,
            entry_price=plan.limit_price,
            opened_at=now,
            expires_at=expires,
            payout=self.risk.config.payout,
            status=PositionStatus.PENDING,
        )
        self.cycle.positions.append(pos)
        print(f"  (seguro) ARMADO {plan.reason} id={order_id}")

    def _sync_insurance_fills(self, price: float) -> None:
        if not self.cycle.pending_positions:
            return
        check = getattr(self.broker, "pending_still_open", None)
        entry = self.cycle.primary_entry()
        for pos in list(self.cycle.pending_positions):
            still = True
            if check is not None:
                try:
                    still = bool(check(pos.id))
                except Exception:
                    still = True

            crossed = False
            if entry is not None:
                crossed = (
                    (pos.direction == "below" and price <= entry)
                    or (pos.direction == "above" and price >= entry)
                )

            if not still:
                self._mark_seguro_filled(pos, via="api")
                continue

            if crossed:
                # Toque na barreira: assume fill do pendente (seguro).
                fill = getattr(self.broker, "fill_pending", None)
                if fill is not None:
                    try:
                        fill(pos.id)
                    except Exception:
                        pass
                else:
                    self._cancel_insurance_remote(pos.id)
                self._mark_seguro_filled(pos, via="barreira")

    def _mark_seguro_filled(self, pos: Position, *, via: str) -> None:
        """Pendente virou OPEN: libera trava para novo seguro apos novo sustain."""
        pos.status = PositionStatus.OPEN
        self.seguro_armed = False
        self.favorable_since = None  # exige novo tempo a favor antes do proximo
        print(
            f"  (seguro) PREENCHIDO ({via}) id={pos.id} {pos.direction} "
            f"stake={pos.stake} @{pos.entry_price} | pronto p/ novo apos sustain"
        )

    def _cancel_insurance_remote(self, order_id: str) -> None:
        cancel = getattr(self.broker, "cancel_pending", None)
        if cancel is not None:
            try:
                cancel(order_id)
            except Exception as exc:
                print(f"  !! cancel pendente {order_id}: {exc}")

    def _cancel_insurance(self, pos: Position) -> None:
        self._cancel_insurance_remote(pos.id)
        self.cycle.positions = [p for p in self.cycle.positions if p.id != pos.id]
        print(f"  (seguro) cancelado id={pos.id}")

    def _cancel_all_insurance(self) -> None:
        for pos in list(self.cycle.pending_positions):
            self._cancel_insurance(pos)
        self.favorable_since = None
        self.seguro_armed = False

    def _refresh_payout(self) -> None:
        """Payout ao vivo para o próximo ajuste (ordens já abertas mantêm o delas)."""
        getter = getattr(self.broker, "get_payout", None)
        if getter is None:
            return
        try:
            payout = float(getter(self.config.asset))
        except Exception as exc:
            print(f"  !! payout indisponivel: {exc}")
            return
        if payout <= 0:
            return
        prev = self.risk.config.payout
        self.risk.set_payout(payout)
        if abs(prev - payout) >= 0.005:
            print(f"  (payout) {prev * 100:.0f}% -> {payout * 100:.0f}%")

    @staticmethod
    def _barrier_crossed(direction: Direction, limit_price: float, price: float) -> bool:
        if direction == "below":
            return price <= limit_price
        return price >= limit_price

    def _try_place_adjust_limit(self, plan: TradePlan, now: datetime) -> bool:
        """Tenta ajuste na entry da 1ª (pendente). True se armou/preencheu.

        Se o preço já cruzou a barreira, marca OPEN na entry (sem zona morta).
        Se o hedge já é necessário e o pendente não preenche na hora, cancela
        e devolve False → caller abre a mercado (aceita gap).
        """
        if plan.limit_price is None:
            return False
        if getattr(self.broker, "pending_api_ok", True) is False:
            return False
        limit_price = float(plan.limit_price)
        price = self.broker.get_price(self.config.asset)

        order_id: str | None = None
        place = getattr(self.broker, "place_pending", None)
        if place is not None:
            try:
                order_id = place(
                    self.config.asset,
                    plan.direction,
                    plan.stake,
                    limit_price,
                    plan.duration_seconds,
                    opened_at=now,
                )
            except Exception as exc:
                print(f"  !! ajuste limite (pending) falhou: {exc}")
                order_id = None

        if not order_id:
            # Fallback: place_limit do broker (pode ela mesma cair p/ mercado).
            try:
                oid, entry, opened_at = self.broker.place_limit(
                    self.config.asset,
                    plan.direction,
                    plan.stake,
                    limit_price,
                    plan.duration_seconds,
                    opened_at=now,
                )
            except Exception as exc:
                print(f"  !! ajuste limite falhou: {exc}")
                return False
            # Se a entry veio diferente do limite, foi fallback mercado no broker.
            if abs(float(entry) - limit_price) > 1e-12:
                expires = self.cycle.anchor_expires_at or self.clock.expires_at(
                    opened_at, plan.duration_seconds
                )
                self.cycle.positions.append(
                    Position(
                        id=oid or str(uuid4()),
                        direction=plan.direction,
                        stake=plan.stake,
                        entry_price=float(entry),
                        opened_at=opened_at,
                        expires_at=expires,
                        payout=self.risk.config.payout,
                    )
                )
                print(
                    f"  (limite) fallback mercado {plan.direction} "
                    f"@{entry} stake={plan.stake:.2f}"
                )
                return True
            order_id = oid

        expires = self.cycle.anchor_expires_at or self.clock.expires_at(
            now, plan.duration_seconds
        )
        pos = Position(
            id=str(order_id),
            direction=plan.direction,
            stake=plan.stake,
            entry_price=limit_price,
            opened_at=now,
            expires_at=expires,
            payout=self.risk.config.payout,
            status=PositionStatus.PENDING,
        )
        self.cycle.positions.append(pos)
        print(
            f"  (limite) ARMADO {plan.direction} @{limit_price} "
            f"stake={plan.stake:.2f} id={order_id}"
        )

        if self._barrier_crossed(plan.direction, limit_price, price):
            fill = getattr(self.broker, "fill_pending", None)
            if fill is not None:
                try:
                    fill(pos.id)
                except Exception:
                    pass
            self._mark_seguro_filled(pos, via="barreira-ajuste")
            return True

        # Hedge ja necessario e pendente so preenche no pullback → mercado.
        hedge = self.cycle.hedge_direction()
        if (
            hedge is not None
            and plan.direction == hedge
            and self.cycle.primary_is_losing(price)
        ):
            print(
                "  (limite) preco ja contra; pendente nao protege agora → mercado"
            )
            self._cancel_insurance(pos)
            return False

        return True

    def _execute_open(self, now: datetime) -> None:
        plan = self.pending_plan
        if plan is None:
            return

        if (
            plan.order_kind == "limit"
            and plan.limit_price is not None
            and plan.reason != "base_open"
        ):
            if self._try_place_adjust_limit(plan, now):
                if plan.reason != "base_open":
                    self.last_adjust_at = now
                self.pending_plan = None
                return
            print("  (limite) sem fill util; abrindo a mercado")

        order_id, entry, opened_at = self.broker.open_order(
            self.config.asset,
            plan.direction,
            plan.stake,
            plan.duration_seconds,
            opened_at=now,
        )

        expires = self.clock.expires_at(opened_at, plan.duration_seconds)
        if self.cycle.anchor_expires_at is None:
            self.cycle.anchor_expires_at = expires
        else:
            expires = self.cycle.anchor_expires_at

        pos = Position(
            id=order_id or str(uuid4()),
            direction=plan.direction,
            stake=plan.stake,
            entry_price=entry,
            opened_at=opened_at,
            expires_at=expires,
            payout=self.risk.config.payout,
        )
        self.cycle.positions.append(pos)
        if plan.reason != "base_open":
            self.last_adjust_at = opened_at
        self.pending_plan = None

    def _settle_due(self, now: datetime) -> bool:
        anchor = self.cycle.anchor_expires_at
        if anchor is None:
            return False
        if now < anchor:
            settled_any = False
            price = self.broker.get_price(self.config.asset)
            for pos in list(self.cycle.open_positions):
                if now >= pos.expires_at:
                    pnl = self.cycle.apply_settlement(pos, price)
                    self.risk.register_pnl(pnl)
                    settled_any = True
            return settled_any and not self.cycle.open_positions

        price = self.broker.get_price(self.config.asset)
        for pos in list(self.cycle.open_positions):
            pnl = self.cycle.apply_settlement(pos, price)
            self.risk.register_pnl(pnl)
        return True

    def _stop(self, reason: str) -> State:
        self.stop_reason = reason
        self.state = State.STOP
        return self.state


BlitzFSM = PocketFSM
