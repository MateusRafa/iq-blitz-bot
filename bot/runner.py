"""Runner do bot Pocket DEMO (thread): start/stop/status + historico de PnL."""

from __future__ import annotations

import os
import threading
import time
from collections import deque
from datetime import datetime, timezone
from typing import Any

from bot.broker_pocket import PocketBroker
from bot.clock import PocketClock
from bot.fsm import BotConfig, PocketFSM
from bot.risk import RiskConfig, RiskManager
from bot.strategy import Strategy

MIN_DURATION = 5
DEFAULT_DURATION = 10
PNL_HISTORY_MAX = 3600  # ~1h se 1 ponto/s


def env_flag(name: str, default: str = "0") -> bool:
    return os.environ.get(name, default).strip().lower() in ("1", "true", "yes")


def env_flag_default_on(name: str, default: str = "1") -> bool:
    return os.environ.get(name, default).strip().lower() not in ("0", "false", "no")


def normalize_asset(asset: str) -> str:
    a = asset.strip()
    if a.upper().endswith("_OTC"):
        return a[:-4] + "_otc"
    return a


def is_connection_error(exc: BaseException) -> bool:
    """True se a falha indica WS/sessao Pocket caída (vale reconectar)."""
    name = type(exc).__name__.lower()
    msg = str(exc).lower()
    blob = f"{name} {msg}"
    needles = (
        "half closed",
        "channel sender",
        "pocketoptionerror",
        "core error",
        "websocket",
        "connection reset",
        "connection aborted",
        "broken pipe",
        "timed out",
        "timeout",
        "not connected",
        "disconnected",
        "send to a half",
        "os error 10054",
        "os error 10053",
        "feed parado",
    )
    return any(n in blob for n in needles)


def load_ssid() -> str:
    ssid = os.environ.get("POCKET_OPTION_SSID", "").strip()
    if not ssid:
        raise ValueError(
            "Defina POCKET_OPTION_SSID com o auth DEMO completo "
            '(isDemo:1).'
        )
    if "isDemo" in ssid and '"isDemo":0' in ssid.replace(" ", ""):
        raise ValueError(
            "SSID indica conta REAL (isDemo:0). Use sessao DEMO."
        )
    return ssid


def build_risk() -> RiskManager:
    return RiskManager(
        RiskConfig(
            base_stake=float(os.environ.get("POCKET_BASE_STAKE", "1.0")),
            min_stake=float(os.environ.get("POCKET_MIN_STAKE", "1.0")),
            buffer=float(os.environ.get("POCKET_BUFFER", "0.30")),
            payout=0.85,
            max_stake=float(os.environ.get("POCKET_MAX_STAKE", "50.0")),
            max_levels=int(os.environ.get("POCKET_MAX_LEVELS", "8")),
            daily_loss_limit=float(os.environ.get("POCKET_DAILY_LOSS", "20.0")),
        )
    )


def build_bot_config(*, asset: str, initial_duration: int) -> BotConfig:
    return BotConfig(
        asset=asset,
        initial_duration_seconds=initial_duration,
        target_payout=float(os.environ.get("POCKET_TARGET_PAYOUT", "0.92")),
        auto_switch_asset=env_flag_default_on("POCKET_AUTO_ASSET", "1"),
        otc_only=env_flag_default_on("POCKET_OTC_ONLY", "1"),
        preplace_limit=env_flag("POCKET_PREPLACE_LIMIT", "0"),
        preplace_sustain_seconds=float(
            os.environ.get("POCKET_PREPLACE_SUSTAIN", "0")
        ),
        adjust_cooldown_seconds=float(
            os.environ.get("POCKET_ADJUST_COOLDOWN", "8")
        ),
    )


class BotRunner:
    """Um bot por processo; controlado pela API web."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._running = False
        self._message = "Stand-by. Clique Iniciar na ferramenta Bot."
        self._last_error: str | None = None
        self._pnl: deque[dict[str, Any]] = deque(maxlen=PNL_HISTORY_MAX)
        self._snap: dict[str, Any] = self._empty_snap()
        self._duration_lock = threading.Lock()
        env_dur = int(
            os.environ.get("POCKET_INITIAL_DURATION", str(DEFAULT_DURATION))
        )
        self._duration_seconds = max(MIN_DURATION, env_dur)
        self._fsm: PocketFSM | None = None
        self._risk: RiskManager | None = None

    def get_duration_seconds(self) -> int:
        with self._duration_lock:
            return self._duration_seconds

    def set_duration_seconds(self, seconds: int) -> dict[str, Any]:
        """Define duracao da proxima 1a ordem (ciclo atual mantem o T)."""
        try:
            sec = int(seconds)
        except (TypeError, ValueError):
            return {"ok": False, "detail": "Duracao invalida."}
        if sec < MIN_DURATION:
            return {
                "ok": False,
                "detail": f"Minimo {MIN_DURATION}s para a 1a ordem.",
            }
        with self._duration_lock:
            self._duration_seconds = sec
        with self._lock:
            fsm = self._fsm
            if fsm is not None:
                try:
                    fsm.set_initial_duration(sec)
                except Exception:
                    pass
            self._snap["duration_seconds"] = sec
        print(f"[web] 1a ordem = {sec}s", flush=True)
        return {
            "ok": True,
            "detail": f"1a ordem = {sec}s (vale na proxima abertura de ciclo).",
            "duration_seconds": sec,
        }

    @staticmethod
    def _empty_snap() -> dict[str, Any]:
        return {
            "running": False,
            "asset": None,
            "price": None,
            "state": "IDLE",
            "sa": 0.0,
            "sb": 0.0,
            "level": 0,
            "resto_s": None,
            "daily_pnl": 0.0,
            "mark_pnl": 0.0,
            "total_pnl": 0.0,
            "duration_seconds": DEFAULT_DURATION,
            "wins": 0,
            "losses": 0,
            "settled": 0,
            "win_rate_pct": None,
            "message": "Stand-by",
            "updated_at": None,
        }

    def is_running(self) -> bool:
        with self._lock:
            return self._running

    def start(self) -> dict[str, Any]:
        with self._lock:
            if self._running:
                return {"ok": False, "detail": "Bot ja esta em execucao."}
            try:
                load_ssid()
            except ValueError as exc:
                return {"ok": False, "detail": str(exc)}

            self._stop.clear()
            self._last_error = None
            self._message = "Iniciando..."
            self._running = True
            self._thread = threading.Thread(
                target=self._loop, name="pocket-bot", daemon=True
            )
            self._thread.start()
            return {"ok": True, "detail": "Bot iniciado."}

    def stop(self) -> dict[str, Any]:
        with self._lock:
            if not self._running and (
                self._thread is None or not self._thread.is_alive()
            ):
                self._message = "Stand-by. Clique Iniciar na ferramenta Bot."
                return {"ok": True, "detail": "Ja estava em stand-by."}
            self._stop.set()
            self._message = "Parando..."
        t = self._thread
        if t is not None and t.is_alive():
            t.join(timeout=8.0)
        with self._lock:
            self._running = False
            self._snap["running"] = False
            self._fsm = None
            self._message = "Stand-by. Clique Iniciar na ferramenta Bot."
            self._snap["message"] = self._message
        return {"ok": True, "detail": "Bot em stand-by."}

    def status(self) -> dict[str, Any]:
        with self._lock:
            out = dict(self._snap)
            out["running"] = self._running
            out["message"] = self._message
            out["last_error"] = self._last_error
            out["duration_seconds"] = self.get_duration_seconds()
            risk = self._risk
            wins = int(risk.wins) if risk is not None else int(out.get("wins") or 0)
            losses = (
                int(risk.losses) if risk is not None else int(out.get("losses") or 0)
            )
            settled = wins + losses
            out["wins"] = wins
            out["losses"] = losses
            out["settled"] = settled
            out["win_rate_pct"] = (
                round(100.0 * wins / settled, 1) if settled > 0 else None
            )
            return out

    def pnl_series(self) -> list[dict[str, Any]]:
        with self._lock:
            return list(self._pnl)

    def _reconnect_broker(
        self,
        broker: PocketBroker,
        *,
        asset: str,
        max_attempts: int,
    ) -> bool:
        """Tenta reabrir WS com backoff. True se feed voltou."""
        self._message = "Reconectando Pocket..."
        self._update_snap(message=self._message)
        for attempt in range(1, max(1, max_attempts) + 1):
            if self._stop.is_set():
                return False
            wait = min(60.0, float(2 ** min(attempt, 5)))
            print(
                f"  !! reconnect tentativa {attempt}/{max_attempts} "
                f"(espera {wait:.0f}s)...",
                flush=True,
            )
            time.sleep(wait)
            if self._stop.is_set():
                return False
            try:
                px = broker.reconnect(asset=asset, feed_wait_seconds=15.0)
            except Exception as exc:
                print(f"  !! reconnect falhou: {exc}", flush=True)
                continue
            if px is not None:
                with self._lock:
                    self._last_error = None
                return True
        return False

    def _record_pnl(
        self,
        *,
        daily: float,
        mark: float,
        total: float,
    ) -> None:
        now = datetime.now(timezone.utc).isoformat()
        point = {
            "t": now,
            "realized": round(daily, 4),
            "mark": round(mark, 4),
            "total": round(total, 4),
        }
        with self._lock:
            # Evita flood: no maximo ~1 ponto/s
            if self._pnl:
                last = self._pnl[-1]["t"]
                try:
                    prev = datetime.fromisoformat(last)
                    if (datetime.now(timezone.utc) - prev).total_seconds() < 0.9:
                        self._pnl[-1] = point
                        return
                except ValueError:
                    pass
            self._pnl.append(point)

    def _update_snap(self, **kwargs: Any) -> None:
        with self._lock:
            self._snap.update(kwargs)
            self._snap["updated_at"] = datetime.now(timezone.utc).isoformat()
            self._snap["message"] = self._message

    def _loop(self) -> None:
        broker: PocketBroker | None = None
        ended = "stand-by"
        try:
            ssid = load_ssid()
            asset = normalize_asset(
                os.environ.get("POCKET_ASSET", "AUDCHF_otc")
            )
            poll_timeout = float(os.environ.get("POCKET_TICK_SECONDS", "0.05"))
            min_payout = int(os.environ.get("POCKET_MIN_PAYOUT", "50"))
            use_limit = env_flag("POCKET_USE_LIMIT", "0")
            log_every = float(os.environ.get("POCKET_LOG_EVERY", "5.0"))
            initial_dur = self.get_duration_seconds()
            if initial_dur < MIN_DURATION:
                raise ValueError(f"duration invalido: minimo {MIN_DURATION}s")

            print("Conectando Pocket DEMO...", flush=True)
            self._message = "Conectando Pocket DEMO..."
            broker = PocketBroker(ssid, require_demo=True, min_payout=min_payout)
            print(
                f"OK demo={broker.is_demo()} balance={broker.balance():.2f} "
                f"asset={asset}",
                flush=True,
            )
            px0 = broker.start_price_feed(asset, wait_seconds=15.0)
            if px0 is None:
                raise RuntimeError("Feed nao trouxe preco a tempo.")

            clock = PocketClock()
            clock.set_allowed_durations(broker.load_allowed_durations(asset))
            risk = build_risk()
            with self._lock:
                self._risk = risk
            fsm = PocketFSM(
                clock=clock,
                risk=risk,
                strategy=Strategy(
                    initial_direction="above", use_limit_for_adjust=use_limit
                ),
                broker=broker,
                config=build_bot_config(
                    asset=asset, initial_duration=initial_dur
                ),
            )
            with self._lock:
                self._fsm = fsm
            asset = fsm.ensure_asset()
            self._message = f"Rodando | {asset} | 1a={initial_dur}s"
            self._update_snap(duration_seconds=initial_dur)
            print(
                f"Estrategia ON | asset={asset} | 1a ordem={initial_dur}s\n",
                flush=True,
            )

            last_log = 0.0
            last_state = ""
            last_dur = initial_dur
            feed_stale_s = float(os.environ.get("POCKET_FEED_STALE_SECONDS", "45"))
            reconnect_max = int(os.environ.get("POCKET_RECONNECT_MAX", "30"))
            while not self._stop.is_set():
                dur_now = self.get_duration_seconds()
                if dur_now != last_dur:
                    fsm.set_initial_duration(dur_now)
                    last_dur = dur_now
                    print(f"[web] duracao 1a ordem atualizada = {dur_now}s", flush=True)

                try:
                    if broker.feed_age_seconds() > feed_stale_s:
                        raise RuntimeError(
                            f"Feed parado ha {broker.feed_age_seconds():.0f}s "
                            f"(limite {feed_stale_s:.0f}s)"
                        )

                    broker.wait_price_update(timeout=poll_timeout)
                    if self._stop.is_set():
                        break
                    now = datetime.now().astimezone()
                    asset = fsm.config.asset
                    try:
                        price = broker.get_price(asset)
                    except Exception as exc:
                        if is_connection_error(exc):
                            raise
                        print(f"preco indisponivel: {exc}", flush=True)
                        time.sleep(0.2)
                        continue

                    state = fsm.tick(now)
                    asset = fsm.config.asset
                    daily = float(risk.daily_pnl)
                    mark = float(fsm.cycle.projected_mark_pnl(price))
                    total = daily + mark
                    resto = None
                    anchor = fsm.cycle.anchor_expires_at
                    if anchor is not None:
                        resto = max(0.0, (anchor - now).total_seconds())

                    wins = int(risk.wins)
                    losses = int(risk.losses)
                    settled = wins + losses
                    win_rate = (
                        round(100.0 * wins / settled, 1) if settled > 0 else None
                    )

                    self._record_pnl(daily=daily, mark=mark, total=total)
                    self._update_snap(
                        running=True,
                        asset=asset,
                        price=price,
                        state=state.value,
                        sa=round(fsm.cycle.stake_above(), 2),
                        sb=round(fsm.cycle.stake_below(), 2),
                        level=fsm.cycle.level,
                        resto_s=None if resto is None else round(resto, 1),
                        daily_pnl=round(daily, 4),
                        mark_pnl=round(mark, 4),
                        total_pnl=round(total, 4),
                        duration_seconds=dur_now,
                        wins=wins,
                        losses=losses,
                        settled=settled,
                        win_rate_pct=win_rate,
                    )

                    now_mono = time.monotonic()
                    if state.value != last_state or (now_mono - last_log) >= log_every:
                        open_pos = [
                            f"{p.direction}:{p.stake}@{p.entry_price}"
                            for p in fsm.cycle.open_positions
                        ]
                        rem = "" if resto is None else f" resto={resto:.0f}s"
                        print(
                            f"{now.strftime('%H:%M:%S.%f')[:-3]} [{asset}] "
                            f"price={price} state={state.value}{rem} "
                            f"Sa={fsm.cycle.stake_above():.2f} "
                            f"Sb={fsm.cycle.stake_below():.2f} "
                            f"levels={fsm.cycle.level} open={open_pos} "
                            f"pnl={total:+.2f}",
                            flush=True,
                        )
                        last_log = now_mono
                        last_state = state.value

                    if state.value == "STOP":
                        ended = f"STOP: {fsm.stop_reason}"
                        print(ended, flush=True)
                        break
                except Exception as exc:
                    if self._stop.is_set():
                        break
                    if not is_connection_error(exc):
                        raise
                    print(f"  !! conexao Pocket: {exc}", flush=True)
                    ok = self._reconnect_broker(
                        broker,
                        asset=fsm.config.asset,
                        max_attempts=reconnect_max,
                    )
                    if not ok:
                        ended = (
                            f"Erro: falha ao reconectar apos queda WS ({exc}). "
                            f"SSID pode ter expirado — atualize POCKET_OPTION_SSID "
                            f"e clique Iniciar."
                        )
                        with self._lock:
                            self._last_error = ended
                        print(ended, flush=True)
                        break
                    self._message = (
                        f"Rodando | {fsm.config.asset} | 1a={dur_now}s"
                    )
                    continue
            else:
                ended = "stand-by"
                print("[web] estrategia em stand-by", flush=True)
        except Exception as exc:
            ended = f"Erro: {exc}"
            print(f"Erro no bot: {exc}", flush=True)
            with self._lock:
                self._last_error = str(exc)
        finally:
            if broker is not None:
                try:
                    broker.close()
                except Exception:
                    pass
            with self._lock:
                self._running = False
                self._fsm = None
                # Mantem _risk para status mostrar W/L apos Parar ate o proximo Iniciar
                self._message = (
                    "Stand-by. Clique Iniciar na ferramenta Bot."
                    if ended == "stand-by"
                    else ended
                )
                self._snap["running"] = False
                self._snap["message"] = self._message
                self._snap["state"] = "IDLE"
                if self._risk is not None:
                    w, l = self._risk.wins, self._risk.losses
                    st = w + l
                    self._snap["wins"] = w
                    self._snap["losses"] = l
                    self._snap["settled"] = st
                    self._snap["win_rate_pct"] = (
                        round(100.0 * w / st, 1) if st > 0 else None
                    )
                self._thread = None


# Singleton do processo (API web)
runner = BotRunner()
