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
            self._message = "Stand-by. Clique Iniciar na ferramenta Bot."
            self._snap["message"] = self._message
        return {"ok": True, "detail": "Bot em stand-by."}

    def status(self) -> dict[str, Any]:
        with self._lock:
            out = dict(self._snap)
            out["running"] = self._running
            out["message"] = self._message
            out["last_error"] = self._last_error
            return out

    def pnl_series(self) -> list[dict[str, Any]]:
        with self._lock:
            return list(self._pnl)

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
            initial_dur = int(
                os.environ.get("POCKET_INITIAL_DURATION", str(DEFAULT_DURATION))
            )
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
            asset = fsm.ensure_asset()
            self._message = f"Rodando | {asset}"
            print(
                f"Estrategia ON | asset={asset} | 1a ordem={initial_dur}s\n",
                flush=True,
            )

            last_log = 0.0
            last_state = ""
            while not self._stop.is_set():
                broker.wait_price_update(timeout=poll_timeout)
                if self._stop.is_set():
                    break
                now = datetime.now().astimezone()
                asset = fsm.config.asset
                try:
                    price = broker.get_price(asset)
                except Exception as exc:
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
                self._message = (
                    "Stand-by. Clique Iniciar na ferramenta Bot."
                    if ended == "stand-by"
                    else ended
                )
                self._snap["running"] = False
                self._snap["message"] = self._message
                self._snap["state"] = "IDLE"
                self._thread = None


# Singleton do processo (API web)
runner = BotRunner()
