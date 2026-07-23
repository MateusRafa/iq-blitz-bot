"""Roda a FSM na conta DEMO da Pocket Option.

Painel (local):
- Parar  -> stand-by (pode Iniciar de novo)
- Fechar (X) -> encerra o bot e o processo

Headless / Railway:
  python run_pocket_demo.py --no-gui

Uso (PowerShell):
  $env:POCKET_OPTION_SSID = '42["auth",{...}]'
  $env:POCKET_ASSET = 'AUDCHF_otc'
  python run_pocket_demo.py
"""

from __future__ import annotations

import argparse
import os
import sys
import threading
import time
from datetime import datetime

from bot.broker_pocket import PocketBroker
from bot.clock import PocketClock
from bot.fsm import BotConfig, PocketFSM
from bot.risk import RiskConfig, RiskManager
from bot.strategy import Strategy

try:
    import tkinter as tk
    from tkinter import messagebox, ttk

    _HAS_TK = True
except ImportError:  # Railway / imagem sem Tk
    tk = None  # type: ignore[assignment]
    messagebox = None  # type: ignore[assignment]
    ttk = None  # type: ignore[assignment]
    _HAS_TK = False

MIN_DURATION = 5
DEFAULT_DURATION = 10


def _env_flag(name: str, default: str = "0") -> bool:
    return os.environ.get(name, default).strip().lower() in ("1", "true", "yes")


def _env_flag_default_on(name: str, default: str = "1") -> bool:
    return os.environ.get(name, default).strip().lower() not in ("0", "false", "no")


def _normalize_asset(asset: str) -> str:
    """Pocket usa sufixo _otc em minusculas (AUDNZD_OTC → AUDNZD_otc)."""
    a = asset.strip()
    if a.upper().endswith("_OTC"):
        return a[:-4] + "_otc"
    return a


def _load_ssid() -> str:
    ssid = os.environ.get("POCKET_OPTION_SSID", "").strip()
    if not ssid:
        raise SystemExit(
            "Defina a variavel de ambiente POCKET_OPTION_SSID com o auth DEMO completo.\n"
            'Exemplo: 42["auth",{"session":"...","isDemo":1,"uid":123,"platform":1}]'
        )
    if "isDemo" in ssid and '"isDemo":0' in ssid.replace(" ", ""):
        raise SystemExit(
            "SSID indica conta REAL (isDemo:0). Use a sessao da conta DEMO."
        )
    return ssid


def _build_risk() -> RiskManager:
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


def _build_bot_config(*, asset: str, initial_duration: int) -> BotConfig:
    return BotConfig(
        asset=asset,
        initial_duration_seconds=initial_duration,
        target_payout=float(os.environ.get("POCKET_TARGET_PAYOUT", "0.92")),
        auto_switch_asset=_env_flag_default_on("POCKET_AUTO_ASSET", "1"),
        otc_only=_env_flag_default_on("POCKET_OTC_ONLY", "1"),
        preplace_limit=_env_flag("POCKET_PREPLACE_LIMIT", "0"),
        preplace_sustain_seconds=float(
            os.environ.get("POCKET_PREPLACE_SUSTAIN", "0")
        ),
        adjust_cooldown_seconds=float(
            os.environ.get("POCKET_ADJUST_COOLDOWN", "8")
        ),
    )


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Pocket DEMO — ciclo ancora-T")
    p.add_argument("--duration", "-d", type=int, default=None)
    p.add_argument("--asset", "-a", type=str, default=None)
    p.add_argument("--no-gui", action="store_true")
    return p.parse_args()


class ControlPanel:
    """Painel persistente e responsivo (Parar = stand-by, Fechar = sair)."""

    def __init__(
        self,
        *,
        ssid: str,
        asset: str,
        initial_duration: int,
        use_limit: bool,
        min_payout: int,
        poll_timeout: float,
        log_every: float,
    ) -> None:
        self.ssid = ssid
        self.asset = asset
        self.use_limit = use_limit
        self.min_payout = min_payout
        self.poll_timeout = poll_timeout
        self.log_every = log_every
        # Piso do loop: cede GIL ao Tk para cliques Parar/Fechar responderem na hora.
        self._loop_floor = max(0.15, float(os.environ.get("POCKET_UI_LOOP_FLOOR", "0.20")))

        self._duration_lock = threading.Lock()
        self._duration = max(initial_duration, MIN_DURATION)
        self._stop = threading.Event()
        self._closing = False
        self._bot_thread: threading.Thread | None = None
        self._fsm: PocketFSM | None = None
        self._broker: PocketBroker | None = None
        self._broker_lock = threading.Lock()

        # Status: bot so escreve string; Tk checa leve e pinta com delay.
        self._status_lock = threading.Lock()
        self._pending_status = "Stand-by. Clique Iniciar."
        self._shown_status = self._pending_status
        self._status_urgent = False
        # Checagem leve (ms). Pintura cara so a cada _min_paint_gap_s (exceto urgent).
        self._status_poll_ms = int(os.environ.get("POCKET_UI_POLL_MS", "200"))
        self._min_paint_gap_s = float(os.environ.get("POCKET_UI_STATUS_MS", "1000")) / 1000.0
        self._last_paint_mono = 0.0
        self._ended_reason: str | None = None
        # Bot so empurra texto de status a cada N segundos (exceto mudanca de estado).
        self._status_every_s = float(os.environ.get("POCKET_UI_STATUS_EVERY", "2.5"))

        self.root = tk.Tk()
        self.root.title("Pocket Bot")
        self.root.resizable(False, False)
        # topmost atrapalha clique em outros apps; deixa so no inicio
        self.root.attributes("-topmost", True)
        self.root.after(800, lambda: self.root.attributes("-topmost", False))

        frame = ttk.Frame(self.root, padding=16)
        frame.grid(row=0, column=0, sticky="nsew")

        ttk.Label(
            frame,
            text="Expiracao da 1a ordem (segundos)",
            font=("Segoe UI", 11, "bold"),
        ).grid(row=0, column=0, columnspan=3, sticky="w")

        ttk.Label(
            frame,
            text=(
                "Ajustes usam o mesmo T do ciclo atual.\n"
                f"Mudar o tempo vale na proxima 1a ordem. Min {MIN_DURATION}s.\n"
                "Parar = stand-by | Fechar (X) = encerra o programa."
            ),
            justify="left",
        ).grid(row=1, column=0, columnspan=3, sticky="w", pady=(8, 12))

        ttk.Label(frame, text="Tempo (s):").grid(row=2, column=0, sticky="w")
        self.duration_var = tk.StringVar(value=str(self._duration))
        self.entry = ttk.Entry(frame, textvariable=self.duration_var, width=10)
        self.entry.grid(row=2, column=1, sticky="w", padx=(8, 8))
        self.apply_btn = ttk.Button(frame, text="Aplicar", command=self._apply_duration)
        self.apply_btn.grid(row=2, column=2, sticky="w")

        self.status_var = tk.StringVar(value=self._pending_status)
        ttk.Label(
            frame, textvariable=self.status_var, wraplength=380, justify="left"
        ).grid(row=3, column=0, columnspan=3, sticky="w", pady=(12, 8))

        self.hint_var = tk.StringVar(
            value=f"Duracao ativa para proxima 1a ordem: {self._duration}s"
        )
        ttk.Label(frame, textvariable=self.hint_var, foreground="#555").grid(
            row=4, column=0, columnspan=3, sticky="w"
        )

        btns = ttk.Frame(frame)
        btns.grid(row=5, column=0, columnspan=3, sticky="e", pady=(16, 0))
        self.start_btn = ttk.Button(btns, text="Iniciar", command=self._start)
        self.start_btn.grid(row=0, column=0, padx=(0, 8))
        self.stop_btn = ttk.Button(
            btns, text="Parar", command=self._stop_bot, state="disabled"
        )
        self.stop_btn.grid(row=0, column=1)

        self.entry.bind("<Return>", lambda _e: self._apply_duration())
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)
        self.root.after(self._status_poll_ms, self._poll_status)

        self.root.update_idletasks()
        w, h = self.root.winfo_width(), self.root.winfo_height()
        x = (self.root.winfo_screenwidth() - w) // 2
        y = (self.root.winfo_screenheight() - h) // 3
        self.root.geometry(f"+{x}+{y}")

    def run(self) -> None:
        self.root.mainloop()

    def _get_duration(self) -> int:
        with self._duration_lock:
            return self._duration

    def _apply_duration(self) -> None:
        raw = self.duration_var.get().strip()
        try:
            value = int(raw)
        except ValueError:
            messagebox.showerror("Valor invalido", "Digite um numero inteiro de segundos.")
            return
        if value < MIN_DURATION:
            messagebox.showerror(
                "Valor invalido",
                f"Minimo {MIN_DURATION}s para permitir ajuste no ciclo.",
            )
            return
        with self._duration_lock:
            self._duration = value
        fsm = self._fsm
        if fsm is not None:
            fsm.set_initial_duration(value)
        self.hint_var.set(f"Duracao ativa para proxima 1a ordem: {value}s")
        print(f"[painel] proxima 1a ordem = {value}s")

    def _ui_kick(self) -> None:
        """Forca o Tk a pintar agora (cliques nao podem esperar o bot)."""
        try:
            self.root.update_idletasks()
            self.root.update()
        except tk.TclError:
            pass

    def _wake_bot(self) -> None:
        """Acorda wait_price_update para o Parar/Fechar nao esperar o proximo tick."""
        with self._broker_lock:
            broker = self._broker
        if broker is not None:
            try:
                broker.wake()
            except Exception:
                pass

    def _set_status(self, text: str, *, urgent: bool = False) -> None:
        """Thread-safe. Bot usa delay; urgent=True prioriza a proxima pintura."""
        with self._status_lock:
            self._pending_status = text
            if urgent:
                self._status_urgent = True

    def _paint_status_now(self, text: str) -> None:
        """So na thread do Tk (botoes): feedback imediato sem esperar o delay."""
        self._set_status(text, urgent=True)
        self._shown_status = text
        self._last_paint_mono = time.monotonic()
        try:
            self.status_var.set(text)
        except tk.TclError:
            pass

    def _poll_status(self) -> None:
        if self._closing:
            return
        with self._status_lock:
            text = self._pending_status
            urgent = self._status_urgent
            self._status_urgent = False
            ended = self._ended_reason
            if ended is not None:
                self._ended_reason = None
        now = time.monotonic()
        changed = text != self._shown_status
        if changed and (
            urgent or (now - self._last_paint_mono) >= self._min_paint_gap_s
        ):
            self._shown_status = text
            self._last_paint_mono = now
            try:
                self.status_var.set(text)
            except tk.TclError:
                return
        if ended is not None:
            self._on_bot_ended(ended)
        try:
            self.root.after(self._status_poll_ms, self._poll_status)
        except tk.TclError:
            pass

    def _start(self) -> None:
        if self._bot_thread and self._bot_thread.is_alive():
            return
        # 1) feedback visual NA HORA  2) so depois sobe a thread do bot
        self._stop.clear()
        self.start_btn.configure(state="disabled")
        self.stop_btn.configure(state="normal")
        self._paint_status_now("Iniciando estrategia...")
        self._ui_kick()
        self.root.after(1, self._start_after_paint)

    def _start_after_paint(self) -> None:
        if self._closing:
            return
        self._apply_duration()
        self.root.after(1, self._spawn_bot_thread)

    def _spawn_bot_thread(self) -> None:
        if self._closing:
            return
        self._bot_thread = threading.Thread(
            target=self._bot_loop, name="pocket-bot", daemon=True
        )
        self._bot_thread.start()

    def _stop_bot(self) -> None:
        """Stand-by: para a estrategia, mantem o painel aberto."""
        self._stop.set()
        self._wake_bot()
        self.stop_btn.configure(state="disabled")
        self._paint_status_now("Parando... aguarde (stand-by)")
        self._ui_kick()
        # Nao faz join aqui — senao a UI trava ate a API responder

    def _on_close(self) -> None:
        """Fechar janela = encerra run do codigo."""
        self._closing = True
        self._stop.set()
        self._wake_bot()
        self.start_btn.configure(state="disabled")
        self.stop_btn.configure(state="disabled")
        self._paint_status_now("Encerrando...")
        self._ui_kick()
        # Fecha a janela ja; limpeza em seguida (join curto)
        self.root.after(1, self._finish_close)

    def _finish_close(self) -> None:
        try:
            self.root.withdraw()
            self._ui_kick()
        except tk.TclError:
            pass
        t = self._bot_thread
        if t and t.is_alive():
            t.join(timeout=1.0)
        self._shutdown_broker()
        try:
            self.root.destroy()
        except tk.TclError:
            pass
        sys.exit(0)

    def _shutdown_broker(self) -> None:
        with self._broker_lock:
            broker = self._broker
            self._broker = None
        if broker is not None:
            try:
                broker.close()
            except Exception:
                pass

    def _ensure_broker(self) -> PocketBroker:
        with self._broker_lock:
            if self._broker is not None:
                return self._broker
        self._set_status("Conectando Pocket DEMO...")
        print("Conectando Pocket DEMO...")
        broker = PocketBroker(
            self.ssid,
            require_demo=True,
            min_payout=self.min_payout,
        )
        print(
            f"OK demo={broker.is_demo()} balance={broker.balance():.2f} asset={self.asset}"
        )
        self._set_status("Iniciando feed de preco...")
        px0 = broker.start_price_feed(self.asset, wait_seconds=10.0)
        if px0 is None:
            broker.close()
            raise RuntimeError("Feed nao trouxe preco a tempo.")
        print(f"Preco inicial={px0}")
        with self._broker_lock:
            self._broker = broker
        return broker

    def _bot_loop(self) -> None:
        ended_reason = "stand-by"
        try:
            broker = self._ensure_broker()
            if self._stop.is_set():
                ended_reason = "stand-by"
                return

            dur = self._get_duration()
            clock = PocketClock()
            # Mescla candles da API com 5/10/15/20/30 (UI); nao bloquear hedge.
            clock.set_allowed_durations(broker.load_allowed_durations(self.asset))
            fsm = PocketFSM(
                clock=clock,
                risk=_build_risk(),
                strategy=Strategy(
                    initial_direction="above",
                    use_limit_for_adjust=self.use_limit,
                ),
                broker=broker,
                config=_build_bot_config(asset=self.asset, initial_duration=dur),
            )
            self._fsm = fsm
            # Antes da 1a rodada: confirma 92% ou troca de ativo.
            self.asset = fsm.ensure_asset()
            print(f"Estrategia ON | asset={self.asset} | 1a ordem={dur}s\n")
            self._set_status(f"Rodando | {self.asset} | 1a ordem={dur}s")

            last_log = 0.0
            last_status = 0.0
            last_state = ""
            last_dur: int | None = None
            status_every = self._status_every_s
            while not self._stop.is_set() and not self._closing:
                t_iter = time.monotonic()
                dur_now = self._get_duration()
                if dur_now != last_dur:
                    fsm.set_initial_duration(dur_now)
                    last_dur = dur_now

                broker.wait_price_update(timeout=self.poll_timeout)
                if self._stop.is_set() or self._closing:
                    break

                now = datetime.now().astimezone()
                self.asset = fsm.config.asset
                try:
                    price = broker.get_price(self.asset)
                except Exception as exc:
                    print(f"preco indisponivel: {exc}")
                    time.sleep(0.05)
                    continue

                state = fsm.tick(now)
                self.asset = fsm.config.asset
                rem = ""
                anchor = fsm.cycle.anchor_expires_at
                if anchor is not None:
                    rem = f" resto={max(0.0, (anchor - now).total_seconds()):.0f}s"

                now_mono = time.monotonic()
                if (
                    state.value != last_state
                    or (now_mono - last_status) >= status_every
                ):
                    self._set_status(
                        f"{state.value}{rem} | {self.asset} | {price} | "
                        f"Sa={fsm.cycle.stake_above():.2f} Sb={fsm.cycle.stake_below():.2f} "
                        f"L{fsm.cycle.level} | p={fsm.risk.config.payout * 100:.0f}%",
                        urgent=(state.value != last_state),
                    )
                    last_status = now_mono

                if state.value != last_state or (now_mono - last_log) >= self.log_every:
                    open_pos = [
                        f"{p.direction}:{p.stake}@{p.entry_price}"
                        for p in fsm.cycle.open_positions
                    ]
                    print(
                        f"{now.strftime('%H:%M:%S.%f')[:-3]} [{self.asset}] price={price} "
                        f"state={state.value}{rem} "
                        f"Sa={fsm.cycle.stake_above():.2f} Sb={fsm.cycle.stake_below():.2f} "
                        f"levels={fsm.cycle.level} open={open_pos}"
                    )
                    last_log = now_mono
                    last_state = state.value

                if state.value == "STOP":
                    print("STOP:", fsm.stop_reason)
                    ended_reason = f"STOP: {fsm.stop_reason}"
                    break

                # Cede GIL ao Tk mesmo com feed inundando ticks.
                remain = self._loop_floor - (time.monotonic() - t_iter)
                if remain > 0:
                    time.sleep(remain)
            else:
                if self._stop.is_set() and not self._closing:
                    ended_reason = "stand-by"
                    print("[painel] estrategia em stand-by")
        except Exception as exc:
            print(f"Erro no bot: {exc}")
            ended_reason = f"Erro: {exc}"
        finally:
            self._fsm = None
            if not self._closing:
                with self._status_lock:
                    self._ended_reason = ended_reason
                    self._pending_status = (
                        "Stand-by. Ajuste o tempo e clique Iniciar."
                        if ended_reason == "stand-by"
                        else ended_reason
                    )

    def _on_bot_ended(self, reason: str) -> None:
        self.start_btn.configure(state="normal")
        self.stop_btn.configure(state="disabled")
        self.entry.configure(state="normal")
        self.apply_btn.configure(state="normal")
        if reason == "stand-by":
            msg = "Stand-by. Ajuste o tempo e clique Iniciar."
        else:
            msg = reason
        self._paint_status_now(msg)
        self._bot_thread = None
        self._ui_kick()


def _run_headless(args: argparse.Namespace) -> None:
    """Loop 24/7 para Railway (sem Tk). Exit 1 em STOP/erro → redeploy/restart."""
    ssid = _load_ssid()
    asset = _normalize_asset(args.asset or os.environ.get("POCKET_ASSET", "AUDCHF_otc"))
    poll_timeout = float(os.environ.get("POCKET_TICK_SECONDS", "0.05"))
    min_payout = int(os.environ.get("POCKET_MIN_PAYOUT", "50"))
    use_limit = _env_flag("POCKET_USE_LIMIT", "0")
    log_every = float(os.environ.get("POCKET_LOG_EVERY", "5.0"))
    initial_dur = args.duration
    if initial_dur is None:
        initial_dur = int(
            os.environ.get("POCKET_INITIAL_DURATION", str(DEFAULT_DURATION))
        )
    if initial_dur < MIN_DURATION:
        raise SystemExit(f"duration={initial_dur} invalido: minimo {MIN_DURATION}s.")

    print("Conectando Pocket DEMO...", flush=True)
    broker = PocketBroker(ssid, require_demo=True, min_payout=min_payout)
    print(
        f"OK demo={broker.is_demo()} balance={broker.balance():.2f} asset={asset}",
        flush=True,
    )
    px0 = broker.start_price_feed(asset, wait_seconds=15.0)
    if px0 is None:
        raise SystemExit("Feed nao trouxe preco a tempo.")
    clock = PocketClock()
    clock.set_allowed_durations(broker.load_allowed_durations(asset))
    print(f"Preco inicial={px0} | 1a ordem={initial_dur}s", flush=True)

    fsm = PocketFSM(
        clock=clock,
        risk=_build_risk(),
        strategy=Strategy(initial_direction="above", use_limit_for_adjust=use_limit),
        broker=broker,
        config=_build_bot_config(asset=asset, initial_duration=initial_dur),
    )
    asset = fsm.ensure_asset()
    print(f"Estrategia ON | asset={asset} | 1a ordem={initial_dur}s\n", flush=True)

    last_log = 0.0
    last_state = ""
    exit_code = 0
    try:
        while True:
            broker.wait_price_update(timeout=poll_timeout)
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
            rem = ""
            anchor = fsm.cycle.anchor_expires_at
            if anchor is not None:
                rem = f" resto={max(0.0, (anchor - now).total_seconds()):.0f}s"

            now_mono = time.monotonic()
            if state.value != last_state or (now_mono - last_log) >= log_every:
                open_pos = [
                    f"{p.direction}:{p.stake}@{p.entry_price}"
                    for p in fsm.cycle.open_positions
                ]
                print(
                    f"{now.strftime('%H:%M:%S.%f')[:-3]} [{asset}] price={price} "
                    f"state={state.value}{rem} "
                    f"Sa={fsm.cycle.stake_above():.2f} Sb={fsm.cycle.stake_below():.2f} "
                    f"levels={fsm.cycle.level} open={open_pos}",
                    flush=True,
                )
                last_log = now_mono
                last_state = state.value

            if state.value == "STOP":
                print(f"STOP: {fsm.stop_reason}", flush=True)
                exit_code = 1
                break
    except KeyboardInterrupt:
        print("\nEncerrado.", flush=True)
        exit_code = 0
    except Exception as exc:
        print(f"Erro fatal headless: {exc}", flush=True)
        exit_code = 1
    finally:
        broker.close()
    raise SystemExit(exit_code)


def main() -> None:
    args = _parse_args()
    if args.no_gui:
        _run_headless(args)
        return

    if not _HAS_TK:
        raise SystemExit(
            "tkinter indisponivel neste ambiente. Use: python run_pocket_demo.py --no-gui"
        )

    ssid = _load_ssid()
    asset = _normalize_asset(args.asset or os.environ.get("POCKET_ASSET", "AUDCHF_otc"))
    # GUI: defaults mais leves (feed OTC e console engasgam o Tk no Windows).
    poll_timeout = float(os.environ.get("POCKET_TICK_SECONDS", "0.15"))
    min_payout = int(os.environ.get("POCKET_MIN_PAYOUT", "50"))
    use_limit = _env_flag("POCKET_USE_LIMIT", "0")
    log_every = float(os.environ.get("POCKET_LOG_EVERY", "5.0"))
    initial_dur = args.duration
    if initial_dur is None:
        initial_dur = int(
            os.environ.get("POCKET_INITIAL_DURATION", str(DEFAULT_DURATION))
        )

    panel = ControlPanel(
        ssid=ssid,
        asset=asset,
        initial_duration=initial_dur,
        use_limit=use_limit,
        min_payout=min_payout,
        poll_timeout=poll_timeout,
        log_every=log_every,
    )
    panel.run()


if __name__ == "__main__":
    main()
