"""Broker Pocket Option via BinaryOptionsToolsV2 (SSID / WebSocket não oficial).

Use apenas conta DEMO até validar. O SSID dá acesso total à sessão —
não compartilhe e não commite em git.
"""

from __future__ import annotations

import json
import threading
import time
from datetime import datetime, timedelta
from typing import Any

from bot.position import Direction


def _parse_price(payload: Any) -> float | None:
    if payload is None:
        return None
    if isinstance(payload, (int, float)):
        return float(payload)
    if isinstance(payload, dict):
        for key in ("close", "Close", "price", "value", "bid", "ask", "openPrice"):
            if key in payload and payload[key] is not None:
                try:
                    return float(payload[key])
                except (TypeError, ValueError):
                    continue
    return None


class PocketBroker:
    """Adapter síncrono para a FSM (get_price / open_order / place_limit).

    Usa DOIS clientes PocketOption:
    - trade: buy/sell/limit
    - price: subscribe em thread (evita deadlock no event loop único)
    """

    def __init__(
        self,
        ssid: str,
        *,
        require_demo: bool = True,
        connect_wait_seconds: float = 5.0,
        min_payout: int = 50,
        price_interval_ms: int = 50,  # mantido por compat; timed hangueia — não usado
    ) -> None:
        try:
            from BinaryOptionsToolsV2.pocketoption import PocketOption
        except ImportError as exc:  # pragma: no cover
            raise ImportError(
                "Instale BinaryOptionsToolsV2: pip install BinaryOptionsToolsV2"
            ) from exc

        self._ssid = ssid.strip()
        self._min_payout = min_payout
        self._price_interval_ms = price_interval_ms
        self._client = PocketOption(ssid=self._ssid)
        time.sleep(connect_wait_seconds)

        if require_demo and not self._client.is_demo():
            self.close()
            raise RuntimeError(
                "Conta REAL detectada. Para operar demo, use SSID com isDemo:1 "
                "ou logue na conta demo da Pocket. "
                "(Override perigoso: require_demo=False)"
            )

        self._price_client = PocketOption(ssid=self._ssid)
        time.sleep(2.0)

        self._last_price: dict[str, float] = {}
        self._lock = threading.Lock()
        self._price_event = threading.Event()
        self._stop_feed = threading.Event()
        self._feed_thread: threading.Thread | None = None
        self._feed_asset: str | None = None
        self._allowed_by_asset: dict[str, tuple[int, ...]] = {}
        self._last_price_signal = 0.0
        # Evita acordar o bot a cada tick do feed (OTC e barulhento).
        self._price_signal_min_s = 0.08
        self._payout_cache: dict[str, tuple[float, float]] = {}  # asset -> (mono, fraction)
        self._payout_ttl_s = 2.0
        self._all_payouts_cache: tuple[float, dict[str, float]] | None = None
        self._all_payouts_ttl_s = 5.0
        # Pocket as vezes responde openPendingOrder com {_placeholder, num} sem ticket.
        # Apos isso, pular pendente → mercado imediato (evita atrasar e alargar o gap).
        self._pending_api_ok = True
        self._require_demo = require_demo
        self._connect_wait_seconds = float(connect_wait_seconds)
        self._last_feed_mono = 0.0
        self._reconnect_lock = threading.Lock()

    @property
    def pending_api_ok(self) -> bool:
        return self._pending_api_ok

    def _disable_pending_api(self, reason: str) -> None:
        if not self._pending_api_ok:
            return
        self._pending_api_ok = False
        print(
            f"  !! pendente API indisponivel nesta sessao ({reason}); "
            f"ajustes seguem so a MERCADO (sem atrasar o fill)"
        )

    def feed_age_seconds(self) -> float:
        """Segundos desde o ultimo tick de preco no feed (inf se nunca recebeu)."""
        if self._last_feed_mono <= 0:
            return float("inf")
        return time.monotonic() - self._last_feed_mono

    def reconnect(
        self,
        *,
        asset: str | None = None,
        connect_wait_seconds: float | None = None,
        feed_wait_seconds: float = 15.0,
    ) -> float | None:
        """Recria clientes WebSocket apos queda (half-closed channel, etc.).

        Mantem a mesma instancia do broker (a FSM continua apontando para self).
        Retorna o primeiro preco do feed ou None.
        """
        with self._reconnect_lock:
            target = asset or self._feed_asset
            wait = (
                self._connect_wait_seconds
                if connect_wait_seconds is None
                else float(connect_wait_seconds)
            )
            print("  !! reconectando Pocket (WS)...")
            self._stop_feed.set()
            self._price_event.set()
            t = self._feed_thread
            if t is not None and t.is_alive():
                t.join(timeout=3.0)
            self._feed_thread = None

            for client in (self._price_client, self._client):
                try:
                    if self._feed_asset:
                        client.unsubscribe(self._feed_asset)
                except Exception:
                    pass
                try:
                    client.close()
                except Exception:
                    pass

            from BinaryOptionsToolsV2.pocketoption import PocketOption

            self._client = PocketOption(ssid=self._ssid)
            time.sleep(wait)
            if self._require_demo and not self._client.is_demo():
                raise RuntimeError(
                    "Apos reconnect: conta REAL detectada (isDemo != 1)."
                )
            self._price_client = PocketOption(ssid=self._ssid)
            time.sleep(2.0)

            self._stop_feed.clear()
            self._payout_cache.clear()
            self._all_payouts_cache = None
            self._last_feed_mono = 0.0
            # Nao reativa pendente automaticamente; continua mercado se ja tinha falhado.

            if not target:
                print("  << reconnect clientes ok (sem asset de feed)")
                return None
            px = self.start_price_feed(target, wait_seconds=feed_wait_seconds)
            if px is not None:
                print(f"  << reconnect ok asset={target} price={px}")
            else:
                print(f"  !! reconnect: clientes ok mas feed sem preco ({target})")
            return px

    @property
    def client(self) -> Any:
        return self._client

    def is_demo(self) -> bool:
        return bool(self._client.is_demo())

    def balance(self) -> float:
        return float(self._client.balance())

    def load_allowed_durations(self, asset: str) -> tuple[int, ...]:
        """Le allowed_candles do ativo (evita IncorrectExpTime)."""
        from bot.clock import POCKET_COMMON_DURATIONS

        if asset in self._allowed_by_asset:
            return self._allowed_by_asset[asset]

        allowed: list[int] = []
        try:
            assets = self._client.active_assets() or []
        except Exception as exc:
            print(f"  !! active_assets falhou ({exc}); usando tempos padrao")
            assets = []

        aliases = {asset, asset.replace("_otc", ""), f"{asset}_otc" if not asset.endswith("_otc") else asset}
        for item in assets:
            if not isinstance(item, dict):
                continue
            sym = str(item.get("symbol") or item.get("name") or "")
            if sym not in aliases and not any(a in sym for a in aliases):
                continue
            candles = item.get("allowed_candles") or item.get("allowedCandles") or []
            for c in candles:
                try:
                    if isinstance(c, dict):
                        # alguns payloads: {"time": 60} ou similar
                        val = c.get("time", c.get("duration", c.get("period")))
                    else:
                        val = c
                    if val is not None:
                        allowed.append(int(val))
                except (TypeError, ValueError):
                    continue
            if allowed:
                print(f"  (asset) {sym} allowed_candles={sorted(set(allowed))}")
                break

        if not allowed:
            allowed = list(POCKET_COMMON_DURATIONS)
            print(f"  (asset) {asset} sem allowed_candles; padrao={list(allowed)}")

        result = tuple(sorted(set(allowed)))
        self._allowed_by_asset[asset] = result
        # tambem indexa aliases
        for a in aliases:
            self._allowed_by_asset[a] = result
        return result

    def start_price_feed(self, asset: str, *, wait_seconds: float = 5.0) -> float | None:
        """Inicia subscribe no cliente de preço (subscribe padrão — estável)."""
        self._feed_asset = asset
        self._stop_feed.clear()

        if self._feed_thread is None or not self._feed_thread.is_alive():
            self._feed_thread = threading.Thread(
                target=self._feed_loop,
                args=(asset,),
                name=f"pocket-price-{asset}",
                daemon=True,
            )
            self._feed_thread.start()

        deadline = time.time() + wait_seconds
        while time.time() < deadline:
            px = self._cached_price(asset)
            if px is not None:
                return px
            time.sleep(0.05)

        print(
            "  !! feed sem preço em "
            f"{wait_seconds:.0f}s (SSID/asset/subscribe)"
        )
        return None

    def switch_asset(self, asset: str, *, wait_seconds: float = 8.0) -> float | None:
        """Troca o feed de preco para outro ativo (entre ciclos)."""
        if asset == self._feed_asset:
            px = self._cached_price(asset)
            if px is not None:
                return px
        print(f"  (asset) trocando feed -> {asset}...")
        self._stop_feed.set()
        self._price_event.set()
        t = self._feed_thread
        if t is not None and t.is_alive():
            t.join(timeout=2.0)
        self._feed_thread = None
        self._stop_feed.clear()
        self._feed_asset = None
        return self.start_price_feed(asset, wait_seconds=wait_seconds)

    def list_payouts(self) -> dict[str, float]:
        """Mapa asset → payout (fração). Cache ~5s."""
        now = time.monotonic()
        if (
            self._all_payouts_cache is not None
            and (now - self._all_payouts_cache[0]) < self._all_payouts_ttl_s
        ):
            return dict(self._all_payouts_cache[1])

        out: dict[str, float] = {}
        try:
            raw = self._client.payout()
        except Exception as exc:
            print(f"  !! payout() geral falhou: {exc}")
            raw = None

        if isinstance(raw, dict):
            # formato comum: {asset: 92} ou {asset: {call:92, put:91}}
            for key, val in raw.items():
                frac = self._normalize_payout(val)
                if frac is not None and frac > 0:
                    out[str(key)] = frac
                    self._payout_cache[str(key)] = (now, frac)

        if not out:
            # fallback: tenta active_assets se houver payout no item
            try:
                assets = self._client.active_assets() or []
            except Exception:
                assets = []
            for item in assets:
                if not isinstance(item, dict):
                    continue
                sym = str(item.get("symbol") or item.get("name") or "")
                if not sym:
                    continue
                frac = self._normalize_payout(
                    item.get("payout")
                    or item.get("profit")
                    or item.get("percent")
                )
                if frac is not None and frac > 0:
                    out[sym] = frac

        self._all_payouts_cache = (now, dict(out))
        return out

    def select_asset(
        self,
        current: str,
        *,
        target: float = 0.92,
        otc_only: bool | None = None,
    ) -> tuple[str, float, str]:
        from bot.assets import choose_asset

        table = self.list_payouts()
        if current not in table:
            try:
                table[current] = self.get_payout(current)
            except Exception:
                pass
        return choose_asset(
            table, current=current, target=target, otc_only=otc_only
        )

    def wait_price_update(self, timeout: float = 0.05) -> bool:
        """Espera próximo tick de preço (ou timeout). Retorna True se houve update."""
        # Se wake() ja sinalizou (Parar), nao limpa o sinal antes de ver.
        if self._price_event.is_set():
            self._price_event.clear()
            return True
        return self._price_event.wait(timeout)

    def wake(self) -> None:
        """Acorda wait_price_update (Parar/Fechar no painel)."""
        self._price_event.set()

    def get_price(self, asset: str) -> float:
        px = self._cached_price(asset)
        if px is not None:
            return px
        raise RuntimeError(f"Preço de {asset} ainda não disponível no feed")

    def get_payout(self, asset: str) -> float:
        """Payout vigente do ativo em fração (0.85 = 85%). Cache curto."""
        now = time.monotonic()
        cached = self._payout_cache.get(asset)
        if cached is not None and (now - cached[0]) < self._payout_ttl_s:
            return cached[1]

        raw = self._client.payout(asset)
        fraction = self._normalize_payout(raw)
        if fraction is None:
            # tenta aliases
            aliases = [
                asset,
                asset.replace("_otc", ""),
                f"{asset}_otc" if not asset.endswith("_otc") else asset,
            ]
            for alt in aliases:
                if alt == asset:
                    continue
                try:
                    fraction = self._normalize_payout(self._client.payout(alt))
                except Exception:
                    fraction = None
                if fraction is not None:
                    break
        if fraction is None or fraction <= 0:
            # mantem ultimo conhecido ou fallback conservador
            if cached is not None:
                return cached[1]
            return 0.85
        self._payout_cache[asset] = (now, fraction)
        return fraction

    @staticmethod
    def _normalize_payout(raw: Any) -> float | None:
        """Aceita 85, 0.85, [85,84], {'call':85,'put':84} → fração (usa o menor)."""
        if raw is None:
            return None
        values: list[float] = []
        if isinstance(raw, dict):
            for key in ("call", "put", "above", "below", "payout", "percent", "value"):
                if key in raw and raw[key] is not None:
                    try:
                        values.append(float(raw[key]))
                    except (TypeError, ValueError):
                        pass
            if not values:
                for v in raw.values():
                    try:
                        values.append(float(v))
                    except (TypeError, ValueError):
                        continue
        elif isinstance(raw, (list, tuple)):
            for v in raw:
                try:
                    values.append(float(v))
                except (TypeError, ValueError):
                    continue
        else:
            try:
                values.append(float(raw))
            except (TypeError, ValueError):
                return None
        if not values:
            return None
        # API costuma devolver percentuais (70..95); se vier fração, deixa.
        as_frac = [v / 100.0 if v > 1.5 else v for v in values]
        return min(as_frac)

    def open_order(
        self,
        asset: str,
        direction: Direction,
        stake: float,
        expiration_seconds: int,
        *,
        opened_at: datetime | None = None,
    ) -> tuple[str, float, datetime]:
        opened = opened_at or datetime.now().astimezone()
        side = "BUY/call" if direction == "above" else "SELL/put"
        t0 = time.perf_counter()
        print(
            f"  >> MERCADO {side} stake={stake} dur={expiration_seconds}s asset={asset} "
            f"@{opened.strftime('%H:%M:%S.%f')[:-3]}"
        )
        # 1) tenta o tempo pedido (como na UI / como no AUDCAD antigo com 8s/23s)
        # 2) se IncorrectExpTime, tenta 30/20/15/10/5 <= pedido
        candidates: list[int] = [int(expiration_seconds)]
        for d in (30, 20, 15, 10, 5):
            if d < int(expiration_seconds) and d not in candidates:
                candidates.append(d)

        trade_id = None
        details: Any = None
        last_exc: Exception | None = None
        used = candidates[0]
        for dur in candidates:
            try:
                if direction == "above":
                    trade_id, details = self._client.buy(
                        asset, float(stake), int(dur), check_win=False
                    )
                else:
                    trade_id, details = self._client.sell(
                        asset, float(stake), int(dur), check_win=False
                    )
                used = dur
                if dur != int(expiration_seconds):
                    print(f"  !! IncorrectExpTime no pedido; aceitou dur={dur}s")
                break
            except Exception as exc:
                last_exc = exc
                msg = str(exc)
                if "IncorrectExpTime" not in msg and "ExpTime" not in msg:
                    raise
                print(f"  !! IncorrectExpTime dur={dur}s")
                continue
        else:
            raise last_exc or RuntimeError("Falha ao abrir ordem")

        entry = self._extract_entry(details, asset)
        ms = (time.perf_counter() - t0) * 1000
        print(f"  << aberta id={trade_id} entry={entry} dur={used}s (api {ms:.0f}ms)")
        return str(trade_id), entry, opened

    def _pending_open_time_unix(self) -> int:
        """Unix agora (get_server_time da lib as vezes devolve lixo ~22)."""
        ts = int(time.time())
        try:
            server_ts = int(self._client.get_server_time())
            if server_ts >= 1_700_000_000:
                ts = server_ts
        except Exception:
            pass
        return ts

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
        """Abre **pedido pendente** na Pocket (linha tracejada no grafico).

        Usa websocket raw `openPendingOrder` — o helper high-level da lib
        (open_pending_order) trava/erra no binding str/int.
        """
        opened = opened_at or datetime.now().astimezone()
        if not self._pending_api_ok:
            return self.open_order(
                asset,
                direction,
                stake,
                expiration_seconds,
                opened_at=opened,
            )

        from BinaryOptionsToolsV2.validator import Validator

        command = 0 if direction == "above" else 1
        side = "COMPRAR/call" if direction == "above" else "VENDER/put"
        open_time = self._pending_open_time_unix()

        # Uma tentativa so — retry atrasava o fallback mercado e alargava o gap.
        attempts = [open_time]
        errors: list[str] = []
        validator = Validator.any(
            [
                Validator.contains("successopenPendingOrder"),
                Validator.contains("failopenPendingOrder"),
            ]
        )

        for ot in attempts:
            payload = {
                "openType": 1,
                "amount": float(stake),
                "asset": asset,
                "openTime": int(ot),
                "openPrice": float(price),
                "timeframe": int(expiration_seconds),
                "minPayout": int(self._min_payout),
                "command": int(command),
            }
            msg = "42" + json.dumps(["openPendingOrder", payload], separators=(",", ":"))
            print(
                f"  >> PEDIDO PENDENTE {side} stake={stake} @ {price} "
                f"dur={expiration_seconds}s openTime={ot} min_payout={self._min_payout}"
            )
            try:
                raw = self._client.create_raw_order_with_timeout(
                    msg, validator, timedelta(seconds=8)
                )
                order_id, ok, detail = self._parse_pending_response(raw)
                if ok and order_id:
                    print(f"  << pedido pendente ok id={order_id}")
                    return order_id, float(price), opened
                errors.append(f"openTime={ot}: rejeitado ({detail})")
                print(f"  !! pedido pendente rejeitado openTime={ot}: {detail}")
                if self._is_pending_hard_fail(detail, raw):
                    self._disable_pending_api(detail)
                    break
            except Exception as exc:
                errors.append(f"openTime={ot}: {exc}")
                print(f"  !! pedido pendente falhou openTime={ot}: {exc}")

        print(f"  !! pedido pendente falhou ({' | '.join(errors)}); fallback MERCADO")
        return self.open_order(
            asset,
            direction,
            stake,
            expiration_seconds,
            opened_at=opened,
        )

    def place_pending(
        self,
        asset: str,
        direction: Direction,
        stake: float,
        price: float,
        expiration_seconds: int,
        *,
        opened_at: datetime | None = None,
    ) -> str | None:
        """Arma pedido pendente SEM fallback a mercado (seguro). None se falhar.

        Uma unica tentativa — retry com openTime=0 duplicava pedidos na Pocket.
        """
        if not self._pending_api_ok:
            return None

        from BinaryOptionsToolsV2.validator import Validator

        command = 0 if direction == "above" else 1
        side = "COMPRAR/call" if direction == "above" else "VENDER/put"
        open_time = self._pending_open_time_unix()
        validator = Validator.any(
            [
                Validator.contains("successopenPendingOrder"),
                Validator.contains("failopenPendingOrder"),
            ]
        )

        payload = {
            "openType": 1,
            "amount": float(stake),
            "asset": asset,
            "openTime": int(open_time),
            "openPrice": float(price),
            "timeframe": int(expiration_seconds),
            "minPayout": int(self._min_payout),
            "command": int(command),
        }
        msg = "42" + json.dumps(["openPendingOrder", payload], separators=(",", ":"))
        print(
            f"  >> SEGURO PENDENTE {side} stake={stake} @ {price} "
            f"dur={expiration_seconds}s openTime={open_time}"
        )
        try:
            raw = self._client.create_raw_order_with_timeout(
                msg, validator, timedelta(seconds=8)
            )
            order_id, ok, detail = self._parse_pending_response(raw)
            if ok and order_id:
                print(f"  << seguro pendente ok id={order_id}")
                return str(order_id)
            print(f"  !! seguro rejeitado: {detail}")
            if self._is_pending_hard_fail(detail, raw):
                self._disable_pending_api(detail)
        except Exception as exc:
            print(f"  !! seguro falhou: {exc}")
        return None

    @staticmethod
    def _is_pending_hard_fail(detail: str, raw: Any = None) -> bool:
        text = f"{detail} {raw}".lower()
        return (
            "_placeholder" in text
            or "sem id" in text
            or "'num'" in text
            or '"num"' in text
        )

    def cancel_pending(self, order_id: str) -> bool:
        """Cancela pedido pendente. Best-effort."""
        ticket = str(order_id)
        try:
            fn = getattr(self._client, "cancel_pending_order", None)
            if fn is not None:
                fn(ticket)
                print(f"  << cancel pendente id={ticket}")
                return True
        except Exception as exc:
            print(f"  !! cancel_pending_order: {exc}")

        try:
            from BinaryOptionsToolsV2.validator import Validator

            payload = {"ticket": ticket}
            msg = "42" + json.dumps(
                ["cancelPendingOrder", payload], separators=(",", ":")
            )
            validator = Validator.any(
                [
                    Validator.contains("successcancelPendingOrder"),
                    Validator.contains("failcancelPendingOrder"),
                    Validator.contains("cancelPendingOrder"),
                ]
            )
            self._client.create_raw_order_with_timeout(
                msg, validator, timedelta(seconds=8)
            )
            print(f"  << cancel pendente raw id={ticket}")
            return True
        except Exception as exc:
            print(f"  !! cancel pendente raw falhou id={ticket}: {exc}")
            return False

    def pending_still_open(self, order_id: str) -> bool:
        """True se o ticket ainda aparece em get_pending_deals."""
        target = str(order_id)
        try:
            pending = self._client.get_pending_deals()
        except Exception:
            return True
        if not isinstance(pending, list):
            return True
        for item in pending:
            if not isinstance(item, dict):
                continue
            for key in ("ticket", "id", "order_id"):
                if key in item and str(item[key]) == target:
                    return True
        return False

    @staticmethod
    def _parse_pending_response(raw: Any) -> tuple[str | None, bool, str]:
        """Parse resposta success/fail openPendingOrder."""
        text = raw if isinstance(raw, str) else str(raw)
        try:
            start = text.find("[")
            data = json.loads(text[start:] if start >= 0 else text)
        except Exception as exc:
            return None, False, f"json:{exc} raw={text[:200]}"

        event = None
        body: Any = data
        if isinstance(data, list) and len(data) >= 2:
            event = data[0]
            body = data[1]
        elif isinstance(data, dict):
            body = data

        if isinstance(event, str) and "fail" in event.lower():
            err = body.get("error", body) if isinstance(body, dict) else body
            return None, False, str(err)

        if isinstance(body, dict):
            # Lib as vezes devolve {_placeholder, num} sem ticket real.
            keys = list(body.keys())
            if "_placeholder" in body and not any(
                k in body and body[k] for k in ("ticket", "id", "order_id", "req_id")
            ):
                return None, False, f"placeholder sem ticket keys={keys}"
            for key in ("ticket", "id", "order_id", "req_id"):
                if key in body and body[key] is not None:
                    return str(body[key]), True, "ok"
            return None, False, f"sem id em {keys}"

        return str(body), True, "ok"

    def wait_limit_fill(self, order_id: str, *, timeout: float = 0.5) -> bool:
        """True se o pedido pendente ja nao esta na lista (executou ou cancelou)."""
        deadline = time.time() + max(timeout, 0.05)
        target = str(order_id)
        while time.time() < deadline:
            try:
                pending = self._client.get_pending_deals()
            except Exception:
                pending = []
            still_pending = False
            if isinstance(pending, list):
                for item in pending:
                    if not isinstance(item, dict):
                        continue
                    for key in ("ticket", "id", "order_id"):
                        if key in item and str(item[key]) == target:
                            still_pending = True
                            break
                    if still_pending:
                        break
            if not still_pending:
                return True
            time.sleep(0.05)
        return False

    def close(self) -> None:
        self._stop_feed.set()
        self._price_event.set()
        for client in (self._price_client, self._client):
            try:
                if self._feed_asset:
                    client.unsubscribe(self._feed_asset)
            except Exception:
                pass
            try:
                client.close()
            except Exception:
                pass

    def _cached_price(self, asset: str) -> float | None:
        with self._lock:
            return self._last_price.get(asset)

    def _set_price(self, asset: str, price: float) -> None:
        with self._lock:
            prev = self._last_price.get(asset)
            self._last_price[asset] = price
        self._last_feed_mono = time.monotonic()
        if prev is None:
            self._last_price_signal = time.monotonic()
            self._price_event.set()
            return
        if prev == price:
            return
        now = time.monotonic()
        if (now - self._last_price_signal) >= self._price_signal_min_s:
            self._last_price_signal = now
            self._price_event.set()

    def _feed_loop(self, asset: str) -> None:
        """Subscribe padrão (subscribe_symbol_timed trava na lib — não usar)."""
        while not self._stop_feed.is_set():
            try:
                print(f"  (feed) subscribe {asset}...")
                stream = self._price_client.subscribe_symbol(asset)
                first = True
                last_yield = 0.0
                for candle in stream:
                    if self._stop_feed.is_set():
                        break
                    px = _parse_price(candle)
                    if px is not None:
                        if first:
                            print(f"  (feed) ok primeiro preço={px}")
                            first = False
                        self._set_price(asset, px)
                    # Cede GIL ao Tk — senao Parar/Fechar demoram para reagir.
                    now = time.monotonic()
                    if (now - last_yield) >= 0.05:
                        time.sleep(0.01)
                        last_yield = now
            except Exception as exc:
                print(f"  (feed) erro: {exc}; retry em 2s")
                time.sleep(2.0)

    def _extract_entry(self, details: Any, asset: str) -> float:
        if isinstance(details, dict):
            for key in ("open_price", "openPrice", "price", "entry_price", "value"):
                if key in details and details[key] is not None:
                    try:
                        val = float(details[key])
                        self._set_price(asset, val)
                        return val
                    except (TypeError, ValueError):
                        pass
            px = _parse_price(details)
            if px is not None:
                self._set_price(asset, px)
                return px
        cached = self._cached_price(asset)
        if cached is not None:
            return cached
        return 0.0

    @staticmethod
    def _extract_pending_id(result: Any) -> str:
        if isinstance(result, dict):
            for key in ("ticket", "id", "order_id", "req_id"):
                if key in result and result[key] is not None:
                    return str(result[key])
        return str(result)
