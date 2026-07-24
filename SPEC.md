# Pocket Bot — Ciclo âncora-T (mercado + pendente na âncora)

## Objetivo

Gestão de posições em opções de tempo fixo na Pocket Option, com **vencimento
alinhado**. Não é arbitragem. Meta: no vencimento comum `T`, o lado reforçado
cobre o lado oposto e sobra `buffer` (padrão $0,30).

Ideal: hedge/reforço na **entry da 1ª** (pedido pendente) para reduzir a zona
morta. Na prática a API `openPendingOrder` pode falhar (`_placeholder` sem
ticket) — nesse caso o bot **desliga pendente na sessão** e segue só a mercado
(sem atrasar o fill).

## Relógio

- A **1ª ordem** (mercado) define a âncora `T = opened_at + duration_inicial`.
- Duração inicial: editável (default 10s).
- Toda ordem seguinte: `duration = max(min_duration, T − agora)`.
- `min_duration` = **5s**. Se `T − agora < 5s`, **não abre** nova ordem.

## Estados

| Estado   | Descrição |
|----------|-----------|
| IDLE     | Sem ciclo; pronto para stake base |
| EVALUATE | Marca Sa/Sb; decide recuperação/reforço |
| OPEN     | Envia ordem (mercado ou pendente na âncora) |
| TRACK    | Monitora preço até reavaliar ou `T` |
| HOLD     | `resto_T < 5s`; espera settle sem abrir |
| SETTLE   | Liquida abertas em `T` |
| STOP     | Limites de risco |

## Transições

```
IDLE → EVALUATE → OPEN → TRACK
TRACK ⇄ EVALUATE → OPEN
HOLD (resto < 5s) → SETTLE → IDLE | STOP
TRACK → SETTLE (now ≥ T)
```

## Marcação (âncora na 1ª ordem)

- Preço contra a 1ª → abre/reforça hedge
- Preço a favor da 1ª e já tem hedge → reforça o lado da 1ª
- Preço a favor sem hedge → não abre
- **Cooldown anti-whipsaw:** após um ajuste, não inverte o lado por
  `POCKET_ADJUST_COOLDOWN` segundos (padrão **8s**). Reforço no mesmo lado ok.

## Ativo / payout alvo

- Alvo padrão: **92%** (`POCKET_TARGET_PAYOUT=0.92`).
- **Durante a rodada:** nunca troca de ativo; se o payout cair, só ajusta stake (buffer $0,30).
- **Ao fim da rodada (SETTLE → IDLE):** confirma se o ativo ainda está ≥ alvo.
  - Se sim → nova rodada no mesmo ativo.
  - Se não → procura outro (OTC) ≥ alvo; se ninguém tiver, pega o **maior payout**.
- Desligar: `POCKET_AUTO_ASSET=0`.

## Exposição Sa / Sb + gate de lucro

```
win_pool(D) = soma(stake_i * payout_i) no lado D
S_other     = soma(stake) do lado oposto
delta       = ceil((buffer - win_pool(D) + S_other) / payout_atual)
```

- Âncora de marcação = **entry da 1ª ordem** (modelo Sa/Sb).
- **Gate de lucro (`POCKET_PROFIT_GUARD=1`, default):** enquanto o lado que o
  preço favorece projetar PnL &lt; `buffer`, o bot **reforça esse lado** (a
  mercado). Fora da zona morta, também reage se o mark PnL (por entry) &lt; buffer.
- Zona morta (Acima e Abaixo ambos OTM): não explode stake por ITM/OTM; usa só Sa/Sb.
- Se não puder reparar (`resto &lt; 5s` ou `max_levels` + bônus esgotados) → **HOLD**
  com log `lucro descoberto`.
- Bônus de níveis só para reparo: `POCKET_REPAIR_LEVEL_BONUS` (padrão **4**).
- `buffer` alvo (padrão **$0,30**; env `POCKET_BUFFER`).
- `payout_atual` vem da Pocket a cada avaliação (ordens antigas mantêm o payout delas).
- Se o payout cair no meio do ciclo, o cruzamento aumenta a stake para preservar o buffer.
- Se o payout subir, a stake do ajuste pode ser menor.

## Ordens

- **1ª ordem:** sempre a **mercado**.
- **Ajustes (2ª, …):** default a **mercado** (rápido). Opcional:
  `POCKET_USE_LIMIT=1` tenta pendente na entry da 1ª; se a API falhar, circuit
  breaker → só mercado pelo resto da sessão.
- **Seguro:** `POCKET_PREPLACE_LIMIT=1` arma pendente na entry da 1ª quando
  PnL ≥ buffer (sustain `POCKET_PREPLACE_SUSTAIN`, default 0s). Mesmo circuit
  breaker se a API não devolver ticket.
- Stake mínimo: **$1**.

## Riscos

- `base_stake`, `max_stake`, `max_levels`, `repair_level_bonus`, `daily_loss_limit`
- Teste só em **DEMO** (`isDemo:1`).

## Deploy Railway (portal web + bot)

1. Start: `uvicorn web.app:app --host 0.0.0.0 --port $PORT` (`railway.toml`).
2. Variáveis: `.env.example` + **`CONTROL_TOKEN`** + SSID DEMO.
3. Gerar domínio público (Networking).
4. Abrir URL → portal → card **Pocket Bot** → token → Iniciar / Parar + gráfico PnL.
5. Bot fica em **stand-by** até Iniciar (não opera sozinho no boot).
6. Conta real só depois de estável em DEMO.

Local: `PYTHONPATH=. uvicorn web.app:app --reload --port 8080`
