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

## Exposição Sa / Sb

```
win_pool(D) = soma(stake_i * payout_i) no lado D
S_other     = soma(stake) do lado oposto
delta       = ceil((buffer - win_pool(D) + S_other) / payout_atual)
```

- Âncora de marcação = **entry da 1ª ordem** (modelo Sa/Sb).
- Não dimensionar por ITM/OTM de cada entry: na zona entre entradas isso explode a stake.
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

- `base_stake`, `max_stake`, `max_levels`, `daily_loss_limit`
- Teste só em **DEMO** (`isDemo:1`).

## Deploy Railway (DEMO 24/7)

1. Testar local: `python run_pocket_demo.py --no-gui`
2. Subir repo no GitHub (sem SSID no código).
3. Railway → New Project → GitHub → este repo.
4. Start command: `python run_pocket_demo.py --no-gui` (já em `railway.toml` / `Procfile`).
5. Variáveis: copiar de `.env.example` (SSID DEMO obrigatório).
6. Confirmar `PYTHONPATH=.` e `PYTHONUNBUFFERED=1`.
7. Abrir **Logs**: connect → feed → ordens.
8. Conta real só depois de estável em DEMO (e liberar `isDemo:0` no código).
