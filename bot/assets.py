"""Escolha de ativo por payout (entre ciclos da estrategia)."""

from __future__ import annotations


def choose_asset(
    payouts: dict[str, float],
    *,
    current: str,
    target: float = 0.92,
    otc_only: bool | None = None,
) -> tuple[str, float, str]:
    """Escolhe ativo para a proxima rodada.

    Regras:
    1) Se o atual ainda esta >= target, mantem.
    2) Senao, qualquer ativo >= target (maior payout; empate: nome).
    3) Se ninguem no target, o de maior payout.

    Retorna (asset, payout, motivo).
    """
    if not payouts:
        cur_p = 0.0
        return current, cur_p, "sem_payouts"

    if otc_only is None:
        otc_only = current.endswith("_otc")

    table = {
        a: float(p)
        for a, p in payouts.items()
        if p is not None and float(p) > 0
    }
    if otc_only:
        otc = {a: p for a, p in table.items() if a.endswith("_otc")}
        if otc:
            table = otc

    cur_p = table.get(current)
    if cur_p is None:
        # tenta alias simples
        alt = (
            current.replace("_otc", "")
            if current.endswith("_otc")
            else f"{current}_otc"
        )
        if alt in table:
            current = alt
            cur_p = table[alt]

    if cur_p is not None and cur_p + 1e-9 >= target:
        return current, cur_p, "mantem_target"

    at_target = {a: p for a, p in table.items() if p + 1e-9 >= target}
    if at_target:
        best = max(at_target.items(), key=lambda kv: (kv[1], kv[0] == current, kv[0]))
        return best[0], best[1], "troca_target"

    best = max(table.items(), key=lambda kv: (kv[1], kv[0] == current, kv[0]))
    return best[0], best[1], "melhor_disponivel"
