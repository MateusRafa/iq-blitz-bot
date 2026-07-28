"""Spread OTC vs EURUSD (1h): serie para o grafico /ohlc-spread.

spread = close_otc - close_eurusd
No fim de semana (EURUSD fechado): carrega o ultimo close EURUSD (sexta)
e compara com cada vela OTC de sabado/domingo.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def _parse_ts(raw: Any) -> datetime | None:
    if not raw:
        return None
    try:
        ts = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
    except ValueError:
        return None
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)


def _key_hour(raw: Any) -> str | None:
    ts = _parse_ts(raw)
    if ts is None:
        return None
    ts = ts.replace(minute=0, second=0, microsecond=0)
    return ts.strftime("%Y-%m-%dT%H:%M:%S")


def build_spread_1h(
    otc_rows: list[dict[str, Any]],
    eurusd_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Constroi pontos de spread alinhados ao tempo das velas OTC.

    Cada ponto:
      time (unix s), spread, otc_close, eurusd_close, mode (paired|carry), weekend (bool)
    """
    by_eu: dict[str, dict[str, Any]] = {}
    for r in eurusd_rows:
        k = _key_hour(r.get("opened_at"))
        if k:
            by_eu[k] = r

    points: list[dict[str, Any]] = []
    last_eu_close: float | None = None

    # Ordena OTC e EURUSD cronologicamente para forward-fill correto.
    otc_sorted = sorted(
        otc_rows, key=lambda r: str(r.get("opened_at") or "")
    )
    # Preenche last_eu_close com o primeiro EURUSD antes do 1o OTC, se houver.
    eu_sorted = sorted(
        eurusd_rows, key=lambda r: str(r.get("opened_at") or "")
    )
    eu_i = 0

    for row in otc_sorted:
        ts = _parse_ts(row.get("opened_at"))
        if ts is None:
            continue
        key = _key_hour(row.get("opened_at"))
        if not key:
            continue
        try:
            otc_c = float(row["close"])
        except (TypeError, ValueError, KeyError):
            continue

        # Avanca EURUSD ate <= tempo atual (forward fill).
        while eu_i < len(eu_sorted):
            eu_ts = _parse_ts(eu_sorted[eu_i].get("opened_at"))
            if eu_ts is None or eu_ts > ts:
                break
            try:
                last_eu_close = float(eu_sorted[eu_i]["close"])
            except (TypeError, ValueError, KeyError):
                pass
            eu_i += 1

        weekday = ts.weekday()  # 0=seg … 5=sab 6=dom
        is_weekend = weekday >= 5
        paired = key in by_eu

        if paired:
            try:
                eu_c = float(by_eu[key]["close"])
            except (TypeError, ValueError, KeyError):
                eu_c = last_eu_close
            mode = "paired"
        else:
            eu_c = last_eu_close
            mode = "carry"

        if eu_c is None:
            continue

        if paired:
            last_eu_close = eu_c

        points.append(
            {
                "time": int(ts.timestamp()),
                "opened_at": ts.isoformat(),
                "spread": otc_c - eu_c,
                "otc_close": otc_c,
                "eurusd_close": eu_c,
                "mode": mode,
                "weekend": is_weekend or mode == "carry",
            }
        )
    return points
