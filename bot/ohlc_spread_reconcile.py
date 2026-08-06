"""Reconcile candle-a-candle: OTC × EURUSD (Dukascopy).

Mantem OTC de fim de semana. So exige Dukascopy nas horas de sessao FX.
Usado pelo botao "Sincronizar agora" das ferramentas de spread.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from bot.dukascopy_fetch import fetch_eurusd_1h_rows_for_store
from bot.ohlc_store import TABLE_EURUSD, fetch_candles_range, upsert_candles


def is_fx_session_hour(ts: datetime) -> bool:
    """True se a hora UTC costuma ter bi5 Dukascopy (mercado FX aberto)."""
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    else:
        ts = ts.astimezone(timezone.utc)
    # Sabado inteiro fechado.
    if ts.weekday() == 5:
        return False
    # Domingo: abre ~21:00 UTC.
    if ts.weekday() == 6 and ts.hour < 21:
        return False
    return True


def _parse_hour(raw: Any) -> datetime | None:
    if not raw:
        return None
    if isinstance(raw, datetime):
        ts = raw
    else:
        try:
            ts = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
        except ValueError:
            return None
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    else:
        ts = ts.astimezone(timezone.utc)
    return ts.replace(minute=0, second=0, microsecond=0)


def _hour_key(ts: datetime) -> str:
    return ts.strftime("%Y-%m-%dT%H:%M:%S")


def missing_session_hours(
    otc_rows: list[dict[str, Any]],
    eurusd_rows: list[dict[str, Any]],
) -> list[datetime]:
    """Horas OTC em sessao FX sem vela EURUSD correspondente."""
    eu_keys: set[str] = set()
    for r in eurusd_rows:
        ts = _parse_hour(r.get("opened_at"))
        if ts is not None:
            eu_keys.add(_hour_key(ts))

    missing: list[datetime] = []
    seen: set[str] = set()
    for r in otc_rows:
        ts = _parse_hour(r.get("opened_at"))
        if ts is None or not is_fx_session_hour(ts):
            continue
        key = _hour_key(ts)
        if key in eu_keys or key in seen:
            continue
        seen.add(key)
        missing.append(ts)
    missing.sort()
    return missing


def _merge_ranges(
    hours: list[datetime], *, max_gap_hours: int = 1
) -> list[tuple[datetime, datetime]]:
    """Agrupa horas faltantes em intervalos [start, end) para download."""
    if not hours:
        return []
    ranges: list[tuple[datetime, datetime]] = []
    start = hours[0]
    prev = hours[0]
    for h in hours[1:]:
        if (h - prev).total_seconds() <= 3600 * max_gap_hours:
            prev = h
            continue
        ranges.append((start, prev + timedelta(hours=1)))
        start = h
        prev = h
    ranges.append((start, prev + timedelta(hours=1)))
    return ranges


def reconcile_eurusd_to_otc(
    *,
    otc_asset: str,
    otc_table: str,
    eurusd_asset: str = "EURUSD",
    days: int = 14,
) -> dict[str, Any]:
    """Pente fino: completa Dukascopy nas horas de sessao que o OTC tem.

    - Nao apaga OTC de fds.
    - So baixa EURUSD onde falta e o mercado FX estava aberto.
    - Janela: ultimos `days` (teto 90) a partir de agora.
    """
    days = max(1, min(int(days), 90))
    end = datetime.now(timezone.utc)
    start = (end - timedelta(days=days)).replace(
        minute=0, second=0, microsecond=0
    )

    otc_win = fetch_candles_range(
        otc_asset, timeframe="1h", table=otc_table, after=start
    )
    weekend_otc = 0
    session_otc = 0
    for r in otc_win:
        ts = _parse_hour(r.get("opened_at"))
        if ts is None:
            continue
        if is_fx_session_hour(ts):
            session_otc += 1
        else:
            weekend_otc += 1

    eu_win = fetch_candles_range(
        eurusd_asset, timeframe="1h", table=TABLE_EURUSD, after=start
    )

    missing = missing_session_hours(otc_win, eu_win)
    paired_before = session_otc - len(missing)

    upserted = 0
    fetched = 0
    ranges_n = 0
    errors: list[str] = []

    for r0, r1 in _merge_ranges(missing):
        ranges_n += 1
        try:
            part = fetch_eurusd_1h_rows_for_store(r0, r1, asset=eurusd_asset)
            fetched += len(part)
            if part:
                upserted += upsert_candles(part, table=TABLE_EURUSD)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{r0.isoformat()}→{r1.isoformat()}: {exc}"[:220])

    # Ponteira: garante a cauda recente mesmo sem gap listado.
    tip_start = end - timedelta(hours=48)
    try:
        tip = fetch_eurusd_1h_rows_for_store(
            tip_start, end, asset=eurusd_asset
        )
        if tip:
            tip_n = upsert_candles(tip, table=TABLE_EURUSD)
            upserted += tip_n
            fetched += len(tip)
    except Exception as exc:  # noqa: BLE001
        errors.append(f"tip: {exc}"[:200])

    # Reconta apos fill.
    eu_after_win = fetch_candles_range(
        eurusd_asset, timeframe="1h", table=TABLE_EURUSD, after=start
    )
    still_missing = missing_session_hours(otc_win, eu_after_win)

    return {
        "ok": len(errors) == 0 or upserted > 0 or not missing,
        "days": days,
        "otc_asset": otc_asset,
        "otc_table": otc_table,
        "eurusd_asset": eurusd_asset,
        "otc_in_window": len(otc_win),
        "otc_session_hours": session_otc,
        "otc_weekend_kept": weekend_otc,
        "eurusd_in_window_before": len(eu_win),
        "paired_session_before": max(0, paired_before),
        "gaps_found": len(missing),
        "gap_ranges": ranges_n,
        "upserted": upserted,
        "fetched": fetched,
        "gaps_remaining": len(still_missing),
        "errors": errors[:5],
        "from": start.isoformat(),
        "to": end.isoformat(),
    }
