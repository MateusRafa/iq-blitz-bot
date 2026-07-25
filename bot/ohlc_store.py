"""Cliente Supabase para upsert/leitura de candles OHLC."""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any

try:
    from supabase import Client, create_client
except ImportError:
    Client = None  # type: ignore[misc, assignment]
    create_client = None

TABLE = "ohlc_candles"
UPSERT_CHUNK = 200


def _service_role_key() -> str:
    return (
        (os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or "").strip()
        or (os.environ.get("SUPABASE_SERVICE_KEY") or "").strip()
    )


def cliente_supabase() -> "Client | None":
    if create_client is None:
        return None
    url = (os.environ.get("SUPABASE_URL") or "").strip()
    key = _service_role_key()
    if not url or not key:
        return None
    return create_client(url, key)


def supabase_ok() -> tuple[bool, str]:
    cli = cliente_supabase()
    if cli is None:
        if create_client is None:
            return False, 'Pacote "supabase" nao instalado.'
        return (
            False,
            "Defina SUPABASE_URL e SUPABASE_SERVICE_ROLE_KEY "
            "(ou SUPABASE_SERVICE_KEY) no ambiente.",
        )
    return True, ""


def upsert_candles(rows: list[dict[str, Any]]) -> int:
    """Upsert por (asset, timeframe, opened_at). Retorna quantas linhas enviadas."""
    if not rows:
        return 0
    ok, msg = supabase_ok()
    if not ok:
        raise RuntimeError(msg)
    sb = cliente_supabase()
    assert sb is not None
    total = 0
    for i in range(0, len(rows), UPSERT_CHUNK):
        chunk = rows[i : i + UPSERT_CHUNK]
        (
            sb.table(TABLE)
            .upsert(chunk, on_conflict="asset,timeframe,opened_at")
            .execute()
        )
        total += len(chunk)
    return total


def count_candles(asset: str, timeframe: str = "1h") -> int:
    """Quantidade de velas salvas no Supabase para asset+tf."""
    ok, msg = supabase_ok()
    if not ok:
        raise RuntimeError(msg)
    sb = cliente_supabase()
    assert sb is not None
    res = (
        sb.table(TABLE)
        .select("id", count="exact")
        .eq("asset", asset)
        .eq("timeframe", timeframe)
        .limit(1)
        .execute()
    )
    count = getattr(res, "count", None)
    if count is not None:
        return int(count)
    # Fallback se o client nao devolver count
    data = getattr(res, "data", None) or []
    return len(data)


def last_opened_at(asset: str, timeframe: str = "1h") -> datetime | None:
    """Maior opened_at salvo (UTC) ou None se vazio."""
    ok, msg = supabase_ok()
    if not ok:
        raise RuntimeError(msg)
    sb = cliente_supabase()
    assert sb is not None
    res = (
        sb.table(TABLE)
        .select("opened_at")
        .eq("asset", asset)
        .eq("timeframe", timeframe)
        .order("opened_at", desc=True)
        .limit(1)
        .execute()
    )
    data = getattr(res, "data", None) or []
    if not data:
        return None
    raw = data[0].get("opened_at")
    if not raw:
        return None
    if isinstance(raw, datetime):
        return raw if raw.tzinfo else raw.replace(tzinfo=timezone.utc)
    text = str(raw).replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def fetch_candles(
    asset: str,
    *,
    timeframe: str = "1h",
    limit: int = 200,
) -> list[dict[str, Any]]:
    """Candles mais recentes (ordem cronologica crescente para o grafico)."""
    ok, msg = supabase_ok()
    if not ok:
        raise RuntimeError(msg)
    sb = cliente_supabase()
    assert sb is not None
    lim = max(1, min(int(limit), 2000))
    res = (
        sb.table(TABLE)
        .select("opened_at,open,high,low,close,volume")
        .eq("asset", asset)
        .eq("timeframe", timeframe)
        .order("opened_at", desc=True)
        .limit(lim)
        .execute()
    )
    data = list(getattr(res, "data", None) or [])
    data.reverse()
    return data


def stored_summary(asset: str, timeframe: str = "1h") -> dict[str, Any]:
    """Resumo do que ja esta no banco (para a UI)."""
    try:
        n = count_candles(asset, timeframe)
        last = last_opened_at(asset, timeframe)
    except Exception as exc:  # noqa: BLE001
        return {
            "stored_count": None,
            "stored_last": None,
            "stored_err": str(exc)[:200],
        }
    return {
        "stored_count": n,
        "stored_last": last.isoformat() if last else None,
        "stored_err": None,
    }
