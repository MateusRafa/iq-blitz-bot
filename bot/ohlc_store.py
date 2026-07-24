"""Cliente Supabase para upsert de candles OHLC."""

from __future__ import annotations

import os
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
