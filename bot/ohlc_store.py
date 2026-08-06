"""Cliente Supabase para upsert/leitura de candles OHLC."""

from __future__ import annotations

import csv
import io
import os
from datetime import datetime, timedelta, timezone
from typing import Any

try:
    from supabase import Client, create_client
except ImportError:
    Client = None  # type: ignore[misc, assignment]
    create_client = None

TABLE = "ohlc_candles"
TABLE_1M = "ohlc_candles_1m"
TABLE_1D = "ohlc_candles_1d"  # coletor diario OTC (/ohlc-1d)
TABLE_EURUSD = "ohlc_candles_eurusd"  # EURUSD mercado 1h (/ohlc-spread)
TABLE_EURUSD_1D = "ohlc_candles_eurusd_1d"  # EURUSD mercado 1D (/ohlc-spread-1d)
TABLE_OLYMP = "ohlc_candles_olymp"  # OTC Olymptrade 1h (/ohlc-spread-olymp)
UPSERT_CHUNK = 200
FETCH_PAGE = 1000

# Retencao da ferramenta 1m
RETENTION_DAYS_1M = 90
WARN_BEFORE_DAYS_1M = 1


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


def _parse_opened_at(raw: Any) -> datetime | None:
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


def upsert_candles(
    rows: list[dict[str, Any]], *, table: str = TABLE
) -> int:
    """Upsert por (asset, timeframe, opened_at). Retorna quantas linhas enviadas."""
    if not rows:
        return 0
    ok, msg = supabase_ok()
    if not ok:
        raise RuntimeError(msg)
    sb = cliente_supabase()
    assert sb is not None
    now_iso = datetime.now(timezone.utc).isoformat()
    total = 0
    for i in range(0, len(rows), UPSERT_CHUNK):
        chunk = []
        for row in rows[i : i + UPSERT_CHUNK]:
            try:
                o = float(row["open"])
                h = float(row["high"])
                lo = float(row["low"])
                c = float(row["close"])
            except (KeyError, TypeError, ValueError):
                continue
            # NaN/Inf quebram o PostgREST ("Value is null" / JSON invalid).
            if not all(map(_finite, (o, h, lo, c))):
                continue
            opened = row.get("opened_at")
            asset = row.get("asset")
            tf = row.get("timeframe") or "1h"
            if not opened or not asset:
                continue
            vol_raw = row.get("volume")
            try:
                vol = float(vol_raw) if vol_raw is not None else 0.0
            except (TypeError, ValueError):
                vol = 0.0
            if not _finite(vol):
                vol = 0.0
            # Mesmas chaves em TODAS as linhas do chunk (evita null em bulk upsert).
            src = (row.get("source") or "").strip()
            if not src:
                if table == TABLE_EURUSD:
                    src = "dukascopy"
                elif table == TABLE_EURUSD_1D:
                    src = "dukascopy_agg"
                elif table == TABLE_OLYMP:
                    src = "olymptrade"
                else:
                    src = "pocket"
            item = {
                "asset": str(asset),
                "timeframe": str(tf),
                "opened_at": opened,
                "open": o,
                "high": max(h, o, c),
                "low": min(lo, o, c),
                "close": c,
                "volume": vol,
                "source": src,
                "updated_at": row.get("updated_at") or now_iso,
            }
            chunk.append(item)
        if not chunk:
            continue
        upsert_kw: dict[str, Any] = {"on_conflict": "asset,timeframe,opened_at"}
        # Preferir DEFAULT do DB em campos ausentes (clientes novos).
        try:
            (
                sb.table(table)
                .upsert(chunk, default_to_null=False, **upsert_kw)
                .execute()
            )
        except TypeError:
            (
                sb.table(table)
                .upsert(chunk, **upsert_kw)
                .execute()
            )
        total += len(chunk)
    return total


def _finite(x: float) -> bool:
    return x == x and x not in (float("inf"), float("-inf"))


def _opened_key(raw: Any) -> str | None:
    if raw is None:
        return None
    s = str(raw).replace("Z", "+00:00")
    try:
        ts = datetime.fromisoformat(s)
    except ValueError:
        return s[:19] if s else None
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    ts = ts.astimezone(timezone.utc).replace(second=0, microsecond=0)
    return ts.strftime("%Y-%m-%dT%H:%M:%S")


def _is_flat_ohlc(o: float, h: float, lo: float, c: float) -> bool:
    return max(h, o, c) - min(lo, o, c) <= 1e-12


def sanitize_ohlc_spikes(
    rows: list[dict[str, Any]],
    *,
    max_range_mult: float = 6.0,
    max_dev_frac: float = 0.004,
) -> list[dict[str, Any]]:
    """Limita pavios absurdos vs mediana do lote (forex OTC 1m).

    Nao descarta a vela: clampa high/low ao redor de open/close.
    Velas com close/open totalmente fora da banda do lote sao removidas.
    """
    if len(rows) < 8:
        return rows
    closes: list[float] = []
    ranges: list[float] = []
    parsed: list[tuple[dict[str, Any], float, float, float, float]] = []
    for row in rows:
        try:
            o = float(row["open"])
            h = float(row["high"])
            lo = float(row["low"])
            c = float(row["close"])
        except (TypeError, ValueError, KeyError):
            continue
        h = max(h, o, c)
        lo = min(lo, o, c)
        parsed.append((row, o, h, lo, c))
        closes.append(c)
        ranges.append(h - lo)
    if len(parsed) < 8:
        return rows
    closes_sorted = sorted(closes)
    mid = closes_sorted[len(closes_sorted) // 2]
    if not (mid > 0):
        return rows
    ranges_sorted = sorted(ranges)
    med_range = ranges_sorted[len(ranges_sorted) // 2]
    band = max(med_range * max_range_mult, mid * 0.00025)  # >= ~2.5 pips
    out: list[dict[str, Any]] = []
    for row, o, h, lo, c in parsed:
        if abs(c - mid) / mid > max_dev_frac and abs(o - mid) / mid > max_dev_frac:
            continue
        body_hi = max(o, c)
        body_lo = min(o, c)
        nh = min(h, body_hi + band)
        nl = max(lo, body_lo - band)
        nh = max(nh, body_hi)
        nl = min(nl, body_lo)
        cleaned = dict(row)
        cleaned["open"] = o
        cleaned["high"] = nh
        cleaned["low"] = nl
        cleaned["close"] = c
        out.append(cleaned)
    return out


def merge_ohlc_with_existing(
    rows: list[dict[str, Any]],
    *,
    asset: str,
    timeframe: str,
    table: str = TABLE,
    lookback: int = 180,
) -> list[dict[str, Any]]:
    """Mescla com OHLC ja salvo: nao deixa tick incompleto (flat) apagar pavio bom.

    - Mantem open da primeira gravacao do minuto
    - high = max, low = min
    - close = mais recente
    - Nao insere vela ja fechada e flat se ainda nao existe no DB
    """
    if not rows:
        return []
    try:
        recent = fetch_candles(
            asset, timeframe=timeframe, limit=lookback, table=table
        )
    except Exception:  # noqa: BLE001
        recent = []
    by_key: dict[str, dict[str, Any]] = {}
    for r in recent:
        k = _opened_key(r.get("opened_at"))
        if k:
            by_key[k] = r

    now = datetime.now(timezone.utc)
    out: list[dict[str, Any]] = []
    for row in rows:
        key = _opened_key(row.get("opened_at"))
        if not key:
            out.append(row)
            continue
        try:
            o = float(row["open"])
            h = float(row["high"])
            lo = float(row["low"])
            c = float(row["close"])
        except (TypeError, ValueError, KeyError):
            out.append(row)
            continue
        h = max(h, o, c)
        lo = min(lo, o, c)
        old = by_key.get(key)
        if old is None:
            # Evita gravar "traco" (O=H=L=C) de minuto ja fechado — API incompleta.
            try:
                ts = datetime.fromisoformat(key).replace(tzinfo=timezone.utc)
            except ValueError:
                ts = None
            age = (now - ts).total_seconds() if ts else 0
            if age > 90 and _is_flat_ohlc(o, h, lo, c):
                continue
            merged = dict(row)
            merged["high"] = h
            merged["low"] = lo
            out.append(merged)
            continue
        try:
            oo = float(old["open"])
            oh = float(old["high"])
            ol = float(old["low"])
        except (TypeError, ValueError, KeyError):
            merged = dict(row)
            merged["high"] = h
            merged["low"] = lo
            out.append(merged)
            continue
        merged = dict(row)
        merged["open"] = oo
        merged["high"] = max(oh, h, oo, c)
        merged["low"] = min(ol, lo, oo, c)
        merged["close"] = c
        out.append(merged)
    return out


def count_candles(
    asset: str, timeframe: str = "1h", *, table: str = TABLE
) -> int:
    """Quantidade de velas salvas no Supabase para asset+tf."""
    ok, msg = supabase_ok()
    if not ok:
        raise RuntimeError(msg)
    sb = cliente_supabase()
    assert sb is not None
    res = (
        sb.table(table)
        .select("id", count="exact")
        .eq("asset", asset)
        .eq("timeframe", timeframe)
        .limit(1)
        .execute()
    )
    count = getattr(res, "count", None)
    if count is not None:
        return int(count)
    data = getattr(res, "data", None) or []
    return len(data)


def last_opened_at(
    asset: str, timeframe: str = "1h", *, table: str = TABLE
) -> datetime | None:
    """Maior opened_at salvo (UTC) ou None se vazio."""
    ok, msg = supabase_ok()
    if not ok:
        raise RuntimeError(msg)
    sb = cliente_supabase()
    assert sb is not None
    res = (
        sb.table(table)
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
    return _parse_opened_at(data[0].get("opened_at"))


def delete_candles_by_source(
    asset: str,
    *,
    timeframe: str = "1h",
    source: str,
    table: str = TABLE,
    since: datetime | None = None,
) -> int:
    """Apaga velas de um source (ex.: limpar Pocket residual na tabela EURUSD)."""
    ok, msg = supabase_ok()
    if not ok:
        raise RuntimeError(msg)
    sb = cliente_supabase()
    assert sb is not None
    q = (
        sb.table(table)
        .delete()
        .eq("asset", asset)
        .eq("timeframe", timeframe)
        .eq("source", source)
    )
    if since is not None:
        q = q.gte("opened_at", since.isoformat())
    res = q.execute()
    data = getattr(res, "data", None) or []
    return len(data)

def oldest_opened_at(
    asset: str, timeframe: str = "1h", *, table: str = TABLE
) -> datetime | None:
    """Menor opened_at salvo (UTC) ou None se vazio."""
    ok, msg = supabase_ok()
    if not ok:
        raise RuntimeError(msg)
    sb = cliente_supabase()
    assert sb is not None
    res = (
        sb.table(table)
        .select("opened_at")
        .eq("asset", asset)
        .eq("timeframe", timeframe)
        .order("opened_at", desc=False)
        .limit(1)
        .execute()
    )
    data = getattr(res, "data", None) or []
    if not data:
        return None
    return _parse_opened_at(data[0].get("opened_at"))


def fetch_candles(
    asset: str,
    *,
    timeframe: str = "1h",
    limit: int = 200,
    table: str = TABLE,
) -> list[dict[str, Any]]:
    """Candles para o grafico (ordem cronologica crescente).

    limit <= 0: traz todo o historico do asset+tf (paginado).
    limit > 0: as N velas mais recentes (teto 5000).
    """
    if int(limit) <= 0:
        return fetch_candles_range(asset, timeframe=timeframe, table=table)
    ok, msg = supabase_ok()
    if not ok:
        raise RuntimeError(msg)
    sb = cliente_supabase()
    assert sb is not None
    lim = max(1, min(int(limit), 5000))
    res = (
        sb.table(table)
        .select("opened_at,open,high,low,close,volume,source")
        .eq("asset", asset)
        .eq("timeframe", timeframe)
        .order("opened_at", desc=True)
        .limit(lim)
        .execute()
    )
    data = list(getattr(res, "data", None) or [])
    data.reverse()
    return data


def count_before(
    asset: str,
    before: datetime,
    *,
    timeframe: str = "1h",
    table: str = TABLE,
) -> int:
    ok, msg = supabase_ok()
    if not ok:
        raise RuntimeError(msg)
    sb = cliente_supabase()
    assert sb is not None
    res = (
        sb.table(table)
        .select("id", count="exact")
        .eq("asset", asset)
        .eq("timeframe", timeframe)
        .lt("opened_at", before.isoformat())
        .limit(1)
        .execute()
    )
    count = getattr(res, "count", None)
    if count is not None:
        return int(count)
    return 0


def delete_candles_before(
    asset: str,
    before: datetime,
    *,
    timeframe: str = "1h",
    table: str = TABLE,
) -> int:
    """Apaga velas com opened_at < before. Retorna estimativa via count previo."""
    ok, msg = supabase_ok()
    if not ok:
        raise RuntimeError(msg)
    n = count_before(asset, before, timeframe=timeframe, table=table)
    if n <= 0:
        return 0
    sb = cliente_supabase()
    assert sb is not None
    (
        sb.table(table)
        .delete()
        .eq("asset", asset)
        .eq("timeframe", timeframe)
        .lt("opened_at", before.isoformat())
        .execute()
    )
    return n


def count_since(
    asset: str,
    since: datetime,
    *,
    timeframe: str = "1h",
    table: str = TABLE,
) -> int:
    ok, msg = supabase_ok()
    if not ok:
        raise RuntimeError(msg)
    sb = cliente_supabase()
    assert sb is not None
    res = (
        sb.table(table)
        .select("id", count="exact")
        .eq("asset", asset)
        .eq("timeframe", timeframe)
        .gte("opened_at", since.isoformat())
        .limit(1)
        .execute()
    )
    count = getattr(res, "count", None)
    if count is not None:
        return int(count)
    return 0


def delete_candles_since(
    asset: str,
    since: datetime,
    *,
    timeframe: str = "1h",
    table: str = TABLE,
) -> int:
    """Apaga velas com opened_at >= since. Retorna estimativa via count previo."""
    ok, msg = supabase_ok()
    if not ok:
        raise RuntimeError(msg)
    n = count_since(asset, since, timeframe=timeframe, table=table)
    if n <= 0:
        return 0
    sb = cliente_supabase()
    assert sb is not None
    (
        sb.table(table)
        .delete()
        .eq("asset", asset)
        .eq("timeframe", timeframe)
        .gte("opened_at", since.isoformat())
        .execute()
    )
    return n


def fetch_candles_range(
    asset: str,
    *,
    timeframe: str = "1h",
    table: str = TABLE,
    before: datetime | None = None,
    after: datetime | None = None,
    max_rows: int = 200_000,
) -> list[dict[str, Any]]:
    """Exporta candles (paginado) em ordem crescente."""
    ok, msg = supabase_ok()
    if not ok:
        raise RuntimeError(msg)
    sb = cliente_supabase()
    assert sb is not None
    out: list[dict[str, Any]] = []
    offset = 0
    while len(out) < max_rows:
        q = (
            sb.table(table)
            .select("opened_at,open,high,low,close,volume,timeframe,asset,source")
            .eq("asset", asset)
            .eq("timeframe", timeframe)
            .order("opened_at", desc=False)
            .range(offset, offset + FETCH_PAGE - 1)
        )
        if before is not None:
            q = q.lt("opened_at", before.isoformat())
        if after is not None:
            q = q.gte("opened_at", after.isoformat())
        res = q.execute()
        chunk = list(getattr(res, "data", None) or [])
        if not chunk:
            break
        out.extend(chunk)
        if len(chunk) < FETCH_PAGE:
            break
        offset += FETCH_PAGE
    return out[:max_rows]


def candles_to_csv(rows: list[dict[str, Any]]) -> str:
    buf = io.StringIO()
    writer = csv.DictWriter(
        buf,
        fieldnames=[
            "asset",
            "timeframe",
            "opened_at",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "source",
        ],
        extrasaction="ignore",
    )
    writer.writeheader()
    for r in rows:
        writer.writerow(r)
    return buf.getvalue()


def retention_status_1m(
    asset: str,
    *,
    retention_days: int = RETENTION_DAYS_1M,
    warn_before_days: int = WARN_BEFORE_DAYS_1M,
) -> dict[str, Any]:
    """Estado da limpeza 1m (aviso 1 dia antes / delete apos 90 dias)."""
    now = datetime.now(timezone.utc)
    delete_before = now - timedelta(days=retention_days)
    warn_before = now - timedelta(days=max(retention_days - warn_before_days, 0))
    try:
        oldest = oldest_opened_at(asset, "1m", table=TABLE_1M)
        rows_delete = count_before(
            asset, delete_before, timeframe="1m", table=TABLE_1M
        )
        rows_at_risk = count_before(
            asset, warn_before, timeframe="1m", table=TABLE_1M
        )
    except Exception as exc:  # noqa: BLE001
        return {
            "retention_days": retention_days,
            "warn_before_days": warn_before_days,
            "oldest": None,
            "next_delete_at": None,
            "delete_before": delete_before.isoformat(),
            "warn": False,
            "rows_to_delete": 0,
            "rows_at_risk": 0,
            "err": str(exc)[:200],
        }
    next_delete_at = None
    if oldest is not None:
        next_delete_at = oldest + timedelta(days=retention_days)
    warn = rows_at_risk > 0 or (
        next_delete_at is not None
        and (next_delete_at - now) <= timedelta(days=warn_before_days)
        and (next_delete_at - now).total_seconds() > 0
    )
    return {
        "retention_days": retention_days,
        "warn_before_days": warn_before_days,
        "oldest": oldest.isoformat() if oldest else None,
        "next_delete_at": next_delete_at.isoformat() if next_delete_at else None,
        "delete_before": delete_before.isoformat(),
        "warn": warn,
        "rows_to_delete": rows_delete,
        "rows_at_risk": rows_at_risk,
        "err": None,
    }


def run_retention_cleanup_1m(asset: str) -> dict[str, Any]:
    """Apaga candles 1m com mais de RETENTION_DAYS_1M."""
    before = datetime.now(timezone.utc) - timedelta(days=RETENTION_DAYS_1M)
    deleted = delete_candles_before(
        asset, before, timeframe="1m", table=TABLE_1M
    )
    return {
        "deleted": deleted,
        "before": before.isoformat(),
        "retention": retention_status_1m(asset),
    }


def stored_summary(
    asset: str, timeframe: str = "1h", *, table: str = TABLE
) -> dict[str, Any]:
    """Resumo do que ja esta no banco (para a UI)."""
    try:
        n = count_candles(asset, timeframe, table=table)
        last = last_opened_at(asset, timeframe, table=table)
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
