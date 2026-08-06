"""API + portal web do IQ Blitz Bot."""

from __future__ import annotations

import io
import os
import secrets
import zipfile
from pathlib import Path

from fastapi import Depends, FastAPI, File, Header, HTTPException, UploadFile
from fastapi.responses import FileResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from bot.eurusd_daily_import import SOURCE_LABEL as EURUSD_IMPORT_SOURCE
from bot.eurusd_daily_import import parse_eurusd_daily_bytes
from bot.ohlc_collector import collector
from bot.ohlc_collector_1d import collector_1d, TABLE_1D
from bot.ohlc_collector_eurusd import TABLE_EURUSD, collector_eurusd
from bot.ohlc_collector_olymp import TABLE as TABLE_OLYMP, collector_olymp
from bot.ohlc_spread import (
    build_spread_1d,
    build_spread_1h,
    detect_eurusd_opens,
    pad_eurusd_tail_to_otc,
)
from bot.ohlc_spread_1d_sync import (
    backfill_dukascopy_1d,
    sync_spread_1d_sources,
)
from bot.ohlc_spread_olymp_1d_sync import (
    backfill_dukascopy_olymp_1d,
    backfill_olymp_otc_1d,
    sync_spread_olymp_1d_sources,
)
from bot.ohlc_spread_expert_1d_sync import (
    backfill_dukascopy_expert_1d,
    backfill_expert_otc_1d,
    sync_spread_expert_1d_sources,
)
from bot.ohlc_collector_expert import TABLE as TABLE_EXPERT, collector_expert
from bot.expertoption_fetch import default_store_asset as expert_default_otc_asset
from bot.expertoption_fetch import expertoption_available
from bot.ohlc_spread_olymp_sync import sync_spread_olymp_sources
from bot.ohlc_spread_sync import sync_spread_sources
from bot.ohlc_store import (
    candles_to_csv,
    fetch_candles,
    fetch_candles_range,
    stored_summary,
    upsert_candles,
)

try:
    from bot.ohlc_store import TABLE_EURUSD_1D, TABLE_OLYMP_1D, TABLE_EXPERT_1D
except ImportError:  # pragma: no cover
    TABLE_EURUSD_1D = "ohlc_candles_eurusd_1d"
    TABLE_OLYMP_1D = "ohlc_candles_olymp_1d"
    TABLE_EXPERT_1D = "ohlc_candles_expert_1d"
from bot.olymptrade_fetch import default_store_asset as olymp_default_otc_asset
from bot.olymptrade_fetch import olymptrade_available
from bot.runner import MIN_DURATION, normalize_asset, runner
from bot.spread_ou import analyze_spread_ou, evaluate_paper_signal

STATIC = Path(__file__).resolve().parent / "static"

app = FastAPI(title="IQ Blitz Bot — Portal", version="1.0.0")
app.mount("/static", StaticFiles(directory=str(STATIC)), name="static")


def _html_page(name: str) -> FileResponse:
    """Serve HTML sem cache (evita botao/texto antigo apos deploy no Railway)."""
    return FileResponse(
        STATIC / name,
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "Pragma": "no-cache",
            "Expires": "0",
        },
    )


class DurationBody(BaseModel):
    """Duracao da 1a ordem: seconds OU minutes (um dos dois)."""

    seconds: int | None = Field(default=None, ge=MIN_DURATION)
    minutes: float | None = Field(default=None, gt=0)


class OhlcAssetBody(BaseModel):
    asset: str = Field(min_length=1, max_length=64)


class OhlcStartBody(BaseModel):
    asset: str | None = Field(default=None, min_length=1, max_length=64)


class OhlcResyncDaysBody(BaseModel):
    days: int = Field(default=30, ge=1, le=365)


class OhlcSpreadSyncBody(BaseModel):
    """Dias de historico Dukascopy (OTC usa backfill Pocket incremental)."""

    days: int = Field(default=14, ge=1, le=90)


class OhlcSpreadOtcHistoryBody(BaseModel):
    """Backfill profundo OTC Pocket (paginado ate ~2 anos)."""

    days: int = Field(default=600, ge=7, le=800)
    asset: str | None = Field(default=None, min_length=1, max_length=64)


class OhlcSpreadDukaHistoryBody(BaseModel):
    """Backfill Dukascopy alinhado ao OTC mais antigo (match_otc=True na UI)."""

    days: int = Field(default=14, ge=1, le=800)
    match_otc: bool = True
    otc_asset: str | None = Field(default=None, min_length=1, max_length=64)


def _control_token() -> str:
    return os.environ.get("CONTROL_TOKEN", "").strip()


def require_token(
    x_control_token: str | None = Header(default=None, alias="X-Control-Token"),
) -> None:
    expected = _control_token()
    if not expected:
        raise HTTPException(
            status_code=503,
            detail="Defina CONTROL_TOKEN nas variaveis do Railway.",
        )
    if not x_control_token or not secrets.compare_digest(
        x_control_token, expected
    ):
        raise HTTPException(status_code=401, detail="Token invalido.")


@app.get("/")
def portal_page() -> FileResponse:
    return _html_page("index.html")


@app.get("/bot")
def bot_page() -> FileResponse:
    return _html_page("bot.html")


@app.get("/ohlc")
def ohlc_page() -> FileResponse:
    return _html_page("ohlc.html")


@app.get("/ohlc-1d")
def ohlc_1d_page() -> FileResponse:
    return _html_page("ohlc_1d.html")


@app.get("/ohlc-spread")
def ohlc_spread_page() -> FileResponse:
    return _html_page("ohlc_spread.html")


@app.get("/ohlc-spread-olymp")
def ohlc_spread_olymp_page() -> FileResponse:
    return _html_page("ohlc_spread_olymp.html")


@app.get("/ohlc-spread-1d")
def ohlc_spread_1d_page() -> FileResponse:
    return _html_page("ohlc_spread_1d.html")


@app.get("/ohlc-spread-olymp-1d")
def ohlc_spread_olymp_1d_page() -> FileResponse:
    return _html_page("ohlc_spread_olymp_1d.html")


@app.get("/ohlc-spread-expert-1d")
def ohlc_spread_expert_1d_page() -> FileResponse:
    return _html_page("ohlc_spread_expert_1d.html")


@app.get("/ohlc-1m")
def ohlc_1m_redirect() -> RedirectResponse:
    """Ferramenta 1m substituida pelo coletor diario."""
    return RedirectResponse(url="/ohlc-1d", status_code=302)


@app.get("/api/health")
def health() -> dict:
    st = collector.status()
    st1 = collector_1d.status()
    return {
        "ok": True,
        "bot_running": runner.is_running(),
        "ohlc_running": collector.is_running(),
        "ohlc_1d_running": collector_1d.is_running(),
        "ohlc_eurusd_running": collector_eurusd.is_running(),
        "ohlc_olymp_running": collector_olymp.is_running(),
        "supabase_ok": bool(st.get("supabase_ok")),
        "token_configured": bool(_control_token()),
        "duration_seconds": runner.get_duration_seconds(),
    }


@app.get("/api/bot/status")
def bot_status(_: None = Depends(require_token)) -> dict:
    return runner.status()


@app.get("/api/bot/pnl")
def bot_pnl(_: None = Depends(require_token)) -> dict:
    return {"points": runner.pnl_series()}


@app.post("/api/bot/start")
def bot_start(_: None = Depends(require_token)) -> dict:
    return runner.start()


@app.post("/api/bot/stop")
def bot_stop(_: None = Depends(require_token)) -> dict:
    return runner.stop()


@app.post("/api/bot/duration")
def bot_duration(
    body: DurationBody, _: None = Depends(require_token)
) -> dict:
    if body.seconds is not None:
        sec = int(body.seconds)
    elif body.minutes is not None:
        sec = int(round(float(body.minutes) * 60))
    else:
        raise HTTPException(
            status_code=400,
            detail="Informe seconds ou minutes.",
        )
    return runner.set_duration_seconds(sec)


@app.get("/api/ohlc/status")
def ohlc_status(_: None = Depends(require_token)) -> dict:
    return collector.status()


@app.post("/api/ohlc/asset")
def ohlc_asset(
    body: OhlcAssetBody, _: None = Depends(require_token)
) -> dict:
    try:
        return collector.set_asset(body.asset)
    except (ValueError, RuntimeError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/ohlc/start")
def ohlc_start(
    body: OhlcStartBody = OhlcStartBody(),
    _: None = Depends(require_token),
) -> dict:
    try:
        return collector.start(body.asset)
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/ohlc/stop")
def ohlc_stop(_: None = Depends(require_token)) -> dict:
    return collector.stop()


@app.post("/api/ohlc/backfill-history")
def ohlc_backfill_history(
    body: OhlcSpreadOtcHistoryBody = OhlcSpreadOtcHistoryBody(),
    _: None = Depends(require_token),
) -> dict:
    """Puxa historico profundo do ativo (paginado, inclui 2024 se a Pocket tiver)."""
    asset = normalize_asset(
        body.asset or collector.status().get("asset") or "EURUSD_otc"
    )
    try:
        pull = collector.pull_history(asset, days=body.days)
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    st = collector.status()
    st["backfill"] = pull.get("pull")
    return st


@app.get("/api/ohlc/candles")
def ohlc_candles(
    asset: str | None = None,
    timeframe: str = "1h",
    limit: int = 0,
    _: None = Depends(require_token),
) -> dict:
    """Candles salvos no Supabase (para o grafico da ferramenta).

    limit=0 (padrao): todas as velas do asset no DB.
    """
    if timeframe != "1h":
        raise HTTPException(status_code=400, detail="Timeframe suportado: 1h")
    a = normalize_asset(asset or collector.status().get("asset") or "EURUSD_otc")
    try:
        rows = fetch_candles(a, timeframe=timeframe, limit=limit)
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {
        "asset": a,
        "timeframe": timeframe,
        "count": len(rows),
        "candles": rows,
        "scope": "all" if limit <= 0 else "recent",
    }


# --- OHLC diario (D1) ---


@app.get("/api/ohlc1d/status")
def ohlc1d_status(_: None = Depends(require_token)) -> dict:
    return collector_1d.status()


@app.post("/api/ohlc1d/asset")
def ohlc1d_asset(
    body: OhlcAssetBody, _: None = Depends(require_token)
) -> dict:
    try:
        return collector_1d.set_asset(body.asset)
    except (ValueError, RuntimeError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/ohlc1d/start")
def ohlc1d_start(
    body: OhlcStartBody = OhlcStartBody(),
    _: None = Depends(require_token),
) -> dict:
    try:
        return collector_1d.start(body.asset)
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/ohlc1d/stop")
def ohlc1d_stop(_: None = Depends(require_token)) -> dict:
    return collector_1d.stop()


@app.get("/api/ohlc1d/candles")
def ohlc1d_candles(
    asset: str | None = None,
    timeframe: str = "1d",
    limit: int = 0,
    _: None = Depends(require_token),
) -> dict:
    """Candles D1 do Supabase. limit=0: historico completo."""
    if timeframe != "1d":
        raise HTTPException(status_code=400, detail="Timeframe suportado: 1d")
    a = normalize_asset(
        asset or collector_1d.status().get("asset") or "EURUSD_otc"
    )
    try:
        rows = fetch_candles(
            a, timeframe=timeframe, limit=limit, table=TABLE_1D
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {
        "asset": a,
        "timeframe": timeframe,
        "count": len(rows),
        "candles": rows,
        "scope": "all" if limit <= 0 else "recent",
    }


@app.get("/api/ohlc1d/export")
def ohlc1d_export(
    asset: str | None = None,
    _: None = Depends(require_token),
) -> Response:
    """Download CSV com todo o historico D1 salvo."""
    st = collector_1d.status()
    a = normalize_asset(asset or st.get("asset") or "EURUSD_otc")
    try:
        rows = fetch_candles_range(a, timeframe="1d", table=TABLE_1D)
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    csv_text = candles_to_csv(rows)
    fname = f"ohlc_1d_{a}.csv"
    return Response(
        content=csv_text,
        media_type="text/csv; charset=utf-8",
        headers={
            "Content-Disposition": f'attachment; filename="{fname}"',
        },
    )


@app.post("/api/ohlc1d/pull-now")
def ohlc1d_pull_now(_: None = Depends(require_token)) -> dict:
    """Puxada manual: D1 nativo Pocket + upsert."""
    try:
        return collector_1d.pull_now()
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/api/ohlc1d/resync-recent")
def ohlc1d_resync_recent(
    body: OhlcResyncDaysBody = OhlcResyncDaysBody(),
    _: None = Depends(require_token),
) -> dict:
    """Apaga os ultimos N dias no Supabase e repuxa da Pocket."""
    try:
        return collector_1d.resync_recent(body.days)
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# --- Compat: rotas antigas /api/ohlc1m/* → D1 ---


@app.get("/api/ohlc1m/status")
def ohlc1m_status_compat(_: None = Depends(require_token)) -> dict:
    return collector_1d.status()


@app.post("/api/ohlc1m/pull-now")
def ohlc1m_pull_now_compat(_: None = Depends(require_token)) -> dict:
    return ohlc1d_pull_now(_)


@app.post("/api/ohlc1m/start")
def ohlc1m_start_compat(
    body: OhlcStartBody = OhlcStartBody(),
    _: None = Depends(require_token),
) -> dict:
    return ohlc1d_start(body, _)


@app.post("/api/ohlc1m/stop")
def ohlc1m_stop_compat(_: None = Depends(require_token)) -> dict:
    return ohlc1d_stop(_)


# --- Spread OTC vs EURUSD (1h) ---


@app.get("/api/ohlc-spread/status")
def ohlc_spread_status(_: None = Depends(require_token)) -> dict:
    otc_asset = normalize_asset(
        os.environ.get("OHLC_SPREAD_OTC_ASSET", "").strip()
        or collector.status().get("asset")
        or "EURUSD_otc"
    )
    try:
        eu_st = collector_eurusd.status()
    except Exception as exc:  # noqa: BLE001
        eu_st = {
            "running": collector_eurusd.is_running(),
            "asset": "EURUSD",
            "phase": "error",
            "message": str(exc)[:200],
            "stored_count": None,
            "stored_last": None,
            "stored_err": str(exc)[:200],
            "supabase_ok": False,
        }
    try:
        otc_sum = stored_summary(otc_asset, "1h")
    except Exception as exc:  # noqa: BLE001
        otc_sum = {
            "stored_count": None,
            "stored_last": None,
            "stored_err": str(exc)[:200],
        }
    return {
        "otc": {
            "asset": otc_asset,
            "collector_running": collector.is_running(),
            "table": "ohlc_candles",
            **otc_sum,
        },
        "eurusd": eu_st,
        "timeframe": "1h",
        "supabase_ok": eu_st.get("supabase_ok"),
        "supabase_msg": eu_st.get("supabase_msg"),
    }


@app.post("/api/ohlc-spread/start")
def ohlc_spread_start(
    body: OhlcStartBody = OhlcStartBody(),
    _: None = Depends(require_token),
) -> dict:
    """Inicia coletor EURUSD via Dukascopy (1h). OTC continua no /ohlc."""
    try:
        collector_eurusd.start(body.asset or "EURUSD")
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return ohlc_spread_status(_)


@app.post("/api/ohlc-spread/stop")
def ohlc_spread_stop(_: None = Depends(require_token)) -> dict:
    collector_eurusd.stop()
    return ohlc_spread_status(_)


@app.post("/api/ohlc-spread/pull-now")
def ohlc_spread_pull_now(
    body: OhlcSpreadSyncBody = OhlcSpreadSyncBody(),
    _: None = Depends(require_token),
) -> dict:
    """Puxa EURUSD_otc (Pocket) + EURUSD (Dukascopy) e grava no Supabase."""
    try:
        sync = sync_spread_sources(days=body.days)
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    st = ohlc_spread_status(_)
    st["sync"] = sync
    st["pull"] = {
        "otc_upserted": (sync.get("otc") or {}).get("upserted"),
        "eurusd_upserted": (sync.get("eurusd") or {}).get("upserted"),
        "source_eurusd": "dukascopy",
    }
    eu = sync.get("eurusd") or {}
    if not eu.get("ok"):
        detail = eu.get("error") or "Falha ao sincronizar Dukascopy EURUSD"
        raise HTTPException(status_code=502, detail=detail)
    return st


@app.post("/api/ohlc-spread/backfill-otc")
def ohlc_spread_backfill_otc(
    body: OhlcSpreadOtcHistoryBody = OhlcSpreadOtcHistoryBody(),
    _: None = Depends(require_token),
) -> dict:
    """Puxa historico profundo EURUSD_otc da Pocket (inclui dias antes do 1o salvo)."""
    otc_a = normalize_asset(
        body.asset
        or os.environ.get("OHLC_SPREAD_OTC_ASSET", "").strip()
        or collector.status().get("asset")
        or "EURUSD_otc"
    )
    try:
        pull = collector.pull_history(otc_a, days=body.days)
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    st = ohlc_spread_status(_)
    st["backfill_otc"] = pull.get("pull")
    return st


@app.post("/api/ohlc-spread/backfill-dukascopy")
def ohlc_spread_backfill_dukascopy(
    body: OhlcSpreadDukaHistoryBody = OhlcSpreadDukaHistoryBody(),
    _: None = Depends(require_token),
) -> dict:
    """Puxa Dukascopy desde o candle OTC Pocket mais antigo ate agora."""
    otc_a = normalize_asset(
        body.otc_asset
        or os.environ.get("OHLC_SPREAD_OTC_ASSET", "").strip()
        or "EURUSD_otc"
    )
    try:
        pull = collector_eurusd.pull_history(
            days=body.days,
            match_otc=body.match_otc,
            otc_asset=otc_a,
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    st = ohlc_spread_status(_)
    st["backfill_dukascopy"] = pull.get("pull")
    return st


@app.get("/api/ohlc-spread/export")
def ohlc_spread_export(
    days: int = 14,
    sync: int = 1,
    _: None = Depends(require_token),
) -> Response:
    """Baixa ZIP com CSVs. Por padrao sincroniza Pocket OTC + Dukascopy antes."""
    lookback = max(1, min(int(days), 90))
    sync_info: dict | None = None
    if sync:
        try:
            sync_info = sync_spread_sources(days=lookback)
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(status_code=500, detail=str(exc)) from exc
        if not sync_info.get("ok"):
            otc_err = (sync_info.get("otc") or {}).get("error")
            eu_err = (sync_info.get("eurusd") or {}).get("error")
            detail = "; ".join(x for x in (otc_err, eu_err) if x) or "Sync falhou"
            raise HTTPException(status_code=502, detail=detail)

    otc_a = normalize_asset(
        os.environ.get("OHLC_SPREAD_OTC_ASSET", "").strip()
        or collector.status().get("asset")
        or "EURUSD_otc"
    )
    eu_a = normalize_asset(
        os.environ.get("OHLC_EURUSD_ASSET", "").strip()
        or collector_eurusd.status().get("asset")
        or "EURUSD"
    )
    try:
        otc_rows = fetch_candles(otc_a, timeframe="1h", limit=0)
        eu_rows = fetch_candles(
            eu_a, timeframe="1h", limit=0, table=TABLE_EURUSD
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(f"ohlc_1h_{otc_a}.csv", candles_to_csv(otc_rows))
        zf.writestr(f"ohlc_1h_{eu_a}_dukascopy.csv", candles_to_csv(eu_rows))
        if sync_info is not None:
            import json

            zf.writestr(
                "sync_meta.json",
                json.dumps(sync_info, ensure_ascii=False, indent=2),
            )
    data = buf.getvalue()
    fname = f"ohlc_spread_{otc_a}_{eu_a}.zip"
    return Response(
        content=data,
        media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="{fname}"'},
    )


@app.get("/api/ohlc-spread/series")
def ohlc_spread_series(
    otc_asset: str | None = None,
    eurusd_asset: str | None = None,
    limit: int = 0,
    _: None = Depends(require_token),
) -> dict:
    """Candles OTC + EURUSD + serie de spread (carry apos fechamento EURUSD)."""
    try:
        payload = _load_spread_bundle(otc_asset, eurusd_asset, limit)
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    weekend_n = sum(1 for p in payload["spread"] if p.get("weekend"))
    after_hours_n = sum(1 for p in payload["spread"] if p.get("after_hours"))
    return {
        "timeframe": "1h",
        "otc_asset": payload["otc_asset"],
        "eurusd_asset": payload["eurusd_asset"],
        "eurusd_source": payload["eurusd_source"],
        "otc_count": len(payload["otc"]),
        "eurusd_count": len(payload["eurusd"]),
        "spread_count": len(payload["spread"]),
        "weekend_points": weekend_n,
        "after_hours_points": after_hours_n,
        "eurusd_opens": payload["eurusd_opens"],
        "otc": payload["otc"],
        "eurusd": payload["eurusd"],
        "spread": payload["spread"],
    }


@app.get("/api/ohlc-spread/ou")
def ohlc_spread_ou(
    otc_asset: str | None = None,
    eurusd_asset: str | None = None,
    paired_only: bool = True,
    min_n: int = 48,
    limit: int = 0,
    payout: float = 0.92,
    z_min: float = 1.5,
    edge_margin: float = 0.03,
    _: None = Depends(require_token),
) -> dict:
    """Fase 1+2: OU no spread + sinal paper GO/SKIP (binaria <=4h)."""
    try:
        payload = _load_spread_bundle(otc_asset, eurusd_asset, limit)
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    analysis = analyze_spread_ou(
        payload["spread"],
        paired_only=paired_only,
        min_n=max(20, min(min_n, 5000)),
    )
    signal = evaluate_paper_signal(
        analysis,
        payout=payout,
        z_min=z_min,
        edge_margin=edge_margin,
    )
    return {
        "timeframe": "1h",
        "otc_asset": payload["otc_asset"],
        "eurusd_asset": payload["eurusd_asset"],
        "eurusd_source": payload["eurusd_source"],
        "spread_count": len(payload["spread"]),
        "ou": analysis,
        "signal": signal,
    }


def _prefer_eurusd_rows(eu_rows: list) -> tuple[list, str]:
    dukas = [
        r for r in eu_rows if str(r.get("source") or "").lower() == "dukascopy"
    ]
    unknown = [r for r in eu_rows if not str(r.get("source") or "").strip()]
    pocket_eu = [
        r for r in eu_rows if str(r.get("source") or "").lower() == "pocket"
    ]
    if dukas:
        return dukas, "dukascopy"
    if unknown and not pocket_eu:
        return unknown, "legacy"
    if pocket_eu:
        return pocket_eu, "pocket"
    return eu_rows, "unknown"


def _load_spread_bundle(
    otc_asset: str | None,
    eurusd_asset: str | None,
    limit: int,
    *,
    default_otc: str | None = None,
    otc_table: str | None = None,
) -> dict:
    otc_a = normalize_asset(
        otc_asset
        or default_otc
        or os.environ.get("OHLC_SPREAD_OTC_ASSET", "").strip()
        or collector.status().get("asset")
        or "EURUSD_otc"
    )
    eu_a = normalize_asset(
        eurusd_asset
        or collector_eurusd.status().get("asset")
        or "EURUSD"
    )
    otc_rows = fetch_candles(
        otc_a, timeframe="1h", limit=limit, table=otc_table or "ohlc_candles"
    )
    eu_rows = fetch_candles(
        eu_a, timeframe="1h", limit=limit, table=TABLE_EURUSD
    )
    eu_rows, eu_source = _prefer_eurusd_rows(eu_rows)
    # Alinha cauda: OTC costuma ter a hora corrente antes do bi5 Dukascopy.
    eu_rows = pad_eurusd_tail_to_otc(otc_rows, eu_rows, max_hours=6)
    try:
        spread = build_spread_1h(otc_rows, eu_rows)
        opens = detect_eurusd_opens(eu_rows)
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"Falha ao montar spread: {exc}") from exc
    return {
        "otc_asset": otc_a,
        "eurusd_asset": eu_a,
        "eurusd_source": eu_source,
        "otc": otc_rows,
        "eurusd": eu_rows,
        "spread": spread,
        "eurusd_opens": opens,
    }


# --- Spread Olymp OTC vs EURUSD (1h) ---


def _olymp_otc_asset(override: str | None = None) -> str:
    return normalize_asset(
        override
        or os.environ.get("OHLC_OLYMP_OTC_ASSET", "").strip()
        or collector_olymp.status().get("asset")
        or olymp_default_otc_asset()
    )


@app.get("/api/ohlc-spread-olymp/status")
def ohlc_spread_olymp_status(_: None = Depends(require_token)) -> dict:
    otc_asset = _olymp_otc_asset()
    try:
        eu_st = collector_eurusd.status()
    except Exception as exc:  # noqa: BLE001
        eu_st = {
            "running": collector_eurusd.is_running(),
            "asset": "EURUSD",
            "phase": "error",
            "message": str(exc)[:200],
            "stored_count": None,
            "stored_last": None,
            "stored_err": str(exc)[:200],
            "supabase_ok": False,
        }
    try:
        olymp_st = collector_olymp.status()
    except Exception as exc:  # noqa: BLE001
        olymp_st = {
            "running": False,
            "asset": otc_asset,
            "phase": "error",
            "message": str(exc)[:200],
        }
    try:
        otc_sum = stored_summary(otc_asset, "1h", table=TABLE_OLYMP)
    except Exception as exc:  # noqa: BLE001
        otc_sum = {
            "stored_count": None,
            "stored_last": None,
            "stored_err": str(exc)[:200],
        }
    o_ok, o_msg = olymptrade_available()
    return {
        "otc": {
            "asset": otc_asset,
            "pair": olymp_st.get("pair"),
            "collector_running": collector_olymp.is_running(),
            "table": TABLE_OLYMP,
            "source": "olymptrade",
            **otc_sum,
        },
        "olymp": olymp_st,
        "olymp_available": o_ok,
        "olymp_msg": o_msg,
        "eurusd": eu_st,
        "timeframe": "1h",
        "supabase_ok": eu_st.get("supabase_ok"),
        "supabase_msg": eu_st.get("supabase_msg"),
    }


@app.post("/api/ohlc-spread-olymp/start")
def ohlc_spread_olymp_start(
    body: OhlcStartBody = OhlcStartBody(),
    _: None = Depends(require_token),
) -> dict:
    """Inicia coletor Olymp OTC + Dukascopy EURUSD."""
    try:
        collector_olymp.start(body.asset or _olymp_otc_asset())
        collector_eurusd.start(
            os.environ.get("OHLC_EURUSD_ASSET", "").strip() or "EURUSD"
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return ohlc_spread_olymp_status(_)


@app.post("/api/ohlc-spread-olymp/stop")
def ohlc_spread_olymp_stop(_: None = Depends(require_token)) -> dict:
    collector_olymp.stop()
    collector_eurusd.stop()
    return ohlc_spread_olymp_status(_)


@app.post("/api/ohlc-spread-olymp/pull-now")
def ohlc_spread_olymp_pull_now(
    body: OhlcSpreadSyncBody = OhlcSpreadSyncBody(),
    _: None = Depends(require_token),
) -> dict:
    """Puxa EURUSD_otc_olymp (Olymp) + EURUSD (Dukascopy)."""
    try:
        sync = sync_spread_olymp_sources(days=body.days)
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    st = ohlc_spread_olymp_status(_)
    st["sync"] = sync
    st["pull"] = {
        "otc_upserted": (sync.get("otc") or {}).get("upserted"),
        "eurusd_upserted": (sync.get("eurusd") or {}).get("upserted"),
        "source_otc": "olymptrade",
        "source_eurusd": "dukascopy",
    }
    eu = sync.get("eurusd") or {}
    otc = sync.get("otc") or {}
    if not eu.get("ok") and not otc.get("ok"):
        detail = "; ".join(
            x for x in (otc.get("error"), eu.get("error")) if x
        ) or "Falha ao sincronizar Olymp + Dukascopy"
        raise HTTPException(status_code=502, detail=detail)
    return st


@app.post("/api/ohlc-spread-olymp/backfill-otc")
def ohlc_spread_olymp_backfill_otc(
    body: OhlcSpreadOtcHistoryBody = OhlcSpreadOtcHistoryBody(),
    _: None = Depends(require_token),
) -> dict:
    """Puxa historico profundo OTC Olymptrade."""
    otc_a = _olymp_otc_asset(body.asset)
    try:
        pull = collector_olymp.pull_history(otc_a, days=body.days)
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    st = ohlc_spread_olymp_status(_)
    st["backfill_otc"] = pull.get("pull")
    return st


@app.post("/api/ohlc-spread-olymp/backfill-dukascopy")
def ohlc_spread_olymp_backfill_dukascopy(
    body: OhlcSpreadDukaHistoryBody = OhlcSpreadDukaHistoryBody(),
    _: None = Depends(require_token),
) -> dict:
    """Puxa Dukascopy desde o candle OTC Olymp mais antigo ate agora."""
    try:
        pull = collector_eurusd.pull_history(
            days=body.days,
            match_otc=body.match_otc,
            otc_asset=body.otc_asset or _olymp_otc_asset(),
            otc_table=TABLE_OLYMP,
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    st = ohlc_spread_olymp_status(_)
    st["backfill_dukascopy"] = pull.get("pull")
    return st


@app.get("/api/ohlc-spread-olymp/export")
def ohlc_spread_olymp_export(
    days: int = 14,
    sync: int = 1,
    _: None = Depends(require_token),
) -> Response:
    lookback = max(1, min(int(days), 90))
    sync_info: dict | None = None
    if sync:
        try:
            sync_info = sync_spread_olymp_sources(days=lookback)
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(status_code=500, detail=str(exc)) from exc
        if not sync_info.get("ok"):
            otc_err = (sync_info.get("otc") or {}).get("error")
            eu_err = (sync_info.get("eurusd") or {}).get("error")
            detail = "; ".join(x for x in (otc_err, eu_err) if x) or "Sync falhou"
            raise HTTPException(status_code=502, detail=detail)

    otc_a = _olymp_otc_asset()
    eu_a = normalize_asset(
        os.environ.get("OHLC_EURUSD_ASSET", "").strip()
        or collector_eurusd.status().get("asset")
        or "EURUSD"
    )
    try:
        otc_rows = fetch_candles(
            otc_a, timeframe="1h", limit=0, table=TABLE_OLYMP
        )
        eu_rows = fetch_candles(
            eu_a, timeframe="1h", limit=0, table=TABLE_EURUSD
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(f"ohlc_1h_{otc_a}.csv", candles_to_csv(otc_rows))
        zf.writestr(f"ohlc_1h_{eu_a}_dukascopy.csv", candles_to_csv(eu_rows))
        if sync_info is not None:
            import json

            zf.writestr(
                "sync_meta.json",
                json.dumps(sync_info, ensure_ascii=False, indent=2),
            )
    data = buf.getvalue()
    fname = f"ohlc_spread_olymp_{otc_a}_{eu_a}.zip"
    return Response(
        content=data,
        media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="{fname}"'},
    )


@app.get("/api/ohlc-spread-olymp/series")
def ohlc_spread_olymp_series(
    otc_asset: str | None = None,
    eurusd_asset: str | None = None,
    limit: int = 0,
    _: None = Depends(require_token),
) -> dict:
    try:
        payload = _load_spread_bundle(
            otc_asset,
            eurusd_asset,
            limit,
            default_otc=_olymp_otc_asset(),
            otc_table=TABLE_OLYMP,
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    weekend_n = sum(1 for p in payload["spread"] if p.get("weekend"))
    after_hours_n = sum(1 for p in payload["spread"] if p.get("after_hours"))
    return {
        "timeframe": "1h",
        "otc_source": "olymptrade",
        "otc_table": TABLE_OLYMP,
        "otc_asset": payload["otc_asset"],
        "eurusd_asset": payload["eurusd_asset"],
        "eurusd_source": payload["eurusd_source"],
        "otc_count": len(payload["otc"]),
        "eurusd_count": len(payload["eurusd"]),
        "spread_count": len(payload["spread"]),
        "weekend_points": weekend_n,
        "after_hours_points": after_hours_n,
        "eurusd_opens": payload["eurusd_opens"],
        "otc": payload["otc"],
        "eurusd": payload["eurusd"],
        "spread": payload["spread"],
    }


@app.get("/api/ohlc-spread-olymp/ou")
def ohlc_spread_olymp_ou(
    otc_asset: str | None = None,
    eurusd_asset: str | None = None,
    paired_only: bool = True,
    min_n: int = 48,
    limit: int = 0,
    payout: float = 0.92,
    z_min: float = 1.5,
    edge_margin: float = 0.03,
    _: None = Depends(require_token),
) -> dict:
    try:
        payload = _load_spread_bundle(
            otc_asset,
            eurusd_asset,
            limit,
            default_otc=_olymp_otc_asset(),
            otc_table=TABLE_OLYMP,
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    analysis = analyze_spread_ou(
        payload["spread"],
        paired_only=paired_only,
        min_n=max(20, min(min_n, 5000)),
    )
    signal = evaluate_paper_signal(
        analysis,
        payout=payout,
        z_min=z_min,
        edge_margin=edge_margin,
    )
    return {
        "timeframe": "1h",
        "otc_source": "olymptrade",
        "otc_asset": payload["otc_asset"],
        "eurusd_asset": payload["eurusd_asset"],
        "eurusd_source": payload["eurusd_source"],
        "spread_count": len(payload["spread"]),
        "ou": analysis,
        "signal": signal,
    }


# --- Spread OTC vs EURUSD (1D / diario) ---


def _spread_1d_otc_asset(override: str | None = None) -> str:
    return normalize_asset(
        override
        or os.environ.get("OHLC_SPREAD_1D_OTC_ASSET", "").strip()
        or os.environ.get("OHLC_SPREAD_OTC_ASSET", "").strip()
        or os.environ.get("OHLC_1D_ASSET", "").strip()
        or collector_1d.status().get("asset")
        or "EURUSD_otc"
    )


# Preferir Dukascopy agregado / import manual se houver misturado com outras fontes.
_EURUSD_1D_PREFERRED_SOURCES = frozenset(
    {"dukascopy_agg", EURUSD_IMPORT_SOURCE, "investing_csv"}
)


def _prefer_eurusd_1d_rows(eu_rows: list) -> tuple[list, str]:
    preferred = [
        r
        for r in eu_rows
        if str(r.get("source") or "") in _EURUSD_1D_PREFERRED_SOURCES
    ]
    if preferred:
        # Se ha import manual e dukascopy, preferir o mais recente por dia via
        # ordem natural do upsert; aqui usa so preferred (ambas fontes validas).
        # Preferencia: manual_import sobrepoe dukascopy_agg no mesmo dia.
        by_day: dict[str, dict] = {}
        for r in preferred:
            key = str(r.get("opened_at") or "")[:19]
            src = str(r.get("source") or "")
            prev = by_day.get(key)
            if prev is None:
                by_day[key] = r
                continue
            prev_src = str(prev.get("source") or "")
            if src == EURUSD_IMPORT_SOURCE and prev_src != EURUSD_IMPORT_SOURCE:
                by_day[key] = r
        rows = sorted(by_day.values(), key=lambda x: str(x.get("opened_at") or ""))
        sources = {str(r.get("source") or "") for r in rows}
        if EURUSD_IMPORT_SOURCE in sources and "dukascopy_agg" in sources:
            eu_source = f"{EURUSD_IMPORT_SOURCE}+dukascopy_agg"
        elif EURUSD_IMPORT_SOURCE in sources:
            eu_source = EURUSD_IMPORT_SOURCE
        else:
            eu_source = next(iter(sources)) if sources else "unknown"
        return rows, eu_source
    eu_source = (eu_rows[-1].get("source") if eu_rows else None) or "unknown"
    return eu_rows, str(eu_source)


def _load_spread_1d_bundle(
    otc_asset: str | None,
    eurusd_asset: str | None,
    limit: int,
) -> dict:
    otc_a = _spread_1d_otc_asset(otc_asset)
    eu_a = normalize_asset(
        eurusd_asset
        or collector_eurusd.status().get("asset")
        or "EURUSD"
    )
    otc_rows = fetch_candles(
        otc_a, timeframe="1d", limit=limit, table=TABLE_1D
    )
    eu_rows = fetch_candles(
        eu_a, timeframe="1d", limit=limit, table=TABLE_EURUSD_1D
    )
    eu_rows, eu_source = _prefer_eurusd_1d_rows(eu_rows)
    try:
        spread = build_spread_1d(otc_rows, eu_rows)
        opens = detect_eurusd_opens(eu_rows)
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"Falha ao montar spread 1d: {exc}") from exc
    return {
        "otc_asset": otc_a,
        "eurusd_asset": eu_a,
        "eurusd_source": eu_source,
        "otc": otc_rows,
        "eurusd": eu_rows,
        "spread": spread,
        "eurusd_opens": opens,
    }


@app.get("/api/ohlc-spread-1d/status")
def ohlc_spread_1d_status(_: None = Depends(require_token)) -> dict:
    otc_asset = _spread_1d_otc_asset()
    eu_a = normalize_asset(
        os.environ.get("OHLC_EURUSD_ASSET", "").strip()
        or collector_eurusd.status().get("asset")
        or "EURUSD"
    )
    try:
        otc_sum = stored_summary(otc_asset, "1d", table=TABLE_1D)
    except Exception as exc:  # noqa: BLE001
        otc_sum = {
            "stored_count": None,
            "stored_last": None,
            "stored_err": str(exc)[:200],
        }
    try:
        eu_sum = stored_summary(eu_a, "1d", table=TABLE_EURUSD_1D)
    except Exception as exc:  # noqa: BLE001
        eu_sum = {
            "stored_count": None,
            "stored_last": None,
            "stored_err": str(exc)[:200],
        }
    otc_st = collector_1d.status()
    return {
        "otc": {
            "asset": otc_asset,
            "collector_running": collector_1d.is_running(),
            "table": TABLE_1D,
            "phase": otc_st.get("phase"),
            "message": otc_st.get("message"),
            **otc_sum,
        },
        "eurusd": {
            "asset": eu_a,
            "running": False,
            "phase": "dukascopy_agg",
            "message": "EURUSD D1 em ohlc_candles_eurusd_1d (agg Dukascopy 1h)",
            "table": TABLE_EURUSD_1D,
            "source": "dukascopy_agg",
            "supabase_ok": otc_st.get("supabase_ok"),
            "supabase_msg": otc_st.get("supabase_msg"),
            **eu_sum,
        },
        "timeframe": "1d",
        "supabase_ok": otc_st.get("supabase_ok"),
        "supabase_msg": otc_st.get("supabase_msg"),
    }


@app.post("/api/ohlc-spread-1d/start")
def ohlc_spread_1d_start(
    body: OhlcStartBody = OhlcStartBody(),
    _: None = Depends(require_token),
) -> dict:
    """Inicia coletor OTC D1 (Pocket). EURUSD D1 via sync/agregacao."""
    asset = body.asset or _spread_1d_otc_asset()
    try:
        collector_1d.start(asset)
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return ohlc_spread_1d_status(_)


@app.post("/api/ohlc-spread-1d/stop")
def ohlc_spread_1d_stop(_: None = Depends(require_token)) -> dict:
    collector_1d.stop()
    return ohlc_spread_1d_status(_)


@app.post("/api/ohlc-spread-1d/pull-now")
def ohlc_spread_1d_pull_now(
    body: OhlcSpreadSyncBody = OhlcSpreadSyncBody(),
    _: None = Depends(require_token),
) -> dict:
    """Pente fino D1: OTC Pocket + EURUSD agregado Dukascopy."""
    # Body.days no 1h e 1–90; no 1d aceitamos ate 90 via mesmo modelo.
    try:
        sync = sync_spread_1d_sources(days=max(body.days, 30))
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    st = ohlc_spread_1d_status(_)
    st["sync"] = sync
    st["pull"] = {
        "otc_upserted": (sync.get("otc") or {}).get("upserted"),
        "eurusd_upserted": (sync.get("eurusd") or {}).get("upserted"),
        "source_eurusd": "dukascopy_agg",
    }
    eu = sync.get("eurusd") or {}
    if not eu.get("ok"):
        detail = eu.get("error") or "Falha ao sincronizar EURUSD D1"
        raise HTTPException(status_code=502, detail=detail)
    return st


@app.post("/api/ohlc-spread-1d/backfill-otc")
def ohlc_spread_1d_backfill_otc(
    body: OhlcSpreadOtcHistoryBody = OhlcSpreadOtcHistoryBody(),
    _: None = Depends(require_token),
) -> dict:
    """Puxa historico D1 nativo da Pocket (EURUSD_otc)."""
    otc_a = _spread_1d_otc_asset(body.asset)
    try:
        if collector_1d.status().get("asset") != otc_a:
            try:
                collector_1d.set_asset(otc_a)
            except RuntimeError:
                pass
        pull = collector_1d.pull_now()
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    st = ohlc_spread_1d_status(_)
    info = pull.get("pull") or {}
    st["backfill_otc"] = {
        "upserted": info.get("upserted"),
        "fetched": info.get("upserted"),
        "asset": otc_a,
        "timeframe": "1d",
    }
    return st


@app.post("/api/ohlc-spread-1d/backfill-dukascopy")
def ohlc_spread_1d_backfill_dukascopy(
    body: OhlcSpreadDukaHistoryBody = OhlcSpreadDukaHistoryBody(),
    _: None = Depends(require_token),
) -> dict:
    """Puxa Dukascopy 1h casado ao OTC D1 e agrega EURUSD diario."""
    otc_a = _spread_1d_otc_asset(body.otc_asset)
    try:
        pull = backfill_dukascopy_1d(
            days=body.days,
            match_otc=body.match_otc,
            otc_asset=otc_a,
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    st = ohlc_spread_1d_status(_)
    st["backfill_dukascopy"] = pull
    return st


@app.post("/api/ohlc-spread-1d/import-eurusd")
async def ohlc_spread_1d_import_eurusd(
    file: UploadFile = File(...),
    _: None = Depends(require_token),
) -> dict:
    """Importa CSV/Excel diario EURUSD (Investing.com PT) → ohlc_candles_eurusd_1d."""
    filename = file.filename or "upload.csv"
    lower = filename.lower()
    if not (
        lower.endswith(".csv")
        or lower.endswith(".txt")
        or lower.endswith(".xlsx")
        or lower.endswith(".xls")
    ):
        raise HTTPException(
            status_code=400,
            detail="Use ficheiro .csv, .txt ou .xlsx (EURUSD diario).",
        )
    if lower.endswith(".xls") and not lower.endswith(".xlsx"):
        raise HTTPException(
            status_code=400,
            detail="Formato .xls antigo nao suportado — salve como .xlsx ou .csv.",
        )
    raw = await file.read()
    if not raw:
        raise HTTPException(status_code=400, detail="Ficheiro vazio.")
    if len(raw) > 12 * 1024 * 1024:
        raise HTTPException(status_code=400, detail="Ficheiro demasiado grande (>12MB).")

    eu_a = normalize_asset(
        os.environ.get("OHLC_EURUSD_ASSET", "").strip() or "EURUSD"
    )
    try:
        rows = parse_eurusd_daily_bytes(raw, filename=filename, asset=eu_a)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(
            status_code=400, detail=f"Falha ao ler ficheiro: {exc}"
        ) from exc

    try:
        upserted = upsert_candles(rows, table=TABLE_EURUSD_1D) if rows else 0
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(
            status_code=500, detail=f"Falha ao gravar no Supabase: {exc}"
        ) from exc

    summary = stored_summary(eu_a, "1d", table=TABLE_EURUSD_1D)
    return {
        "ok": True,
        "filename": filename,
        "asset": eu_a,
        "timeframe": "1d",
        "table": TABLE_EURUSD_1D,
        "source": EURUSD_IMPORT_SOURCE,
        "parsed": len(rows),
        "upserted": upserted,
        "first": rows[0]["opened_at"] if rows else None,
        "last": rows[-1]["opened_at"] if rows else None,
        "stored": summary,
    }


@app.get("/api/ohlc-spread-1d/export")
def ohlc_spread_1d_export(
    days: int = 120,
    sync: int = 1,
    _: None = Depends(require_token),
) -> Response:
    lookback = max(1, min(int(days), 800))
    sync_info: dict | None = None
    if sync:
        try:
            sync_info = sync_spread_1d_sources(days=min(lookback, 120))
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(status_code=500, detail=str(exc)) from exc
        if not sync_info.get("ok"):
            otc_err = (sync_info.get("otc") or {}).get("error")
            eu_err = (sync_info.get("eurusd") or {}).get("error")
            detail = "; ".join(x for x in (otc_err, eu_err) if x) or "Sync falhou"
            raise HTTPException(status_code=502, detail=detail)

    otc_a = _spread_1d_otc_asset()
    eu_a = normalize_asset(
        os.environ.get("OHLC_EURUSD_ASSET", "").strip() or "EURUSD"
    )
    otc_rows = fetch_candles_range(otc_a, timeframe="1d", table=TABLE_1D)
    eu_rows = fetch_candles_range(eu_a, timeframe="1d", table=TABLE_EURUSD_1D)
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(f"ohlc_1d_{otc_a}.csv", candles_to_csv(otc_rows))
        zf.writestr(f"ohlc_1d_{eu_a}_dukascopy.csv", candles_to_csv(eu_rows))
        if sync_info is not None:
            import json

            zf.writestr("sync.json", json.dumps(sync_info, indent=2, default=str))
    fname = f"ohlc_spread_1d_{otc_a}_{eu_a}.zip"
    return Response(
        content=buf.getvalue(),
        media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="{fname}"'},
    )


@app.get("/api/ohlc-spread-1d/series")
def ohlc_spread_1d_series(
    otc_asset: str | None = None,
    eurusd_asset: str | None = None,
    limit: int = 0,
    _: None = Depends(require_token),
) -> dict:
    try:
        payload = _load_spread_1d_bundle(otc_asset, eurusd_asset, limit)
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    weekend_n = sum(1 for p in payload["spread"] if p.get("weekend"))
    after_hours_n = sum(1 for p in payload["spread"] if p.get("after_hours"))
    return {
        "timeframe": "1d",
        "otc_asset": payload["otc_asset"],
        "eurusd_asset": payload["eurusd_asset"],
        "eurusd_source": payload["eurusd_source"],
        "otc_count": len(payload["otc"]),
        "eurusd_count": len(payload["eurusd"]),
        "spread_count": len(payload["spread"]),
        "weekend_points": weekend_n,
        "after_hours_points": after_hours_n,
        "otc": payload["otc"],
        "eurusd": payload["eurusd"],
        "spread": payload["spread"],
        "eurusd_opens": payload["eurusd_opens"],
    }


@app.get("/api/ohlc-spread-1d/ou")
def ohlc_spread_1d_ou(
    otc_asset: str | None = None,
    eurusd_asset: str | None = None,
    limit: int = 0,
    paired_only: bool = True,
    min_n: int = 48,
    payout: float = 0.92,
    z_min: float = 1.5,
    edge_margin: float = 0.03,
    _: None = Depends(require_token),
) -> dict:
    try:
        payload = _load_spread_1d_bundle(otc_asset, eurusd_asset, limit)
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    analysis = analyze_spread_ou(
        payload["spread"],
        paired_only=paired_only,
        min_n=max(20, min(min_n, 5000)),
    )
    signal = evaluate_paper_signal(
        analysis,
        payout=payout,
        z_min=z_min,
        edge_margin=edge_margin,
    )
    return {
        "timeframe": "1d",
        "otc_source": "pocket",
        "otc_asset": payload["otc_asset"],
        "eurusd_asset": payload["eurusd_asset"],
        "eurusd_source": payload["eurusd_source"],
        "spread_count": len(payload["spread"]),
        "ou": analysis,
        "signal": signal,
    }


# --- Spread Olymp OTC vs EURUSD (1D) ---


def _spread_olymp_1d_otc_asset(override: str | None = None) -> str:
    return normalize_asset(
        override
        or os.environ.get("OHLC_SPREAD_OLYMP_1D_OTC_ASSET", "").strip()
        or os.environ.get("OHLC_OLYMP_OTC_ASSET", "").strip()
        or collector_olymp.status().get("asset")
        or olymp_default_otc_asset()
    )


def _load_spread_olymp_1d_bundle(
    otc_asset: str | None,
    eurusd_asset: str | None,
    limit: int,
) -> dict:
    otc_a = _spread_olymp_1d_otc_asset(otc_asset)
    eu_a = normalize_asset(
        eurusd_asset
        or collector_eurusd.status().get("asset")
        or "EURUSD"
    )
    otc_rows = fetch_candles(
        otc_a, timeframe="1d", limit=limit, table=TABLE_OLYMP_1D
    )
    eu_rows = fetch_candles(
        eu_a, timeframe="1d", limit=limit, table=TABLE_EURUSD_1D
    )
    eu_rows, eu_source = _prefer_eurusd_1d_rows(eu_rows)
    try:
        spread = build_spread_1d(otc_rows, eu_rows)
        opens = detect_eurusd_opens(eu_rows)
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"Falha ao montar spread Olymp 1d: {exc}") from exc
    return {
        "otc_asset": otc_a,
        "eurusd_asset": eu_a,
        "eurusd_source": eu_source,
        "otc": otc_rows,
        "eurusd": eu_rows,
        "spread": spread,
        "eurusd_opens": opens,
    }


@app.get("/api/ohlc-spread-olymp-1d/status")
def ohlc_spread_olymp_1d_status(_: None = Depends(require_token)) -> dict:
    otc_asset = _spread_olymp_1d_otc_asset()
    eu_a = normalize_asset(
        os.environ.get("OHLC_EURUSD_ASSET", "").strip()
        or collector_eurusd.status().get("asset")
        or "EURUSD"
    )
    try:
        otc_sum = stored_summary(otc_asset, "1d", table=TABLE_OLYMP_1D)
    except Exception as exc:  # noqa: BLE001
        otc_sum = {
            "stored_count": None,
            "stored_last": None,
            "stored_err": str(exc)[:200],
        }
    try:
        eu_sum = stored_summary(eu_a, "1d", table=TABLE_EURUSD_1D)
    except Exception as exc:  # noqa: BLE001
        eu_sum = {
            "stored_count": None,
            "stored_last": None,
            "stored_err": str(exc)[:200],
        }
    otc_st = collector_olymp.status()
    return {
        "otc": {
            "asset": otc_asset,
            "collector_running": collector_olymp.is_running(),
            "table": TABLE_OLYMP_1D,
            "pair": otc_st.get("pair"),
            "phase": otc_st.get("phase"),
            "message": otc_st.get("message"),
            **otc_sum,
        },
        "eurusd": {
            "asset": eu_a,
            "running": False,
            "phase": "dukascopy_agg",
            "message": "EURUSD D1 em ohlc_candles_eurusd_1d",
            "table": TABLE_EURUSD_1D,
            "source": "dukascopy_agg",
            "supabase_ok": otc_st.get("supabase_ok"),
            "supabase_msg": otc_st.get("supabase_msg"),
            **eu_sum,
        },
        "timeframe": "1d",
        "supabase_ok": otc_st.get("supabase_ok"),
        "supabase_msg": otc_st.get("supabase_msg"),
    }


@app.post("/api/ohlc-spread-olymp-1d/start")
def ohlc_spread_olymp_1d_start(
    body: OhlcStartBody = OhlcStartBody(),
    _: None = Depends(require_token),
) -> dict:
    """Inicia coletor Olymp 1h (base da agregacao D1)."""
    asset = body.asset or _spread_olymp_1d_otc_asset()
    try:
        collector_olymp.start(asset)
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return ohlc_spread_olymp_1d_status(_)


@app.post("/api/ohlc-spread-olymp-1d/stop")
def ohlc_spread_olymp_1d_stop(_: None = Depends(require_token)) -> dict:
    collector_olymp.stop()
    return ohlc_spread_olymp_1d_status(_)


@app.post("/api/ohlc-spread-olymp-1d/pull-now")
def ohlc_spread_olymp_1d_pull_now(
    body: OhlcSpreadSyncBody = OhlcSpreadSyncBody(),
    _: None = Depends(require_token),
) -> dict:
    try:
        sync = sync_spread_olymp_1d_sources(days=max(body.days, 30))
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    st = ohlc_spread_olymp_1d_status(_)
    st["sync"] = sync
    st["pull"] = {
        "otc_upserted": (sync.get("otc") or {}).get("upserted"),
        "eurusd_upserted": (sync.get("eurusd") or {}).get("upserted"),
        "source_eurusd": "dukascopy_agg",
        "source_otc": "olymptrade_agg",
    }
    eu = sync.get("eurusd") or {}
    otc = sync.get("otc") or {}
    if not eu.get("ok") and not otc.get("ok"):
        detail = eu.get("error") or otc.get("error") or "Falha sync Olymp 1D"
        raise HTTPException(status_code=502, detail=detail)
    return st


@app.post("/api/ohlc-spread-olymp-1d/backfill-otc")
def ohlc_spread_olymp_1d_backfill_otc(
    body: OhlcSpreadOtcHistoryBody = OhlcSpreadOtcHistoryBody(),
    _: None = Depends(require_token),
) -> dict:
    otc_a = _spread_olymp_1d_otc_asset(body.asset)
    hours = max(24, min(int(body.days) * 24, 24 * 800))
    try:
        pull = backfill_olymp_otc_1d(hours=hours, otc_asset=otc_a)
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    st = ohlc_spread_olymp_1d_status(_)
    st["backfill_otc"] = pull
    return st


@app.post("/api/ohlc-spread-olymp-1d/backfill-dukascopy")
def ohlc_spread_olymp_1d_backfill_dukascopy(
    body: OhlcSpreadDukaHistoryBody = OhlcSpreadDukaHistoryBody(),
    _: None = Depends(require_token),
) -> dict:
    otc_a = _spread_olymp_1d_otc_asset(body.otc_asset)
    try:
        pull = backfill_dukascopy_olymp_1d(
            days=body.days,
            match_otc=body.match_otc,
            otc_asset=otc_a,
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    st = ohlc_spread_olymp_1d_status(_)
    st["backfill_dukascopy"] = pull
    return st


@app.get("/api/ohlc-spread-olymp-1d/export")
def ohlc_spread_olymp_1d_export(
    days: int = 120,
    sync: int = 1,
    _: None = Depends(require_token),
) -> Response:
    lookback = max(1, min(int(days), 800))
    sync_info: dict | None = None
    if sync:
        try:
            sync_info = sync_spread_olymp_1d_sources(days=min(lookback, 120))
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(status_code=500, detail=str(exc)) from exc
        if not sync_info.get("ok"):
            otc_err = (sync_info.get("otc") or {}).get("error")
            eu_err = (sync_info.get("eurusd") or {}).get("error")
            detail = "; ".join(x for x in (otc_err, eu_err) if x) or "Sync falhou"
            raise HTTPException(status_code=502, detail=detail)

    otc_a = _spread_olymp_1d_otc_asset()
    eu_a = normalize_asset(
        os.environ.get("OHLC_EURUSD_ASSET", "").strip() or "EURUSD"
    )
    otc_rows = fetch_candles_range(otc_a, timeframe="1d", table=TABLE_OLYMP_1D)
    eu_rows = fetch_candles_range(eu_a, timeframe="1d", table=TABLE_EURUSD_1D)
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(f"ohlc_1d_{otc_a}.csv", candles_to_csv(otc_rows))
        zf.writestr(f"ohlc_1d_{eu_a}_dukascopy.csv", candles_to_csv(eu_rows))
        if sync_info is not None:
            import json

            zf.writestr("sync.json", json.dumps(sync_info, indent=2, default=str))
    fname = f"ohlc_spread_olymp_1d_{otc_a}_{eu_a}.zip"
    return Response(
        content=buf.getvalue(),
        media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="{fname}"'},
    )


@app.get("/api/ohlc-spread-olymp-1d/series")
def ohlc_spread_olymp_1d_series(
    otc_asset: str | None = None,
    eurusd_asset: str | None = None,
    limit: int = 0,
    _: None = Depends(require_token),
) -> dict:
    try:
        payload = _load_spread_olymp_1d_bundle(otc_asset, eurusd_asset, limit)
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    weekend_n = sum(1 for p in payload["spread"] if p.get("weekend"))
    after_hours_n = sum(1 for p in payload["spread"] if p.get("after_hours"))
    return {
        "timeframe": "1d",
        "otc_source": "olymptrade",
        "otc_asset": payload["otc_asset"],
        "eurusd_asset": payload["eurusd_asset"],
        "eurusd_source": payload["eurusd_source"],
        "otc_count": len(payload["otc"]),
        "eurusd_count": len(payload["eurusd"]),
        "spread_count": len(payload["spread"]),
        "weekend_points": weekend_n,
        "after_hours_points": after_hours_n,
        "otc": payload["otc"],
        "eurusd": payload["eurusd"],
        "spread": payload["spread"],
        "eurusd_opens": payload["eurusd_opens"],
    }


@app.get("/api/ohlc-spread-olymp-1d/ou")
def ohlc_spread_olymp_1d_ou(
    otc_asset: str | None = None,
    eurusd_asset: str | None = None,
    limit: int = 0,
    paired_only: bool = True,
    min_n: int = 48,
    payout: float = 0.92,
    z_min: float = 1.5,
    edge_margin: float = 0.03,
    _: None = Depends(require_token),
) -> dict:
    try:
        payload = _load_spread_olymp_1d_bundle(otc_asset, eurusd_asset, limit)
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    analysis = analyze_spread_ou(
        payload["spread"],
        paired_only=paired_only,
        min_n=max(20, min(min_n, 5000)),
    )
    signal = evaluate_paper_signal(
        analysis,
        payout=payout,
        z_min=z_min,
        edge_margin=edge_margin,
    )
    return {
        "timeframe": "1d",
        "otc_source": "olymptrade",
        "otc_asset": payload["otc_asset"],
        "eurusd_asset": payload["eurusd_asset"],
        "eurusd_source": payload["eurusd_source"],
        "spread_count": len(payload["spread"]),
        "ou": analysis,
        "signal": signal,
    }


# --- Spread ExpertOption OTC vs EURUSD (1D) ---


def _spread_expert_1d_otc_asset(override: str | None = None) -> str:
    return normalize_asset(
        override
        or os.environ.get("OHLC_SPREAD_EXPERT_1D_OTC_ASSET", "").strip()
        or os.environ.get("OHLC_EXPERT_OTC_ASSET", "").strip()
        or collector_expert.status().get("asset")
        or expert_default_otc_asset()
    )


def _load_spread_expert_1d_bundle(
    otc_asset: str | None,
    eurusd_asset: str | None,
    limit: int,
) -> dict:
    otc_a = _spread_expert_1d_otc_asset(otc_asset)
    eu_a = normalize_asset(
        eurusd_asset
        or collector_eurusd.status().get("asset")
        or "EURUSD"
    )
    otc_rows = fetch_candles(
        otc_a, timeframe="1d", limit=limit, table=TABLE_EXPERT_1D
    )
    eu_rows = fetch_candles(
        eu_a, timeframe="1d", limit=limit, table=TABLE_EURUSD_1D
    )
    eu_rows, eu_source = _prefer_eurusd_1d_rows(eu_rows)
    try:
        spread = build_spread_1d(otc_rows, eu_rows)
        opens = detect_eurusd_opens(eu_rows)
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"Falha ao montar spread Expert 1d: {exc}") from exc
    return {
        "otc_asset": otc_a,
        "eurusd_asset": eu_a,
        "eurusd_source": eu_source,
        "otc": otc_rows,
        "eurusd": eu_rows,
        "spread": spread,
        "eurusd_opens": opens,
    }


@app.get("/api/ohlc-spread-expert-1d/status")
def ohlc_spread_expert_1d_status(_: None = Depends(require_token)) -> dict:
    otc_asset = _spread_expert_1d_otc_asset()
    eu_a = normalize_asset(
        os.environ.get("OHLC_EURUSD_ASSET", "").strip()
        or collector_eurusd.status().get("asset")
        or "EURUSD"
    )
    try:
        otc_sum = stored_summary(otc_asset, "1d", table=TABLE_EXPERT_1D)
    except Exception as exc:  # noqa: BLE001
        otc_sum = {
            "stored_count": None,
            "stored_last": None,
            "stored_err": str(exc)[:200],
        }
    try:
        eu_sum = stored_summary(eu_a, "1d", table=TABLE_EURUSD_1D)
    except Exception as exc:  # noqa: BLE001
        eu_sum = {
            "stored_count": None,
            "stored_last": None,
            "stored_err": str(exc)[:200],
        }
    otc_st = collector_expert.status()
    return {
        "otc": {
            "asset": otc_asset,
            "collector_running": collector_expert.is_running(),
            "table": TABLE_EXPERT_1D,
            "pair": otc_st.get("pair"),
            "phase": otc_st.get("phase"),
            "message": otc_st.get("message"),
            **otc_sum,
        },
        "eurusd": {
            "asset": eu_a,
            "running": False,
            "phase": "dukascopy_agg",
            "message": "EURUSD D1 em ohlc_candles_eurusd_1d",
            "table": TABLE_EURUSD_1D,
            "source": "dukascopy_agg",
            "supabase_ok": otc_st.get("supabase_ok"),
            "supabase_msg": otc_st.get("supabase_msg"),
            **eu_sum,
        },
        "timeframe": "1d",
        "supabase_ok": otc_st.get("supabase_ok"),
        "supabase_msg": otc_st.get("supabase_msg"),
    }


@app.post("/api/ohlc-spread-expert-1d/start")
def ohlc_spread_expert_1d_start(
    body: OhlcStartBody = OhlcStartBody(),
    _: None = Depends(require_token),
) -> dict:
    """Inicia coletor Expert 1h (base da agregacao D1)."""
    asset = body.asset or _spread_expert_1d_otc_asset()
    try:
        collector_expert.start(asset)
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return ohlc_spread_expert_1d_status(_)


@app.post("/api/ohlc-spread-expert-1d/stop")
def ohlc_spread_expert_1d_stop(_: None = Depends(require_token)) -> dict:
    collector_expert.stop()
    return ohlc_spread_expert_1d_status(_)


@app.post("/api/ohlc-spread-expert-1d/pull-now")
def ohlc_spread_expert_1d_pull_now(
    body: OhlcSpreadSyncBody = OhlcSpreadSyncBody(),
    _: None = Depends(require_token),
) -> dict:
    try:
        sync = sync_spread_expert_1d_sources(days=max(body.days, 30))
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    st = ohlc_spread_expert_1d_status(_)
    st["sync"] = sync
    st["pull"] = {
        "otc_upserted": (sync.get("otc") or {}).get("upserted"),
        "eurusd_upserted": (sync.get("eurusd") or {}).get("upserted"),
        "source_eurusd": "dukascopy_agg",
        "source_otc": "expertoption_agg",
    }
    eu = sync.get("eurusd") or {}
    otc = sync.get("otc") or {}
    if not eu.get("ok") and not otc.get("ok"):
        detail = eu.get("error") or otc.get("error") or "Falha sync Expert 1D"
        raise HTTPException(status_code=502, detail=detail)
    return st


@app.post("/api/ohlc-spread-expert-1d/backfill-otc")
def ohlc_spread_expert_1d_backfill_otc(
    body: OhlcSpreadOtcHistoryBody = OhlcSpreadOtcHistoryBody(),
    _: None = Depends(require_token),
) -> dict:
    otc_a = _spread_expert_1d_otc_asset(body.asset)
    hours = max(24, min(int(body.days) * 24, 24 * 800))
    try:
        pull = backfill_expert_otc_1d(hours=hours, otc_asset=otc_a)
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    st = ohlc_spread_expert_1d_status(_)
    st["backfill_otc"] = pull
    return st


@app.post("/api/ohlc-spread-expert-1d/backfill-dukascopy")
def ohlc_spread_expert_1d_backfill_dukascopy(
    body: OhlcSpreadDukaHistoryBody = OhlcSpreadDukaHistoryBody(),
    _: None = Depends(require_token),
) -> dict:
    otc_a = _spread_expert_1d_otc_asset(body.otc_asset)
    try:
        pull = backfill_dukascopy_expert_1d(
            days=body.days,
            match_otc=body.match_otc,
            otc_asset=otc_a,
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    st = ohlc_spread_expert_1d_status(_)
    st["backfill_dukascopy"] = pull
    return st


@app.get("/api/ohlc-spread-expert-1d/export")
def ohlc_spread_expert_1d_export(
    days: int = 120,
    sync: int = 1,
    _: None = Depends(require_token),
) -> Response:
    lookback = max(1, min(int(days), 800))
    sync_info: dict | None = None
    if sync:
        try:
            sync_info = sync_spread_expert_1d_sources(days=min(lookback, 120))
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(status_code=500, detail=str(exc)) from exc
        if not sync_info.get("ok"):
            otc_err = (sync_info.get("otc") or {}).get("error")
            eu_err = (sync_info.get("eurusd") or {}).get("error")
            detail = "; ".join(x for x in (otc_err, eu_err) if x) or "Sync falhou"
            raise HTTPException(status_code=502, detail=detail)

    otc_a = _spread_expert_1d_otc_asset()
    eu_a = normalize_asset(
        os.environ.get("OHLC_EURUSD_ASSET", "").strip() or "EURUSD"
    )
    otc_rows = fetch_candles_range(otc_a, timeframe="1d", table=TABLE_EXPERT_1D)
    eu_rows = fetch_candles_range(eu_a, timeframe="1d", table=TABLE_EURUSD_1D)
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(f"ohlc_1d_{otc_a}.csv", candles_to_csv(otc_rows))
        zf.writestr(f"ohlc_1d_{eu_a}_dukascopy.csv", candles_to_csv(eu_rows))
        if sync_info is not None:
            import json

            zf.writestr("sync.json", json.dumps(sync_info, indent=2, default=str))
    fname = f"ohlc_spread_expert_1d_{otc_a}_{eu_a}.zip"
    return Response(
        content=buf.getvalue(),
        media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="{fname}"'},
    )


@app.get("/api/ohlc-spread-expert-1d/series")
def ohlc_spread_expert_1d_series(
    otc_asset: str | None = None,
    eurusd_asset: str | None = None,
    limit: int = 0,
    _: None = Depends(require_token),
) -> dict:
    try:
        payload = _load_spread_expert_1d_bundle(otc_asset, eurusd_asset, limit)
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    weekend_n = sum(1 for p in payload["spread"] if p.get("weekend"))
    after_hours_n = sum(1 for p in payload["spread"] if p.get("after_hours"))
    return {
        "timeframe": "1d",
        "otc_source": "expertoption",
        "otc_asset": payload["otc_asset"],
        "eurusd_asset": payload["eurusd_asset"],
        "eurusd_source": payload["eurusd_source"],
        "otc_count": len(payload["otc"]),
        "eurusd_count": len(payload["eurusd"]),
        "spread_count": len(payload["spread"]),
        "weekend_points": weekend_n,
        "after_hours_points": after_hours_n,
        "otc": payload["otc"],
        "eurusd": payload["eurusd"],
        "spread": payload["spread"],
        "eurusd_opens": payload["eurusd_opens"],
    }


@app.get("/api/ohlc-spread-expert-1d/ou")
def ohlc_spread_expert_1d_ou(
    otc_asset: str | None = None,
    eurusd_asset: str | None = None,
    limit: int = 0,
    paired_only: bool = True,
    min_n: int = 48,
    payout: float = 0.92,
    z_min: float = 1.5,
    edge_margin: float = 0.03,
    _: None = Depends(require_token),
) -> dict:
    try:
        payload = _load_spread_expert_1d_bundle(otc_asset, eurusd_asset, limit)
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    analysis = analyze_spread_ou(
        payload["spread"],
        paired_only=paired_only,
        min_n=max(20, min(min_n, 5000)),
    )
    signal = evaluate_paper_signal(
        analysis,
        payout=payout,
        z_min=z_min,
        edge_margin=edge_margin,
    )
    return {
        "timeframe": "1d",
        "otc_source": "expertoption",
        "otc_asset": payload["otc_asset"],
        "eurusd_asset": payload["eurusd_asset"],
        "eurusd_source": payload["eurusd_source"],
        "spread_count": len(payload["spread"]),
        "ou": analysis,
        "signal": signal,
    }

