"""API + portal web do IQ Blitz Bot."""

from __future__ import annotations

import os
import secrets
from pathlib import Path

from fastapi import Depends, FastAPI, Header, HTTPException
from fastapi.responses import FileResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from bot.ohlc_collector import collector
from bot.ohlc_collector_1d import collector_1d, TABLE_1D
from bot.ohlc_collector_eurusd import TABLE_EURUSD, collector_eurusd
from bot.ohlc_spread import build_spread_1h
from bot.ohlc_store import (
    candles_to_csv,
    fetch_candles,
    fetch_candles_range,
    stored_summary,
)
from bot.runner import MIN_DURATION, normalize_asset, runner

STATIC = Path(__file__).resolve().parent / "static"

app = FastAPI(title="IQ Blitz Bot — Portal", version="1.0.0")
app.mount("/static", StaticFiles(directory=str(STATIC)), name="static")


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
    return FileResponse(STATIC / "index.html")


@app.get("/bot")
def bot_page() -> FileResponse:
    return FileResponse(STATIC / "bot.html")


@app.get("/ohlc")
def ohlc_page() -> FileResponse:
    return FileResponse(STATIC / "ohlc.html")


@app.get("/ohlc-1d")
def ohlc_1d_page() -> FileResponse:
    return FileResponse(STATIC / "ohlc_1d.html")


@app.get("/ohlc-spread")
def ohlc_spread_page() -> FileResponse:
    return FileResponse(STATIC / "ohlc_spread.html")


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
    eu_st = collector_eurusd.status()
    otc_sum = stored_summary(otc_asset, "1h")
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
    """Inicia coletor EURUSD (mercado). OTC continua no /ohlc."""
    try:
        eu = collector_eurusd.start(body.asset or "EURUSD")
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return ohlc_spread_status(_)


@app.post("/api/ohlc-spread/stop")
def ohlc_spread_stop(_: None = Depends(require_token)) -> dict:
    collector_eurusd.stop()
    return ohlc_spread_status(_)


@app.post("/api/ohlc-spread/pull-now")
def ohlc_spread_pull_now(_: None = Depends(require_token)) -> dict:
    try:
        pull = collector_eurusd.pull_now()
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    st = ohlc_spread_status(_)
    st["pull"] = pull.get("pull")
    return st


@app.get("/api/ohlc-spread/series")
def ohlc_spread_series(
    otc_asset: str | None = None,
    eurusd_asset: str | None = None,
    limit: int = 0,
    _: None = Depends(require_token),
) -> dict:
    """Candles OTC + EURUSD + serie de spread (carry no fim de semana)."""
    otc_a = normalize_asset(
        otc_asset
        or os.environ.get("OHLC_SPREAD_OTC_ASSET", "").strip()
        or collector.status().get("asset")
        or "EURUSD_otc"
    )
    eu_a = normalize_asset(
        eurusd_asset
        or collector_eurusd.status().get("asset")
        or "EURUSD"
    )
    try:
        otc_rows = fetch_candles(otc_a, timeframe="1h", limit=limit)
        eu_rows = fetch_candles(
            eu_a, timeframe="1h", limit=limit, table=TABLE_EURUSD
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    spread = build_spread_1h(otc_rows, eu_rows)
    weekend_n = sum(1 for p in spread if p.get("weekend"))
    return {
        "timeframe": "1h",
        "otc_asset": otc_a,
        "eurusd_asset": eu_a,
        "otc_count": len(otc_rows),
        "eurusd_count": len(eu_rows),
        "spread_count": len(spread),
        "weekend_points": weekend_n,
        "otc": otc_rows,
        "eurusd": eu_rows,
        "spread": spread,
    }
