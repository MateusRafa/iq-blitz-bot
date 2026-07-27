"""API + portal web do IQ Blitz Bot."""

from __future__ import annotations

import os
import secrets
from datetime import datetime, timedelta, timezone
from pathlib import Path

from fastapi import Depends, FastAPI, Header, HTTPException, Query
from fastapi.responses import FileResponse, Response
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from bot.ohlc_collector import collector
from bot.ohlc_collector_1m import collector_1m
from bot.ohlc_store import (
    TABLE_1M,
    candles_to_csv,
    fetch_candles,
    fetch_candles_range,
    run_retention_cleanup_1m,
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


class OhlcResyncBody(BaseModel):
    minutes: int = Field(default=20, ge=1, le=180)


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


@app.get("/ohlc-1m")
def ohlc_1m_page() -> FileResponse:
    return FileResponse(STATIC / "ohlc_1m.html")


@app.get("/api/health")
def health() -> dict:
    st = collector.status()
    st1 = collector_1m.status()
    return {
        "ok": True,
        "bot_running": runner.is_running(),
        "ohlc_running": collector.is_running(),
        "ohlc_1m_running": collector_1m.is_running(),
        "supabase_ok": bool(st.get("supabase_ok")),
        "token_configured": bool(_control_token()),
        "duration_seconds": runner.get_duration_seconds(),
        "ohlc_1m_warn": bool((st1.get("retention") or {}).get("warn")),
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
    limit: int = 200,
    _: None = Depends(require_token),
) -> dict:
    """Candles salvos no Supabase (para o grafico da ferramenta)."""
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
    }


# --- OHLC 1m ---


@app.get("/api/ohlc1m/status")
def ohlc1m_status(_: None = Depends(require_token)) -> dict:
    return collector_1m.status()


@app.post("/api/ohlc1m/asset")
def ohlc1m_asset(
    body: OhlcAssetBody, _: None = Depends(require_token)
) -> dict:
    try:
        return collector_1m.set_asset(body.asset)
    except (ValueError, RuntimeError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/ohlc1m/start")
def ohlc1m_start(
    body: OhlcStartBody = OhlcStartBody(),
    _: None = Depends(require_token),
) -> dict:
    try:
        return collector_1m.start(body.asset)
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/ohlc1m/stop")
def ohlc1m_stop(_: None = Depends(require_token)) -> dict:
    return collector_1m.stop()


@app.get("/api/ohlc1m/candles")
def ohlc1m_candles(
    asset: str | None = None,
    timeframe: str = "1m",
    limit: int = 500,
    _: None = Depends(require_token),
) -> dict:
    if timeframe != "1m":
        raise HTTPException(status_code=400, detail="Timeframe suportado: 1m")
    a = normalize_asset(
        asset or collector_1m.status().get("asset") or "EURUSD_otc"
    )
    try:
        rows = fetch_candles(
            a, timeframe=timeframe, limit=limit, table=TABLE_1M
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {
        "asset": a,
        "timeframe": timeframe,
        "count": len(rows),
        "candles": rows,
    }


@app.get("/api/ohlc1m/export")
def ohlc1m_export(
    asset: str | None = None,
    scope: str = Query(default="all"),
    _: None = Depends(require_token),
) -> Response:
    """Download CSV (tudo ou so o que entra na janela de aviso/limpeza)."""
    if scope not in ("all", "at_risk"):
        raise HTTPException(status_code=400, detail="scope: all | at_risk")
    st = collector_1m.status()
    a = normalize_asset(asset or st.get("asset") or "EURUSD_otc")
    before = None
    if scope == "at_risk":
        ret = st.get("retention") or {}
        raw = ret.get("delete_before")
        try:
            delete_before = datetime.fromisoformat(
                str(raw).replace("Z", "+00:00")
            )
            if delete_before.tzinfo is None:
                delete_before = delete_before.replace(tzinfo=timezone.utc)
            before = delete_before + timedelta(days=1)
        except Exception:  # noqa: BLE001
            before = None
    try:
        rows = fetch_candles_range(
            a, timeframe="1m", table=TABLE_1M, before=before
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    csv_text = candles_to_csv(rows)
    fname = f"ohlc_1m_{a}_{scope}.csv"
    return Response(
        content=csv_text,
        media_type="text/csv; charset=utf-8",
        headers={
            "Content-Disposition": f'attachment; filename="{fname}"',
        },
    )


@app.post("/api/ohlc1m/cleanup")
def ohlc1m_cleanup(_: None = Depends(require_token)) -> dict:
    """Forca limpeza de velas >90 dias (tambem roda no loop do coletor)."""
    a = normalize_asset(collector_1m.status().get("asset") or "EURUSD_otc")
    try:
        return run_retention_cleanup_1m(a)
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/ohlc1m/pull-now")
def ohlc1m_pull_now(_: None = Depends(require_token)) -> dict:
    """Puxada manual igual a 1a sync: so velas fechadas, upsert sem duplicar."""
    try:
        return collector_1m.pull_now()
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/api/ohlc1m/resync-recent")
def ohlc1m_resync_recent(
    body: OhlcResyncBody = OhlcResyncBody(),
    _: None = Depends(require_token),
) -> dict:
    """Apaga os ultimos N minutos no Supabase e repuxa da Pocket (upsert)."""
    try:
        return collector_1m.resync_recent(body.minutes)
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc
