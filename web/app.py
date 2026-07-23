"""API + portal web do IQ Blitz Bot."""

from __future__ import annotations

import os
import secrets
from pathlib import Path

from fastapi import Depends, FastAPI, Header, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from bot.runner import MIN_DURATION, runner

STATIC = Path(__file__).resolve().parent / "static"

app = FastAPI(title="IQ Blitz Bot — Portal", version="1.0.0")
app.mount("/static", StaticFiles(directory=str(STATIC)), name="static")


class DurationBody(BaseModel):
    """Duracao da 1a ordem: seconds OU minutes (um dos dois)."""

    seconds: int | None = Field(default=None, ge=MIN_DURATION)
    minutes: float | None = Field(default=None, gt=0)


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


@app.get("/api/health")
def health() -> dict:
    return {
        "ok": True,
        "bot_running": runner.is_running(),
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
