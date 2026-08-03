"""Baixa OlympTradeAPI (zip) e instala em modo editavel (sem git no PATH)."""

from __future__ import annotations

import io
import subprocess
import sys
import urllib.request
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
VENDOR = ROOT / "vendor"
URL = "https://github.com/ChipaDevTeam/OlympTradeAPI/archive/refs/heads/main.zip"
PYPROJECT = """[build-system]
requires = ["setuptools>=61"]
build-backend = "setuptools.build_meta"

[project]
name = "olymptrade-ws"
version = "0.1.0"
dependencies = ["websockets", "aiohttp"]

[tool.setuptools.packages.find]
include = ["olymptrade_ws*"]
"""


def main() -> int:
    VENDOR.mkdir(exist_ok=True)
    print("downloading", URL)
    data = urllib.request.urlopen(URL, timeout=120).read()
    with zipfile.ZipFile(io.BytesIO(data)) as zf:
        zf.extractall(VENDOR)
    src = next(VENDOR.glob("OlympTradeAPI-*"))
    print("extracted", src)
    if not (src / "pyproject.toml").exists() and not (src / "setup.py").exists():
        (src / "pyproject.toml").write_text(PYPROJECT, encoding="utf-8")
    # Dependencias comuns da lib
    subprocess.check_call(
        [sys.executable, "-m", "pip", "install", "websockets", "aiohttp", "-q"]
    )
    r = subprocess.run(
        [sys.executable, "-m", "pip", "install", "-e", str(src)],
        check=False,
    )
    if r.returncode != 0:
        return r.returncode
    probe = subprocess.run(
        [
            sys.executable,
            "-c",
            "from olymptrade_ws import OlympTradeClient; print('OK', OlympTradeClient)",
        ],
        check=False,
    )
    return probe.returncode


if __name__ == "__main__":
    raise SystemExit(main())
