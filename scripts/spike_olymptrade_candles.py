"""Spike: testa login Olymp + get_candles EURUSD 1h.

Uso:
  set OLYMPTRADE_ACCESS_TOKEN=...
  python scripts/spike_olymptrade_candles.py

Opcional:
  set OLYMPTRADE_PAIR=EURUSD
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from bot.olymptrade_fetch import (  # noqa: E402
    default_pair,
    fetch_candles,
    olymptrade_available,
    rows_for_store,
)


def main() -> int:
    ok, msg = olymptrade_available()
    print("available:", ok, msg)
    if not ok:
        return 1
    pair = default_pair()
    print(f"pair={pair} size=3600 count=24")
    try:
        raw = fetch_candles(pair, size=3600, count=24)
    except Exception as exc:  # noqa: BLE001
        print("ERRO:", exc)
        return 2
    print(f"raw_count={len(raw)}")
    if raw:
        print("sample_raw:", json.dumps(raw[0], ensure_ascii=False, default=str)[:500])
    rows = rows_for_store(raw, timeframe="1h", pair=pair)
    print(f"normalized={len(rows)}")
    if rows:
        print("first:", rows[0])
        print("last:", rows[-1])
    out = ROOT / "tmp_olymp_spike.json"
    out.write_text(
        json.dumps({"raw": raw[:5], "rows": rows[:5]}, indent=2, default=str),
        encoding="utf-8",
    )
    print("wrote", out)
    return 0 if rows else 3


if __name__ == "__main__":
    raise SystemExit(main())
