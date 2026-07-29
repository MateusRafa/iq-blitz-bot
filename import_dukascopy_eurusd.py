"""Importa CSV Dukascopy (1h, UTC) para ohlc_candles_eurusd no Supabase.

Uso (PowerShell):
  $env:SUPABASE_URL = "https://....supabase.co"
  $env:SUPABASE_SERVICE_ROLE_KEY = "..."
  python import_dukascopy_eurusd.py C:\\Downloads\\EURUSD_H1.csv

Opcoes uteis:
  --dry-run          so mostra resumo, nao grava
  --asset EURUSD     asset na tabela (default: EURUSD)
"""

from __future__ import annotations

import argparse
import os
import sys

from bot.dukascopy_import import parse_dukascopy_csv
from bot.ohlc_store import TABLE_EURUSD, upsert_candles
from bot.runner import normalize_asset


def _load_dotenv() -> None:
    try:
        from dotenv import load_dotenv
    except ImportError:
        return
    load_dotenv()


def main(argv: list[str] | None = None) -> int:
    _load_dotenv()
    parser = argparse.ArgumentParser(
        description="Importa CSV Dukascopy 1h para ohlc_candles_eurusd."
    )
    parser.add_argument("csv_path", help="Caminho do CSV exportado pelo Dukascopy")
    parser.add_argument(
        "--asset",
        default=os.environ.get("OHLC_EURUSD_ASSET", "EURUSD"),
        help="Asset gravado na tabela (default: EURUSD ou OHLC_EURUSD_ASSET)",
    )
    parser.add_argument(
        "--source",
        default="dukascopy",
        help="Valor da coluna source (default: dukascopy)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Apenas parseia e mostra resumo, sem upsert",
    )
    args = parser.parse_args(argv)

    asset = normalize_asset(args.asset)
    try:
        rows = parse_dukascopy_csv(
            args.csv_path,
            asset=asset,
            timeframe="1h",
            source_label=args.source,
            assume_utc=True,
        )
    except OSError as exc:
        print(f"Erro ao ler arquivo: {exc}", file=sys.stderr)
        return 1
    except ValueError as exc:
        print(f"Erro no CSV: {exc}", file=sys.stderr)
        return 1

    if not rows:
        print("Nenhuma vela valida encontrada no CSV.", file=sys.stderr)
        return 1

    first = rows[0]["opened_at"]
    last = rows[-1]["opened_at"]
    print(f"Velas parseadas: {len(rows)}")
    print(f"Intervalo: {first} -> {last}")
    print(f"Asset: {asset} | timeframe: 1h | source: {args.source}")

    if args.dry_run:
        print("Dry-run: nada foi gravado.")
        return 0

    try:
        n = upsert_candles(rows, table=TABLE_EURUSD)
    except RuntimeError as exc:
        print(f"Erro Supabase: {exc}", file=sys.stderr)
        return 1

    print(f"Upsert concluido: {n} linhas enviadas para {TABLE_EURUSD}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
