"""Importa CSV/Excel de EURUSD diario (ex.: Investing.com PT) → ohlc_candles_eurusd_1d."""

from __future__ import annotations

import csv
import io
import re
import zipfile
import xml.etree.ElementTree as ET
from datetime import date, datetime, timezone
from typing import Any, BinaryIO
from unicodedata import normalize

from bot.ohlc_collector_1d import pocket_midnight_utc, pocket_tz_offset

SOURCE_LABEL = "manual_import"
TIMEFRAME = "1d"

_NS = {"m": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}

_DATE_FORMATS = (
    "%d.%m.%Y",
    "%d/%m/%Y",
    "%Y-%m-%d",
    "%d-%m-%Y",
    "%Y.%m.%d",
    "%m/%d/%Y",
)

_CLOSE_ALIASES = (
    "ultimo",
    "última",
    "ultima",
    "close",
    "price",
    "last",
    "fechamento",
)
_OPEN_ALIASES = ("abertura", "open", "o")
_HIGH_ALIASES = ("maxima", "máxima", "high", "max", "h")
_LOW_ALIASES = ("minima", "mínima", "low", "min", "l")
_VOL_ALIASES = ("vol", "vol.", "volume", "v")
_DATE_ALIASES = ("data", "date", "datetime", "time", "timestamp", "dia")


def _strip_accents(text: str) -> str:
    return "".join(
        c
        for c in normalize("NFKD", text)
        if not (0x300 <= ord(c) <= 0x36F)
    )


def _norm_header(name: str) -> str:
    t = _strip_accents(str(name or "")).strip().lower()
    t = t.replace("%", "").replace(".", " ")
    t = re.sub(r"\s+", "_", t)
    t = t.strip("_")
    return t


def _to_float(raw: Any) -> float | None:
    if raw is None:
        return None
    text = str(raw).strip()
    if not text or text in ("-", "—", "N/A", "n/a"):
        return None
    text = text.replace("%", "").replace(" ", "")
    # 1.152,3 (EU) vs 1,1523 (BR decimal) vs 1,152.30 (US thousands)
    if "," in text and "." in text:
        if text.rfind(",") > text.rfind("."):
            text = text.replace(".", "").replace(",", ".")
        else:
            text = text.replace(",", "")
    elif "," in text:
        parts = text.split(",")
        if len(parts) == 2 and len(parts[1]) <= 6:
            text = text.replace(",", ".")
        else:
            text = text.replace(",", "")
    try:
        return float(text)
    except ValueError:
        return None


def _parse_day(raw: Any) -> date | None:
    if raw is None:
        return None
    if isinstance(raw, datetime):
        return raw.date()
    if isinstance(raw, date):
        return raw
    text = str(raw).strip().strip('"').strip("'")
    if not text:
        return None
    # Excel serial date as number string
    try:
        as_num = float(text.replace(",", "."))
        if 20000 < as_num < 80000 and "." not in text[:4]:
            # days since 1899-12-30 (Excel)
            from datetime import timedelta

            base = date(1899, 12, 30)
            return base + timedelta(days=int(as_num))
    except ValueError:
        pass
    if "T" in text or " " in text:
        try:
            dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
            return dt.date()
        except ValueError:
            text = text.split(" ")[0].split("T")[0]
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def _pick(row: dict[str, str], aliases: tuple[str, ...]) -> str | None:
    for key in aliases:
        nk = _norm_header(key)
        if nk in row and row[nk] not in (None, ""):
            return row[nk]
        # exact alias already normalized in row keys
        if key in row and row[key] not in (None, ""):
            return row[key]
    for alias in aliases:
        want = _norm_header(alias)
        for k, v in row.items():
            if _norm_header(k) == want and v not in (None, ""):
                return v
    return None


def _detect_delimiter(sample: str) -> str:
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",;\t")
        return dialect.delimiter
    except csv.Error:
        if sample.count(";") > sample.count(","):
            return ";"
        if "\t" in sample:
            return "\t"
        return ","


def _rows_from_matrix(matrix: list[list[str]]) -> list[dict[str, str]]:
    if not matrix:
        return []
    # Caso Excel abriu CSV numa unica coluna: cada celula e uma linha CSV.
    if all(len(r) <= 1 for r in matrix):
        lines = [(r[0] if r else "").strip() for r in matrix]
        lines = [ln for ln in lines if ln]
        if not lines:
            return []
        text = "\n".join(lines)
        return _rows_from_csv_text(text)

    header = [_norm_header(c) for c in matrix[0]]
    out: list[dict[str, str]] = []
    for cells in matrix[1:]:
        if not any(str(c).strip() for c in cells):
            continue
        row: dict[str, str] = {}
        for i, key in enumerate(header):
            if not key:
                continue
            row[key] = str(cells[i]).strip() if i < len(cells) else ""
        out.append(row)
    return out


def _rows_from_csv_text(text: str) -> list[dict[str, str]]:
    text = text.lstrip("\ufeff").strip()
    if not text:
        return []
    delimiter = _detect_delimiter(text[:4096])
    reader = csv.DictReader(io.StringIO(text), delimiter=delimiter)
    if not reader.fieldnames:
        return []
    out: list[dict[str, str]] = []
    for raw in reader:
        row = {
            _norm_header(k): (v or "").strip()
            for k, v in raw.items()
            if k is not None
        }
        if any(row.values()):
            out.append(row)
    return out


def _col_letter_to_index(cell_ref: str) -> int:
    m = re.match(r"([A-Z]+)", cell_ref.upper())
    if not m:
        return 0
    letters = m.group(1)
    n = 0
    for ch in letters:
        n = n * 26 + (ord(ch) - 64)
    return n - 1


def _read_xlsx_matrix(data: bytes) -> list[list[str]]:
    with zipfile.ZipFile(io.BytesIO(data)) as zf:
        shared: list[str] = []
        if "xl/sharedStrings.xml" in zf.namelist():
            root = ET.fromstring(zf.read("xl/sharedStrings.xml"))
            for si in root.findall("m:si", _NS):
                texts = [t.text or "" for t in si.findall(".//m:t", _NS)]
                shared.append("".join(texts))

        sheet_name = None
        for name in zf.namelist():
            if name.startswith("xl/worksheets/sheet") and name.endswith(".xml"):
                sheet_name = name
                break
        if not sheet_name:
            raise ValueError("Excel sem planilha legivel.")

        root = ET.fromstring(zf.read(sheet_name))
        rows_out: list[list[str]] = []
        for row in root.findall("m:sheetData/m:row", _NS):
            cells: dict[int, str] = {}
            max_idx = -1
            for c in row.findall("m:c", _NS):
                ref = c.get("r") or ""
                idx = _col_letter_to_index(ref)
                max_idx = max(max_idx, idx)
                cell_type = c.get("t")
                v_el = c.find("m:v", _NS)
                if v_el is None or v_el.text is None:
                    val = ""
                elif cell_type == "s":
                    try:
                        val = shared[int(v_el.text)]
                    except (ValueError, IndexError):
                        val = v_el.text
                else:
                    val = v_el.text
                cells[idx] = val
            if max_idx < 0:
                continue
            line = [cells.get(i, "") for i in range(max_idx + 1)]
            rows_out.append(line)
        return rows_out


def parse_eurusd_daily_table(
    rows: list[dict[str, str]],
    *,
    asset: str = "EURUSD",
    source_label: str = SOURCE_LABEL,
    pocket_offset: int | None = None,
) -> list[dict[str, Any]]:
    """Converte linhas ja normalizadas em candles D1 alinhados ao dia Pocket."""
    off = pocket_tz_offset() if pocket_offset is None else pocket_offset
    now_iso = datetime.now(timezone.utc).isoformat()
    out: list[dict[str, Any]] = []
    seen: set[str] = set()

    for line_no, row in enumerate(rows, start=2):
        day_raw = _pick(row, _DATE_ALIASES)
        day = _parse_day(day_raw)
        if day is None:
            continue

        # Investing: Ultimo=close, Abertura=open, Maxima=high, Minima=low
        c = _to_float(_pick(row, _CLOSE_ALIASES))
        o = _to_float(_pick(row, _OPEN_ALIASES))
        h = _to_float(_pick(row, _HIGH_ALIASES))
        lo = _to_float(_pick(row, _LOW_ALIASES))
        if None in (o, h, lo, c):
            # se so tem close, preenche OHLC flat
            if c is not None and o is None and h is None and lo is None:
                o = h = lo = c
            else:
                raise ValueError(
                    f"Linha {line_no}: OHLC incompleto (data={day_raw!r})"
                )

        assert o is not None and h is not None and lo is not None and c is not None
        h = max(h, o, c)
        lo = min(lo, o, c)
        opened = pocket_midnight_utc(day, offset=off)
        key = opened.strftime("%Y-%m-%dT%H:%M:%S")
        if key in seen:
            continue
        seen.add(key)

        item: dict[str, Any] = {
            "asset": asset,
            "timeframe": TIMEFRAME,
            "opened_at": opened.isoformat(),
            "open": o,
            "high": h,
            "low": lo,
            "close": c,
            "source": source_label,
            "updated_at": now_iso,
        }
        vol = _to_float(_pick(row, _VOL_ALIASES))
        if vol is not None:
            item["volume"] = vol
        out.append(item)

    out.sort(key=lambda r: r["opened_at"])
    return out


def parse_eurusd_daily_bytes(
    data: bytes,
    *,
    filename: str = "",
    asset: str = "EURUSD",
    source_label: str = SOURCE_LABEL,
) -> list[dict[str, Any]]:
    """Detecta CSV ou XLSX e devolve candles prontos para upsert."""
    name = (filename or "").lower()
    if name.endswith(".xlsx") or data[:2] == b"PK":
        matrix = _read_xlsx_matrix(data)
        rows = _rows_from_matrix(matrix)
    else:
        # tenta utf-8 / latin-1
        text = None
        for enc in ("utf-8-sig", "utf-8", "cp1252", "latin-1"):
            try:
                text = data.decode(enc)
                break
            except UnicodeDecodeError:
                continue
        if text is None:
            raise ValueError("Encoding do ficheiro nao reconhecido.")
        rows = _rows_from_csv_text(text)

    if not rows:
        raise ValueError("Ficheiro sem linhas de dados.")

    candles = parse_eurusd_daily_table(
        rows, asset=asset, source_label=source_label
    )
    if not candles:
        raise ValueError(
            "Nenhuma vela parseada. Esperado cabecalho: "
            "Data, Ultimo, Abertura, Maxima, Minima."
        )
    return candles


def parse_eurusd_daily_file(
    source: str | BinaryIO,
    *,
    filename: str = "",
    asset: str = "EURUSD",
) -> list[dict[str, Any]]:
    if isinstance(source, str):
        with open(source, "rb") as fh:
            return parse_eurusd_daily_bytes(
                fh.read(), filename=filename or source, asset=asset
            )
    data = source.read()
    return parse_eurusd_daily_bytes(data, filename=filename, asset=asset)
