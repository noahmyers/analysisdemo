"""Basic Excel (XLSX) analysis workflow for JE samples.

This script avoids external dependencies by parsing the XLSX XML files directly.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timedelta
import statistics
import zipfile
from xml.etree import ElementTree as ET

XLSX_PATH = "je_samples.xlsx"
OUTPUT_DIR = "analysis_outputs"

NS = {"main": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}

DATE_NUMFMT_IDS = {14, 15, 16, 17, 22, 45, 46, 47}


def _col_letter_to_index(letter: str) -> int:
    total = 0
    for char in letter:
        total = total * 26 + (ord(char.upper()) - ord("A") + 1)
    return total


def _excel_date(value: float) -> datetime:
    return datetime(1899, 12, 30) + timedelta(days=float(value))


def _load_shared_strings(zf: zipfile.ZipFile) -> list[str]:
    shared = []
    if "xl/sharedStrings.xml" not in zf.namelist():
        return shared
    sst = ET.fromstring(zf.read("xl/sharedStrings.xml"))
    for si in sst.findall("main:si", NS):
        texts = []
        for t in si.findall(".//main:t", NS):
            texts.append(t.text or "")
        shared.append("".join(texts))
    return shared


def _load_numfmt_map(zf: zipfile.ZipFile) -> tuple[list[int], dict[int, str]]:
    styles = ET.fromstring(zf.read("xl/styles.xml"))
    num_fmts = styles.find("main:numFmts", NS)
    custom_formats: dict[int, str] = {}
    if num_fmts is not None:
        for fmt in num_fmts.findall("main:numFmt", NS):
            custom_formats[int(fmt.attrib["numFmtId"])] = fmt.attrib["formatCode"]

    cell_xfs = styles.find("main:cellXfs", NS)
    xf_numfmts = [
        int(xf.attrib.get("numFmtId", "0"))
        for xf in cell_xfs.findall("main:xf", NS)
    ]
    return xf_numfmts, custom_formats


def _load_sheet_paths(zf: zipfile.ZipFile) -> list[tuple[str, str]]:
    wb = ET.fromstring(zf.read("xl/workbook.xml"))
    rels = ET.fromstring(zf.read("xl/_rels/workbook.xml.rels"))
    relmap = {rel.attrib["Id"]: rel.attrib["Target"] for rel in rels}
    sheets = []
    for sheet in wb.findall("main:sheets/main:sheet", NS):
        name = sheet.attrib["name"]
        rid = sheet.attrib[
            "{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id"
        ]
        sheets.append((name, f"xl/{relmap[rid]}"))
    return sheets


def _is_date_format(numfmt_id: int, custom_formats: dict[int, str]) -> bool:
    if numfmt_id in DATE_NUMFMT_IDS:
        return True
    fmt = custom_formats.get(numfmt_id)
    if not fmt:
        return False
    lowered = fmt.lower()
    return "y" in lowered and "d" in lowered


def _parse_sheet(
    zf: zipfile.ZipFile,
    sheet_path: str,
    shared_strings: list[str],
    xf_numfmts: list[int],
    custom_formats: dict[int, str],
) -> list[dict[str, object]]:
    root = ET.fromstring(zf.read(sheet_path))
    rows = []
    for row in root.findall("main:sheetData/main:row", NS):
        row_data: dict[str, object] = {}
        for cell in row.findall("main:c", NS):
            ref = cell.attrib["r"]
            col = "".join(char for char in ref if char.isalpha())
            value_node = cell.find("main:v", NS)
            value: object | None
            if value_node is None:
                value = None
            else:
                value = value_node.text
            cell_type = cell.attrib.get("t")
            style = cell.attrib.get("s")
            if value is not None:
                if cell_type == "s":
                    value = shared_strings[int(value)]
                elif cell_type == "b":
                    value = value == "1"
                else:
                    try:
                        value = float(value) if "." in value else int(value)
                    except ValueError:
                        pass
                if style is not None:
                    numfmt_id = xf_numfmts[int(style)]
                    if _is_date_format(numfmt_id, custom_formats):
                        if isinstance(value, (int, float)):
                            value = _excel_date(float(value))
            if value == "":
                value = None
            row_data[col] = value
        rows.append(row_data)
    return rows


def _rows_to_records(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    if not rows:
        return []
    header_row = rows[0]
    headers = {
        col: header_row.get(col)
        for col in sorted(header_row, key=_col_letter_to_index)
    }
    records = []
    for row in rows[1:]:
        record = {}
        for col, header in headers.items():
            if header is None:
                continue
            record[str(header)] = row.get(col)
        records.append(record)
    return records


def _infer_column_types(records: list[dict[str, object]]) -> dict[str, str]:
    types: dict[str, str] = {}
    for col in records[0].keys():
        values = [r.get(col) for r in records if r.get(col) is not None]
        if not values:
            types[col] = "empty"
            continue
        if all(isinstance(v, datetime) for v in values):
            types[col] = "date"
        elif all(isinstance(v, (int, float)) for v in values):
            types[col] = "numeric"
        else:
            types[col] = "text"
    return types


def _summarize_numeric(values: list[float]) -> dict[str, float]:
    if not values:
        return {}
    return {
        "count": len(values),
        "min": min(values),
        "max": max(values),
        "mean": statistics.mean(values),
        "sum": sum(values),
    }


def _ensure_output_dir() -> None:
    import os

    os.makedirs(OUTPUT_DIR, exist_ok=True)


def _write_summary(records: list[dict[str, object]]) -> str:
    types = _infer_column_types(records)
    total_rows = len(records)
    columns = list(records[0].keys())
    missing = {col: sum(1 for r in records if r.get(col) is None) for col in columns}

    lines = []
    lines.append("JE Samples Basic Analysis Summary")
    lines.append("=" * 36)
    lines.append(f"Total rows (excluding header): {total_rows}")
    lines.append(f"Total columns: {len(columns)}")
    lines.append("")
    lines.append("Column overview:")
    for col in columns:
        lines.append(
            f"- {col}: type={types[col]}, missing={missing[col]} ({missing[col] / total_rows:.1%})"
        )

    lines.append("")
    lines.append("Date ranges:")
    for col in columns:
        if types[col] == "date":
            values = [r[col] for r in records if isinstance(r.get(col), datetime)]
            if values:
                lines.append(f"- {col}: {min(values).date()} to {max(values).date()}")

    lines.append("")
    lines.append("Numeric summaries:")
    for col in columns:
        if types[col] == "numeric":
            values = [r[col] for r in records if isinstance(r.get(col), (int, float))]
            summary = _summarize_numeric(values)
            if summary:
                lines.append(
                    f"- {col}: count={summary['count']}, min={summary['min']:.2f}, "
                    f"max={summary['max']:.2f}, mean={summary['mean']:.2f}, sum={summary['sum']:.2f}"
                )

    lines.append("")
    lines.append("Top categories (count):")
    for col in columns:
        if types[col] == "text" and col not in {"JEDescription"}:
            values = [r[col] for r in records if isinstance(r.get(col), str)]
            if not values:
                continue
            counts = Counter(values).most_common(5)
            lines.append(f"- {col}:")
            for value, count in counts:
                lines.append(f"  - {value}: {count}")

    summary_path = f"{OUTPUT_DIR}/summary.txt"
    with open(summary_path, "w", encoding="utf-8") as handle:
        handle.write("\n".join(lines) + "\n")
    return summary_path


def _build_bar_chart_svg(
    data: list[tuple[str, float]],
    title: str,
    x_label: str,
    y_label: str,
    width: int = 900,
    height: int = 500,
) -> str:
    padding = 60
    chart_width = width - padding * 2
    chart_height = height - padding * 2

    max_value = max(value for _, value in data) if data else 1
    bar_width = chart_width / max(len(data), 1)

    svg_parts = [
        f"<svg xmlns='http://www.w3.org/2000/svg' width='{width}' height='{height}'>",
        f"<rect width='100%' height='100%' fill='white' />",
        f"<text x='{width / 2}' y='30' font-size='18' text-anchor='middle'>{title}</text>",
        f"<text x='{width / 2}' y='{height - 10}' font-size='12' text-anchor='middle'>{x_label}</text>",
        f"<text transform='translate(15,{height / 2}) rotate(-90)' font-size='12' text-anchor='middle'>{y_label}</text>",
        f"<line x1='{padding}' y1='{padding}' x2='{padding}' y2='{height - padding}' stroke='black' />",
        f"<line x1='{padding}' y1='{height - padding}' x2='{width - padding}' y2='{height - padding}' stroke='black' />",
    ]

    for idx, (label, value) in enumerate(data):
        bar_height = (value / max_value) * chart_height if max_value else 0
        x = padding + idx * bar_width
        y = height - padding - bar_height
        svg_parts.append(
            f"<rect x='{x + 5}' y='{y}' width='{bar_width - 10}' height='{bar_height}' fill='#4C78A8' />"
        )
        svg_parts.append(
            f"<text x='{x + bar_width / 2}' y='{height - padding + 15}' font-size='10' "
            f"text-anchor='middle' transform='rotate(45 {x + bar_width / 2},{height - padding + 15})'>{label}</text>"
        )
        svg_parts.append(
            f"<text x='{x + bar_width / 2}' y='{y - 5}' font-size='10' text-anchor='middle'>{value:,.0f}</text>"
        )

    svg_parts.append("</svg>")
    return "\n".join(svg_parts)


def _write_svg(path: str, svg: str) -> None:
    with open(path, "w", encoding="utf-8") as handle:
        handle.write(svg)


def _safe_float(value: object) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    return None


def _select_amount_key(records: list[dict[str, object]]) -> str:
    for candidate in ("AbsoluteAmount", "Amount"):
        values = [r.get(candidate) for r in records if r.get(candidate) is not None]
        if any(isinstance(value, (int, float)) for value in values):
            return candidate
    return "Amount"


def main() -> None:
    _ensure_output_dir()
    with zipfile.ZipFile(XLSX_PATH) as zf:
        shared = _load_shared_strings(zf)
        xf_numfmts, custom_formats = _load_numfmt_map(zf)
        sheets = _load_sheet_paths(zf)
        if not sheets:
            raise RuntimeError("No sheets found in workbook")
        sheet_name, sheet_path = sheets[0]
        rows = _parse_sheet(zf, sheet_path, shared, xf_numfmts, custom_formats)

    records = _rows_to_records(rows)
    if not records:
        raise RuntimeError("No records parsed from sheet")

    summary_path = _write_summary(records)

    # Chart: top 10 GLAccountName by AbsoluteAmount sum
    account_totals = defaultdict(float)
    amount_key = _select_amount_key(records)
    for record in records:
        account = record.get("GLAccountName") or "(Missing)"
        value = _safe_float(record.get(amount_key))
        if value is not None:
            account_totals[str(account)] += abs(value)
    top_accounts = sorted(account_totals.items(), key=lambda x: x[1], reverse=True)[:10]

    account_svg = _build_bar_chart_svg(
        top_accounts,
        title=f"Top 10 GLAccountName by Total {amount_key}",
        x_label="GLAccountName",
        y_label=f"Total {amount_key}",
    )
    account_chart_path = f"{OUTPUT_DIR}/top_accounts.svg"
    _write_svg(account_chart_path, account_svg)

    # Chart: totals by period
    period_totals = defaultdict(float)
    for record in records:
        period = record.get("Period")
        if period is None:
            date_value = record.get("EffectiveDate")
            if isinstance(date_value, datetime):
                period = date_value.strftime("%Y-%m")
        if period is None:
            continue
        value = _safe_float(record.get(amount_key))
        if value is not None:
            period_totals[str(period)] += value
    sorted_periods = sorted(period_totals.items())
    period_svg = _build_bar_chart_svg(
        sorted_periods,
        title=f"Total {amount_key} by Period",
        x_label="Period",
        y_label=f"Total {amount_key}",
    )
    period_chart_path = f"{OUTPUT_DIR}/period_totals.svg"
    _write_svg(period_chart_path, period_svg)

    print("Analysis complete.")
    print(f"Summary: {summary_path}")
    print(f"Charts: {account_chart_path}, {period_chart_path}")
    print(f"Sheet analyzed: {sheet_name}")


if __name__ == "__main__":
    main()
