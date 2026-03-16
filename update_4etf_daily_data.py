#!/usr/bin/env python3
"""
Safe daily updater for the 4-ETF workbook plus QQQ and SMH signal ETFs.

Safety improvements:
- writes to a temporary workbook first
- keeps a backup copy before replacing the original
- updates an existing row if the date already exists
- only replaces the original workbook after a successful save
"""

from __future__ import annotations

import shutil
import sys
from pathlib import Path
import openpyxl

try:
    import yfinance as yf
except ImportError:
    print("Missing package: yfinance")
    print("Install with: pip install yfinance openpyxl")
    raise

SYMBOLS = ["SOXL", "SOXS", "TQQQ", "SQQQ", "QQQ", "SMH"]


def fetch_last_daily_bar(symbol: str) -> dict:
    ticker = yf.Ticker(symbol)
    data = ticker.history(period="10d", interval="1d", auto_adjust=False)
    if data.empty:
        raise ValueError(f"No data returned for {symbol}")

    last = data.iloc[-1]
    idx = data.index[-1].to_pydatetime()

    return {
        "date": idx.date(),
        "open": float(last["Open"]),
        "high": float(last["High"]),
        "low": float(last["Low"]),
        "close": float(last["Close"]),
    }


def normalize_excel_date(value):
    if value is None:
        return None
    if hasattr(value, "date"):
        return value.date()
    return value


def find_target_row(ws, target_date):
    """
    If target_date already exists, return that row.
    Otherwise return the next empty row starting at row 4.
    """
    row = 4
    while True:
        value = ws[f"A{row}"].value
        if value is None:
            return row

        value_date = normalize_excel_date(value)
        if value_date == target_date:
            return row

        row += 1


def write_row(ws, row: int, bars: dict) -> None:
    ws[f"A{row}"] = bars["SOXL"]["date"]

    # Trading ETFs
    ws[f"B{row}"] = bars["SOXL"]["open"]
    ws[f"C{row}"] = bars["SOXL"]["high"]
    ws[f"D{row}"] = bars["SOXL"]["low"]
    ws[f"E{row}"] = bars["SOXL"]["close"]
    ws[f"G{row}"] = bars["SOXS"]["close"]

    ws[f"I{row}"] = bars["TQQQ"]["open"]
    ws[f"J{row}"] = bars["TQQQ"]["high"]
    ws[f"K{row}"] = bars["TQQQ"]["low"]
    ws[f"L{row}"] = bars["TQQQ"]["close"]
    ws[f"N{row}"] = bars["SQQQ"]["close"]

    # Signal ETFs
    # Workbook layout currently expects:
    # AD = QQQ Close
    # AJ = SMH Close
    ws[f"AD{row}"] = bars["QQQ"]["close"]
    ws[f"AJ{row}"] = bars["SMH"]["close"]


def update_workbook_safe(workbook_path: Path) -> Path:
    if not workbook_path.exists():
        raise FileNotFoundError(f"Workbook not found: {workbook_path}")

    temp_path = workbook_path.with_suffix(".tmp.xlsx")
    backup_path = workbook_path.with_suffix(".bak.xlsx")

    # Clean up stale temp file from a previous failed run
    if temp_path.exists():
        temp_path.unlink()

    # Keep a backup of the current workbook before editing
    shutil.copy2(workbook_path, backup_path)
    shutil.copy2(workbook_path, temp_path)

    try:
        wb = openpyxl.load_workbook(temp_path)
        if "Daily_Data" not in wb.sheetnames:
            raise ValueError("Workbook does not contain a 'Daily_Data' sheet")

        ws = wb["Daily_Data"]
        bars = {symbol: fetch_last_daily_bar(symbol) for symbol in SYMBOLS}

        target_date = bars["SOXL"]["date"]
        for symbol, bar in bars.items():
            if bar["date"] != target_date:
                raise ValueError(
                    f"Date mismatch: {symbol} returned {bar['date']} but expected {target_date}"
                )

        row = find_target_row(ws, target_date)
        write_row(ws, row, bars)

        wb.save(temp_path)

        # Replace original only after the temp workbook saved successfully
        shutil.move(str(temp_path), str(workbook_path))
        return workbook_path

    except Exception:
        # If anything goes wrong, remove temp and keep original untouched
        if temp_path.exists():
            temp_path.unlink()
        raise


def main():
    if len(sys.argv) < 2:
        print("Usage: python update_4etf_daily_data.py /path/to/4_ETF_Trading_Workbook_Template.xlsx")
        sys.exit(1)

    workbook_path = Path(sys.argv[1]).expanduser().resolve()

    try:
        updated = update_workbook_safe(workbook_path)
        print(f"Updated workbook safely: {updated}")
        print(f"Backup file kept at: {updated.with_suffix('.bak.xlsx')}")
    except Exception as exc:
        print(f"Update failed: {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()
