#!/usr/bin/env python3
"""
Append daily OHLC data for the 4-ETF system into the Excel workbook.

Default symbols:
- SOXL
- SOXS
- TQQQ
- SQQQ

Expected workbook sheet:
- Daily_Data

Expected columns in Daily_Data:
A  Date
B  SOXL Open
C  SOXL High
D  SOXL Low
E  SOXL Close
F  SOXL Daily %
G  SOXS Close
H  SOXS Daily %
I  TQQQ Open
J  TQQQ High
K  TQQQ Low
L  TQQQ Close
M  TQQQ Daily %
N  SQQQ Close
O  SQQQ Daily %

This script only writes the raw daily values.
Percentages are left to spreadsheet formulas.
"""

from __future__ import annotations

import sys
from pathlib import Path
from datetime import datetime

import openpyxl

try:
    import yfinance as yf
except ImportError:
    print("Missing package: yfinance")
    print("Install with: pip install yfinance openpyxl")
    raise

SYMBOLS = ["SOXL", "SOXS", "TQQQ", "SQQQ"]


def fetch_last_daily_bar(symbol: str) -> dict:
    """
    Fetch the most recent daily bar for a symbol.
    """
    ticker = yf.Ticker(symbol)
    data = ticker.history(period="7d", interval="1d", auto_adjust=False)

    if data.empty:
        raise ValueError(f"No data returned for {symbol}")

    last = data.iloc[-1]
    idx = data.index[-1].to_pydatetime()

    row = {
        "date": idx.date(),
        "open": float(last["Open"]),
        "high": float(last["High"]),
        "low": float(last["Low"]),
        "close": float(last["Close"]),
    }
    return row


def find_target_row(ws, target_date):
    """
    Return the row for target_date if it already exists in column A.
    Otherwise return the next empty row.
    """
    row = 4
    while True:
        value = ws[f"A{row}"].value
        if value is None:
            return row

        if hasattr(value, "date"):
            value_date = value.date()
        else:
            value_date = value

        if value_date == target_date:
            return row

        row += 1


def update_workbook(workbook_path: Path) -> Path:
    wb = openpyxl.load_workbook(workbook_path)
    if "Daily_Data" not in wb.sheetnames:
        raise ValueError("Workbook does not contain a 'Daily_Data' sheet")

    ws = wb["Daily_Data"]

    bars = {symbol: fetch_last_daily_bar(symbol) for symbol in SYMBOLS}

    # Use SOXL date as the row date anchor.
    target_date = bars["SOXL"]["date"]

    # Basic date consistency check.
    for symbol, bar in bars.items():
        if bar["date"] != target_date:
            raise ValueError(
                f"Date mismatch: {symbol} returned {bar['date']} but expected {target_date}"
            )

    row = find_target_row(ws, target_date)

    # Write date
    ws[f"A{row}"] = target_date

    # SOXL OHLC + close
    ws[f"B{row}"] = bars["SOXL"]["open"]
    ws[f"C{row}"] = bars["SOXL"]["high"]
    ws[f"D{row}"] = bars["SOXL"]["low"]
    ws[f"E{row}"] = bars["SOXL"]["close"]

    # SOXS close
    ws[f"G{row}"] = bars["SOXS"]["close"]

    # TQQQ OHLC + close
    ws[f"I{row}"] = bars["TQQQ"]["open"]
    ws[f"J{row}"] = bars["TQQQ"]["high"]
    ws[f"K{row}"] = bars["TQQQ"]["low"]
    ws[f"L{row}"] = bars["TQQQ"]["close"]

    # SQQQ close
    ws[f"N{row}"] = bars["SQQQ"]["close"]

    # Preserve formulas if they aren't already present.
    if row >= 5:
        # Daily % columns
        if ws[f"F{row}"].value in (None, ""):
            ws[f"F{row}"] = f'=IFERROR((E{row}/E{row-1})-1,"")'
        if ws[f"H{row}"].value in (None, ""):
            ws[f"H{row}"] = f'=IFERROR((G{row}/G{row-1})-1,"")'
        if ws[f"M{row}"].value in (None, ""):
            ws[f"M{row}"] = f'=IFERROR((L{row}/L{row-1})-1,"")'
        if ws[f"O{row}"].value in (None, ""):
            ws[f"O{row}"] = f'=IFERROR((N{row}/N{row-1})-1,"")'

    wb.save(workbook_path)
    return workbook_path


def main():
    if len(sys.argv) < 2:
        print("Usage: python update_4etf_daily_data.py /path/to/4_ETF_Trading_Workbook_Template.xlsx")
        sys.exit(1)

    workbook_path = Path(sys.argv[1]).expanduser().resolve()

    if not workbook_path.exists():
        print(f"Workbook not found: {workbook_path}")
        sys.exit(1)

    updated = update_workbook(workbook_path)
    print(f"Updated workbook: {updated}")


if __name__ == "__main__":
    main()
