#!/usr/bin/env python3
"""
Append daily OHLC/close data for the 4-ETF workbook plus QQQ and SMH signal ETFs.
"""

from __future__ import annotations
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

def find_target_row(ws, target_date):
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
    ws = wb["Daily_Data"]
    bars = {symbol: fetch_last_daily_bar(symbol) for symbol in SYMBOLS}

    target_date = bars["SOXL"]["date"]
    for symbol, bar in bars.items():
        if bar["date"] != target_date:
            raise ValueError(f"Date mismatch: {symbol} returned {bar['date']} but expected {target_date}")

    row = find_target_row(ws, target_date)
    ws[f"A{row}"] = target_date

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

    ws[f"AD{row}"] = bars["QQQ"]["close"]
    ws[f"AJ{row}"] = bars["SMH"]["close"]

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
