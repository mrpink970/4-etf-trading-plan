import yfinance as yf
import pandas as pd
from openpyxl import load_workbook

FILE = "4_ETF_Trading_Workbook_Template.xlsx"

TICKERS = ["SOXL", "SOXS", "TQQQ", "SQQQ", "QQQ", "SMH"]

def get_data(ticker):
    data = yf.download(ticker, period="5d", interval="1d", progress=False)

    if data.empty:
        return None

    # Fix multi-index issue
    if hasattr(data.columns, "levels"):
        data.columns = data.columns.get_level_values(0)

    last = data.iloc[-1]

    return {
        "open": float(last["Open"]),
        "high": float(last["High"]),
        "low": float(last["Low"]),
        "close": float(last["Close"]),
    }

def main():
    wb = load_workbook(FILE)
    ws = wb["Daily_Data"]

    # Find next empty row
    row = ws.max_row + 1

    # Date
    ws[f"A{row}"] = pd.Timestamp.today().date()

    col_map = {
        "SOXL": ("B", "C", "D", "E"),
        "SOXS": ("I", "J", "K", "L"),
        "TQQQ": ("P", "Q", "R", "S"),
        "SQQQ": ("W", "X", "Y", "Z"),
        "QQQ":  ("AD", None, None, None),
        "SMH":  ("AH", None, None, None),
    }

    for ticker in TICKERS:
        d = get_data(ticker)
        if d is None:
            continue

        cols = col_map[ticker]

        ws[f"{cols[0]}{row}"] = d["open"]
        if cols[1]:
            ws[f"{cols[1]}{row}"] = d["high"]
        if cols[2]:
            ws[f"{cols[2]}{row}"] = d["low"]
        if cols[3]:
            ws[f"{cols[3]}{row}"] = d["close"]

    wb.save(FILE)

if __name__ == "__main__":
    main()
