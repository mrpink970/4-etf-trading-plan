
import sys
from datetime import datetime

import openpyxl
import yfinance as yf

FILE = sys.argv[1] if len(sys.argv) > 1 else "4_ETF_Trading_Workbook_Template.xlsx"

OHLC_MAP = {
    "SOXL": 2,   # B:E
    "SOXS": 9,   # I:L
    "TQQQ": 16,  # P:S
    "SQQQ": 23,  # W:Z
}

CLOSE_ONLY_MAP = {
    "QQQ": 30,   # AD
    "SMH": 34,   # AH
}


def get_data(ticker):
    data = yf.download(
        ticker,
        period="5d",
        interval="1d",
        progress=False,
        auto_adjust=False,
        group_by="column",
        threads=False,
    )

    if data.empty:
        return None

    if hasattr(data.columns, "nlevels") and data.columns.nlevels > 1:
        data.columns = data.columns.get_level_values(0)

    def scalar(value):
        import numpy as np
        if hasattr(value, "iloc"):
            value = value.iloc[0]
        if isinstance(value, np.ndarray):
            value = value.flatten()[0]
        return float(value)

    last = data.tail(1)

    return {
        "open": scalar(last["Open"]),
        "high": scalar(last["High"]),
        "low": scalar(last["Low"]),
        "close": scalar(last["Close"]),
    }


def next_empty_row(ws, start_row: int = 3, date_col: int = 1) -> int:
    row = start_row
    while ws.cell(row=row, column=date_col).value not in (None, ""):
        row += 1
    return row


def main():
    wb = openpyxl.load_workbook(FILE)
    ws = wb["Daily_Data"]

    row = next_empty_row(ws)
    ws.cell(row=row, column=1).value = datetime.now().strftime("%m/%d/%y")

    for ticker, col in OHLC_MAP.items():
        d = get_data(ticker)
        if d:
            ws.cell(row=row, column=col).value = d["open"]
            ws.cell(row=row, column=col + 1).value = d["high"]
            ws.cell(row=row, column=col + 2).value = d["low"]
            ws.cell(row=row, column=col + 3).value = d["close"]

    for ticker, col in CLOSE_ONLY_MAP.items():
        d = get_data(ticker)
        if d:
            ws.cell(row=row, column=col).value = d["close"]

    wb.save(FILE)
    print(f"Updated successfully: row {row}")


if __name__ == "__main__":
    main()
