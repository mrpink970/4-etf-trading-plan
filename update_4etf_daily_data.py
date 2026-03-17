import sys
from datetime import datetime

import openpyxl
import yfinance as yf
from openpyxl.workbook.properties import CalcProperties

DEFAULT_FILE = "4_ETF_Trading_Workbook_Template.xlsx"
FILE = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_FILE

# Full-OHLC workbook layout
COL_MAP = {
    "SOXL": 2,   # B:E
    "SOXS": 9,   # I:L
    "TQQQ": 16,  # P:S
    "SQQQ": 23,  # W:Z
}
QQQ_CLOSE_COL = 30   # AD
SMH_CLOSE_COL = 34   # AH


def get_data(ticker: str):
    data = yf.download(
        ticker,
        period="10d",
        interval="1d",
        progress=False,
        auto_adjust=False,
        group_by="column",
        threads=False,
    )

    if data is None or data.empty:
        return None

    # yfinance sometimes returns MultiIndex columns like ('Open', 'SOXL').
    if getattr(data.columns, "nlevels", 1) > 1:
        data.columns = data.columns.get_level_values(0)

    required = ["Open", "High", "Low", "Close"]
    missing = [c for c in required if c not in data.columns]
    if missing:
        raise ValueError(f"Missing expected columns for {ticker}: {missing}")

    # Keep only complete rows to avoid partial intraday / malformed values.
    data = data.dropna(subset=required)
    if data.empty:
        return None

    last = data.iloc[-1]

    return {
        "open": float(last["Open"]),
        "high": float(last["High"]),
        "low": float(last["Low"]),
        "close": float(last["Close"]),
    }


def find_target_row(ws):
    # Write into the first truly empty date row so prefilled formulas below remain intact.
    row = 3
    while ws.cell(row=row, column=1).value not in (None, ""):
        row += 1
    return row


def main():
    wb = openpyxl.load_workbook(FILE)
    ws = wb["Daily_Data"]

    row = find_target_row(ws)
    ws.cell(row=row, column=1).value = datetime.now().strftime("%m/%d/%y")

    for ticker, col in COL_MAP.items():
        d = get_data(ticker)
        if d:
            ws.cell(row=row, column=col).value = d["open"]
            ws.cell(row=row, column=col + 1).value = d["high"]
            ws.cell(row=row, column=col + 2).value = d["low"]
            ws.cell(row=row, column=col + 3).value = d["close"]

    qqq = get_data("QQQ")
    smh = get_data("SMH")

    if qqq:
        ws.cell(row=row, column=QQQ_CLOSE_COL).value = qqq["close"]
    if smh:
        ws.cell(row=row, column=SMH_CLOSE_COL).value = smh["close"]

    # Force Excel to recalculate formulas when the workbook is opened.
    wb.calculation = CalcProperties(calcMode="auto", fullCalcOnLoad=True, forceFullCalc=True)

    wb.save(FILE)
    print(f"Updated successfully: row {row}")


if __name__ == "__main__":
    main()
