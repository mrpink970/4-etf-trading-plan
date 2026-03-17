
import yfinance as yf
import openpyxl
from datetime import datetime

# Update this path if your workbook lives in a subfolder in GitHub
FILE = "4_ETF_Trading_Workbook_Template.xlsx"

TICKERS = {
    "SOXL": "SOXL",
    "SOXS": "SOXS",
    "TQQQ": "TQQQ",
    "SQQQ": "SQQQ",
    "QQQ": "QQQ",
    "SMH": "SMH",
}

def get_data(ticker):
    data = yf.download(ticker, period="5d", interval="1d", progress=False)
    if data.empty:
        return None
    last = data.iloc[-1]
    return {
        "open": float(last["Open"]),
        "high": float(last["High"]),
        "low": float(last["Low"]),
        "close": float(last["Close"]),
    }

wb = openpyxl.load_workbook(FILE)
ws = wb["Daily_Data"]

row = ws.max_row + 1
today = datetime.now().strftime("%m/%d/%y")
ws[f"A{row}"] = today

# Column starts in v7/vTemplate rebuild structure
col_map = {
    "SOXL": 2,   # B:E
    "SOXS": 9,   # I:L
    "TQQQ": 16,  # P:S
    "SQQQ": 23,  # W:Z
}

for ticker, col in col_map.items():
    d = get_data(ticker)
    if d:
        ws.cell(row=row, column=col).value = d["open"]
        ws.cell(row=row, column=col + 1).value = d["high"]
        ws.cell(row=row, column=col + 2).value = d["low"]
        ws.cell(row=row, column=col + 3).value = d["close"]

qqq = get_data("QQQ")
smh = get_data("SMH")

if qqq:
    ws.cell(row=row, column=30).value = qqq["close"]  # AD
if smh:
    ws.cell(row=row, column=34).value = smh["close"]  # AH

wb.save(FILE)
print("Updated successfully")
