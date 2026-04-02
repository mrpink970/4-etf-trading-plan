import sys
from datetime import datetime

import openpyxl
import yfinance as yf
from openpyxl.workbook.properties import CalcProperties
import pandas as pd

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


def get_data(ticker: str, period="10d"):
    data = yf.download(
        ticker,
        period=period,
        interval="1d",
        progress=False,
        auto_adjust=False,
        group_by="column",
        threads=False,
    )

    if data is None or data.empty:
        return None

    if getattr(data.columns, "nlevels", 1) > 1:
        data.columns = data.columns.get_level_values(0)

    required = ["Open", "High", "Low", "Close"]
    missing = [c for c in required if c not in data.columns]
    if missing:
        raise ValueError(f"Missing expected columns for {ticker}: {missing}")

    data = data.dropna(subset=required)
    if data.empty:
        return None

    return data


def find_target_row(ws):
    row = 3
    while ws.cell(row=row, column=1).value not in (None, ""):
        row += 1
    return row


def calculate_signals(ws_daily):
    """Read Daily_Data and determine which ETF to trade"""
    # Load daily data into DataFrame
    rows = list(ws_daily.iter_rows(values_only=True))
    if not rows:
        return "WAIT", "WAIT"
    
    # Find header row
    header_row = None
    for i, row in enumerate(rows):
        if row and row[0] == "Date":
            header_row = i
            break
    
    if header_row is None:
        return "WAIT", "WAIT"
    
    headers = [str(cell) if cell else "" for cell in rows[header_row]]
    
    data = []
    for row in rows[header_row + 1:]:
        if row and row[0]:
            data.append(row)
    
    if len(data) < 5:
        return "WAIT", "WAIT"
    
    df = pd.DataFrame(data, columns=headers)
    
    # Convert to numeric where needed
    for etf in ["SOXL", "SOXS", "TQQQ", "SQQQ"]:
        close_col = f"{etf} Close"
        if close_col in df.columns:
            df[close_col] = pd.to_numeric(df[close_col], errors='coerce')
    
    # Get latest close prices
    latest = df.iloc[-1]
    
    # Simple momentum strategy - example:
    # If SOXL > 20-day SMA of SOXL, buy SOXL, else buy SOXS
    if "SOXL Close" in df.columns:
        soxl_prices = df["SOXL Close"].dropna()
        if len(soxl_prices) >= 20:
            soxl_sma20 = soxl_prices.tail(20).mean()
            current_soxl = latest["SOXL Close"]
            
            if current_soxl > soxl_sma20:
                primary = "SOXL"
                secondary = "TQQQ"
            else:
                primary = "SOXS"
                secondary = "SQQQ"
        else:
            primary = "SOXL"
            secondary = "TQQQ"
    else:
        primary = "SOXL"
        secondary = "TQQQ"
    
    return primary, secondary


def update_signals(ws_signal, ws_daily):
    """Write calculated signals to the Signal sheet"""
    primary, secondary = calculate_signals(ws_daily)
    
    ws_signal["D23"] = primary  # Primary ETF
    ws_signal["D24"] = secondary  # Secondary ETF
    ws_signal["D27"] = datetime.now().strftime("%m/%d/%y")  # Signal date
    
    print(f"Signals updated: Primary={primary}, Secondary={secondary}")


def main():
    wb = openpyxl.load_workbook(FILE)
    ws_daily = wb["Daily_Data"]
    ws_signal = wb["Signal"]
    
    row = find_target_row(ws_daily)
    ws_daily.cell(row=row, column=1).value = datetime.now().strftime("%m/%d/%y")
    
    # Get OHLC data for each ETF
    for ticker, col in COL_MAP.items():
        data = get_data(ticker)
        if data is not None and not data.empty:
            latest = data.iloc[-1]
            ws_daily.cell(row=row, column=col).value = latest["Open"]
            ws_daily.cell(row=row, column=col + 1).value = latest["High"]
            ws_daily.cell(row=row, column=col + 2).value = latest["Low"]
            ws_daily.cell(row=row, column=col + 3).value = latest["Close"]
    
    # Get QQQ and SMH data
    qqq = get_data("QQQ")
    smh = get_data("SMH")
    
    if qqq is not None and not qqq.empty:
        ws_daily.cell(row=row, column=QQQ_CLOSE_COL).value = qqq.iloc[-1]["Close"]
    if smh is not None and not smh.empty:
        ws_daily.cell(row=row, column=SMH_CLOSE_COL).value = smh.iloc[-1]["Close"]
    
    # Update signals based on latest data
    update_signals(ws_signal, ws_daily)
    
    # Force Excel recalculation
    wb.calculation = CalcProperties(calcMode="auto", fullCalcOnLoad=True, forceFullCalc=True)
    
    wb.save(FILE)
    print(f"Updated successfully: row {row}")


if __name__ == "__main__":
    main()
