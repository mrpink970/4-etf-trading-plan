#!/usr/bin/env python3
"""
ETF Daily Data Updater - SIMPLIFIED VERSION
Fetches only last 30 days of data
"""

import sys
import pandas as pd
import yfinance as yf
from openpyxl import load_workbook
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

TICKERS = ['SOXL', 'TQQQ', 'SOXS', 'SQQQ']

def fetch_ticker_data(ticker, days=30):
    """Fetch data for a single ticker"""
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    
    print(f"  Fetching {ticker} from {start_date.date()} to {end_date.date()}")
    
    try:
        data = yf.download(ticker, start=start_date, end=end_date, progress=False)
        if data.empty:
            print(f"    WARNING: No data for {ticker}")
            return None
        
        # Reset index to get Date as column
        data = data.reset_index()
        data['Date'] = data['Date'].dt.strftime('%Y-%m-%d')
        
        return data
    except Exception as e:
        print(f"    ERROR: {e}")
        return None


def update_workbook(workbook_path):
    print("=" * 60)
    print("ETF DATA UPDATER - SIMPLIFIED")
    print("=" * 60)
    
    # Fetch data for all tickers
    all_data = {}
    for ticker in TICKERS:
        df = fetch_ticker_data(ticker, days=30)
        if df is not None:
            all_data[ticker] = df
    
    if not all_data:
        print("ERROR: No data fetched")
        return False
    
    # Get all unique dates
    all_dates = set()
    for df in all_data.values():
        all_dates.update(df['Date'].tolist())
    all_dates = sorted(all_dates)
    
    print(f"\nDates: {all_dates[0]} to {all_dates[-1]} ({len(all_dates)} days)")
    
    # Build the DataFrame
    rows = []
    for date in all_dates:
        row = {'Date': date}
        for ticker, df in all_data.items():
            ticker_data = df[df['Date'] == date]
            if not ticker_data.empty:
                row[f"{ticker}_Open"] = round(float(ticker_data['Open'].iloc[0]), 4)
                row[f"{ticker}_High"] = round(float(ticker_data['High'].iloc[0]), 4)
                row[f"{ticker}_Low"] = round(float(ticker_data['Low'].iloc[0]), 4)
                row[f"{ticker}_Close"] = round(float(ticker_data['Close'].iloc[0]), 4)
            else:
                row[f"{ticker}_Open"] = None
                row[f"{ticker}_High"] = None
                row[f"{ticker}_Low"] = None
                row[f"{ticker}_Close"] = None
        rows.append(row)
    
    df_final = pd.DataFrame(rows)
    
    # Calculate returns
    for ticker in TICKERS:
        close_col = f"{ticker}_Close"
        if close_col in df_final.columns:
            df_final[f"{ticker}_%Chg"] = df_final[close_col].pct_change() * 100
            df_final[f"{ticker}_%Chg"] = df_final[f"{ticker}_%Chg"].round(4)
    
    print(f"\nFinal data: {len(df_final)} rows, {len(df_final.columns)} columns")
    
    # Write to Excel
    try:
        wb = load_workbook(workbook_path)
        if 'Daily_Data' in wb.sheetnames:
            wb.remove(wb['Daily_Data'])
    except:
        wb = load_workbook()
    
    ws = wb.create_sheet('Daily_Data')
    
    # Write headers
    headers = list(df_final.columns)
    for col_idx, header in enumerate(headers, 1):
        ws.cell(row=1, column=col_idx, value=header)
    
    # Write data
    for row_idx, row in df_final.iterrows():
        excel_row = row_idx + 2
        for col_idx, header in enumerate(headers, 1):
            value = row[header]
            if pd.notna(value):
                ws.cell(row=excel_row, column=col_idx, value=value)
    
    # Ensure Signal sheet
    if 'Signal' not in wb.sheetnames:
        ws_signal = wb.create_sheet('Signal')
        ws_signal['D23'] = 'SOXL'
        ws_signal['D24'] = 'TQQQ'
    
    wb.save(workbook_path)
    print(f"\n✅ Updated {workbook_path}")
    print(f"   Rows: {len(df_final)}")
    
    # Print last few rows for verification
    print("\nLast 3 rows of data:")
    print(df_final.tail(3)[['Date', 'SOXL_Open', 'SOXL_High', 'SOXL_Close']])
    
    return True


if __name__ == "__main__":
    from pathlib import Path
    workbook_path = Path("4_ETF_Trading_Workbook_Template.xlsx")
    success = update_workbook(workbook_path)
    sys.exit(0 if success else 1)
