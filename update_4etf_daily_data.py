#!/usr/bin/env python3
"""
ETF Daily Data Updater - FIXED VERSION
Fetches OHLC data for SOXL, TQQQ, SOXS, SQQQ, SMH, QQQ, and SOXX
Completely overwrites Daily_Data sheet with clean, validated data
"""

import sys
import pandas as pd
import yfinance as yf
from openpyxl import load_workbook
from openpyxl.utils import get_column_letter
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

# Tickers to fetch
TICKERS = {
    'SOXL': 'SOXL',
    'TQQQ': 'TQQQ',
    'SOXS': 'SOXS',
    'SQQQ': 'SQQQ',
    'SMH': 'SMH',
    'QQQ': 'QQQ',
    'SOXX': 'SOXX',
}

# Column suffixes for each ticker
COLUMN_SUFFIXES = ['_Open', '_High', '_Low', '_Close', '_%Chg', '_3D', '_5D']


def fetch_historical_data(ticker, start_date, end_date):
    """Fetch historical OHLC data from Yahoo Finance"""
    try:
        stock = yf.Ticker(ticker)
        data = stock.history(start=start_date, end=end_date)
        if data.empty:
            print(f"Warning: No data for {ticker}")
            return None
        return data
    except Exception as e:
        print(f"Error fetching {ticker}: {e}")
        return None


def calculate_returns(df, ticker):
    """Calculate 1D, 3D, 5D returns from close prices"""
    if df is None or df.empty:
        return {}
    
    # Calculate returns
    returns_1d = df['Close'].pct_change() * 100
    returns_3d = df['Close'].pct_change(periods=3) * 100
    returns_5d = df['Close'].pct_change(periods=5) * 100
    
    # Create return columns
    result = {}
    for date in df.index:
        date_str = date.strftime('%Y-%m-%d')
        result[date_str] = {
            f"{ticker}_%Chg": round(returns_1d.get(date, 0), 4) if date in returns_1d.index else 0,
            f"{ticker}_3D": round(returns_3d.get(date, 0), 4) if date in returns_3d.index else 0,
            f"{ticker}_5D": round(returns_5d.get(date, 0), 4) if date in returns_5d.index else 0,
        }
    
    return result


def validate_date(date_obj):
    """Validate that a date is reasonable (not in future, not too far in past)"""
    current_year = datetime.now().year
    if date_obj.year > current_year + 1:
        return False  # Future date beyond next year
    if date_obj.year < 2000:
        return False  # Too old
    return True


def update_workbook(workbook_path):
    """Main function to update the workbook with clean, validated data"""
    
    print("=" * 60)
    print("ETF DAILY DATA UPDATER - FIXED VERSION")
    print("=" * 60)
    
    # Determine date range
    end_date = datetime.now()
    start_date = end_date - timedelta(days=400)  # Get ~400 days of history
    
    print(f"\nFetching data from {start_date.date()} to {end_date.date()}")
    
    # Fetch data for all tickers
    all_data = {}
    all_returns = {}
    
    for ticker_symbol in TICKERS.keys():
        print(f"Fetching {ticker_symbol}...")
        df = fetch_historical_data(ticker_symbol, start_date, end_date)
        if df is not None and not df.empty:
            all_data[ticker_symbol] = df
            all_returns[ticker_symbol] = calculate_returns(df, ticker_symbol)
        else:
            print(f"  WARNING: No data for {ticker_symbol}")
    
    if not all_data:
        print("ERROR: No data fetched for any ticker")
        return False
    
    # Build complete dataset with all dates
    all_dates = set()
    for ticker, df in all_data.items():
        for date in df.index:
            if validate_date(date):
                all_dates.add(date.strftime('%Y-%m-%d'))
    
    all_dates = sorted(all_dates)
    print(f"\nTotal unique dates: {len(all_dates)}")
    print(f"Date range: {all_dates[0]} to {all_dates[-1]}")
    
    # Create complete DataFrame
    complete_data = {}
    skipped_dates = 0
    
    for date_str in all_dates:
        date_obj = datetime.strptime(date_str, '%Y-%m-%d')
        
        # Skip unreasonable dates
        if not validate_date(date_obj):
            skipped_dates += 1
            continue
            
        complete_data[date_str] = {'Date': date_str}
        
        for ticker, df in all_data.items():
            if date_obj in df.index:
                row = df.loc[date_obj]
                # Validate price data is reasonable
                open_price = row['Open']
                high_price = row['High']
                low_price = row['Low']
                close_price = row['Close']
                
                # Skip if prices are obviously wrong (negative or zero)
                if open_price <= 0 or high_price <= 0 or low_price <= 0 or close_price <= 0:
                    print(f"  WARNING: Invalid prices for {ticker} on {date_str}")
                    complete_data[date_str][f"{ticker}_Open"] = None
                    complete_data[date_str][f"{ticker}_High"] = None
                    complete_data[date_str][f"{ticker}_Low"] = None
                    complete_data[date_str][f"{ticker}_Close"] = None
                else:
                    complete_data[date_str][f"{ticker}_Open"] = round(open_price, 6)
                    complete_data[date_str][f"{ticker}_High"] = round(high_price, 6)
                    complete_data[date_str][f"{ticker}_Low"] = round(low_price, 6)
                    complete_data[date_str][f"{ticker}_Close"] = round(close_price, 6)
            else:
                complete_data[date_str][f"{ticker}_Open"] = None
                complete_data[date_str][f"{ticker}_High"] = None
                complete_data[date_str][f"{ticker}_Low"] = None
                complete_data[date_str][f"{ticker}_Close"] = None
            
            # Add return data
            if ticker in all_returns and date_str in all_returns[ticker]:
                complete_data[date_str].update(all_returns[ticker][date_str])
    
    if skipped_dates > 0:
        print(f"Skipped {skipped_dates} invalid dates")
    
    # Convert to DataFrame
    new_df = pd.DataFrame.from_dict(complete_data, orient='index')
    new_df = new_df.sort_index()  # Sort by date
    
    print(f"\nFinal DataFrame: {len(new_df)} rows, {len(new_df.columns)} columns")
    
    # Load or create workbook
    try:
        if workbook_path.exists():
            wb = load_workbook(workbook_path)
            print(f"Loaded existing workbook: {workbook_path}")
        else:
            wb = load_workbook()
            print(f"Created new workbook: {workbook_path}")
    except Exception as e:
        print(f"Error loading workbook: {e}")
        wb = load_workbook()
        print("Created new workbook")
    
    # Create or clear Daily_Data sheet
    if 'Daily_Data' in wb.sheetnames:
        print("Clearing existing Daily_Data sheet...")
        wb.remove(wb['Daily_Data'])
    
    ws = wb.create_sheet('Daily_Data')
    print("Created fresh Daily_Data sheet")
    
    # Write headers
    headers = ['Date'] + [col for col in new_df.columns if col != 'Date']
    for col_idx, header in enumerate(headers, start=1):
        ws.cell(row=1, column=col_idx, value=header)
    
    # Write data rows (limit to reasonable number to avoid corruption)
    max_rows = 500  # Limit to prevent corruption
    rows_written = 0
    
    for row_idx, (_, row) in enumerate(new_df.iterrows(), start=2):
        if rows_written >= max_rows:
            print(f"Reached maximum rows ({max_rows}), stopping")
            break
            
        # Skip rows with all None values
        has_data = False
        for col in headers:
            if col != 'Date':
                val = row[col] if col in row.index else None
                if val is not None and not pd.isna(val):
                    has_data = True
                    break
        
        if not has_data:
            continue
            
        for col_idx, header in enumerate(headers, start=1):
            if header == 'Date':
                value = row.name if hasattr(row, 'name') else row.get('Date', '')
            else:
                value = row[header] if header in row.index else None
                
            if value is not None and not pd.isna(value):
                # Format numbers nicely
                if isinstance(value, float):
                    ws.cell(row=row_idx, column=col_idx, value=round(value, 6))
                else:
                    ws.cell(row=row_idx, column=col_idx, value=value)
        
        rows_written += 1
    
    print(f"Wrote {rows_written} rows of data")
    
    # Auto-adjust column widths
    for col in range(1, ws.max_column + 1):
        max_length = 0
        col_letter = get_column_letter(col)
        for row in range(1, min(ws.max_row, 100) + 1):
            cell_value = ws.cell(row=row, column=col).value
            if cell_value:
                max_length = max(max_length, len(str(cell_value)))
        adjusted_width = min(max_length + 2, 15)  # Cap width at 15
        ws.column_dimensions[col_letter].width = adjusted_width
    
    # Ensure Signal sheet exists (preserve existing or create default)
    if 'Signal' not in wb.sheetnames:
        print("Creating Signal sheet with default values...")
        signal_ws = wb.create_sheet('Signal')
        signal_ws['D23'] = 'SOXL'
        signal_ws['D24'] = 'TQQQ'
        signal_ws['D27'] = datetime.now().strftime('%Y-%m-%d')
    else:
        print("Signal sheet preserved (unchanged)")
    
    # Save workbook
    try:
        wb.save(workbook_path)
        print(f"\n✅ Successfully updated {workbook_path}")
        print(f"   Total rows written: {rows_written}")
        print(f"   Date range: {all_dates[0]} to {all_dates[-1]}")
        print(f"   Columns: {len(headers)}")
        return True
    except Exception as e:
        print(f"Error saving workbook: {e}")
        return False


def main():
    if len(sys.argv) > 1:
        workbook_path = sys.argv[1]
    else:
        workbook_path = "4_ETF_Trading_Workbook_Template.xlsx"
    
    from pathlib import Path
    success = update_workbook(Path(workbook_path))
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
