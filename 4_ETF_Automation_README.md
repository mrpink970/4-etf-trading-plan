# Daily Data Automation for the 4-ETF Workbook

## What this does
This script appends one daily row into the `Daily_Data` sheet for:

- SOXL
- SOXS
- TQQQ
- SQQQ

It fills:

- Date
- SOXL Open / High / Low / Close
- SOXS Close
- TQQQ Open / High / Low / Close
- SQQQ Close

The workbook formulas then calculate the daily % columns automatically.

## Files
- `update_4etf_daily_data.py`
- Your workbook: `4_ETF_Trading_Workbook_Template.xlsx`

## Install packages
```bash
pip install yfinance openpyxl
```

## Run manually
```bash
python update_4etf_daily_data.py 4_ETF_Trading_Workbook_Template.xlsx
```

## Daily use
Run it once after market close each day.

## Important
This script only automates data collection.
It does **not** place trades.
It does **not** change the trade log.
