#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path
from typing import Dict, Optional
import os
import smtplib
from email.message import EmailMessage
from datetime import datetime

import pandas as pd
from openpyxl import load_workbook


WORKBOOK_PATH = Path("4_ETF_Trading_Workbook_Template.xlsx")
POSITIONS_PATH = Path("etf_paper_positions.csv")
TRADE_LOG_PATH = Path("etf_paper_trade_log.csv")
PERFORMANCE_PATH = Path("etf_paper_performance.csv")

SHARES_PER_TRADE = 100
MAX_TRADES = 2
TRAILING_STOP_PCT = 0.12

BULL_ETFS = {"SOXL", "TQQQ"}
BEAR_ETFS = {"SOXS", "SQQQ"}
ALL_ETFS = BULL_ETFS | BEAR_ETFS


def normalize_text(value) -> str:
    if value is None:
        return ""
    return str(value).strip().upper()


def safe_float(value) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except Exception:
        return None


def determine_regime(primary_etf: str) -> str:
    if primary_etf in BULL_ETFS:
        return "bull"
    if primary_etf in BEAR_ETFS:
        return "bear"
    return "neutral"


def read_daily_data_wide(daily_ws) -> pd.DataFrame:
    rows = list(daily_ws.iter_rows(values_only=True))
    if not rows:
        raise ValueError("Daily_Data sheet is empty.")

    # Find the row that contains the actual headers by looking for "Date"
    header_idx = None
    for i, row in enumerate(rows):
        values = [str(cell).strip() if cell is not None else "" for cell in row]
        if "Date" in values:
            header_idx = i
            break

    if header_idx is None:
        raise ValueError("Could not find Daily_Data header row.")

    headers = [str(h).strip() if h is not None else "" for h in rows[header_idx]]

    records = []
    for row in rows[header_idx + 1:]:
        if all(v is None or str(v).strip() == "" for v in row):
            continue

        rec = {}
        for i, h in enumerate(headers):
            if h == "":
                continue
            rec[h] = row[i] if i < len(row) else None
        records.append(rec)

    df = pd.DataFrame(records)
    if df.empty:
        raise ValueError("Daily_Data has no usable data rows.")

    if "Date" not in df.columns:
        raise ValueError("Daily_Data missing Date column.")

    df = df[df["Date"].notna()].copy()
    df["Date"] = pd.to_datetime(df["Date"]).dt.date

    return df


def extract_latest_prices(df: pd.DataFrame) -> tuple[str, Dict[str, Dict[str, Optional[float]]]]:
    latest_row = df.sort_values("Date").iloc[-1]
    latest_date = str(latest_row["Date"])

    prices: Dict[str, Dict[str, Optional[float]]] = {}
    for etf in sorted(ALL_ETFS):
        # Try both underscore and space formats
        open_col_underscore = f"{etf}_Open"
        high_col_underscore = f"{etf}_High"
        low_col_underscore = f"{etf}_Low"
        close_col_underscore = f"{etf}_Close"
        
        open_col_space = f"{etf} Open"
        high_col_space = f"{etf} High"
        low_col_space = f"{etf} Low"
        close_col_space = f"{etf} Close"
        
        open_val = latest_row.get(open_col_underscore)
        if open_val is None:
            open_val = latest_row.get(open_col_space)
            
        high_val = latest_row.get(high_col_underscore)
        if high_val is None:
            high_val = latest_row.get(high_col_space)
            
        low_val = latest_row.get(low_col_underscore)
        if low_val is None:
            low_val = latest_row.get(low_col_space)
            
        close_val = latest_row.get(close_col_underscore)
        if close_val is None:
            close_val = latest_row.get(close_col_space)
        
        prices[etf] = {
            "open": safe_float(open_val),
            "high": safe_float(high_val),
            "low": safe_float(low_val),
            "close": safe_float(close_val),
        }

    return latest_date, prices


def load_workbook_state(path: Path) -> Dict[str, object]:
    if not path.exists():
        raise FileNotFoundError(f"Workbook not found: {path}")

    wb = load_workbook(path, data_only=True)

    if "Signal" not in wb.sheetnames:
        raise ValueError("Workbook missing Signal sheet.")
    if "Daily_Data" not in wb.sheetnames:
        raise ValueError("Workbook missing Daily_Data sheet.")

    signal_ws = wb["Signal"]
    daily_ws = wb["Daily_Data"]

    primary_etf = normalize_text(signal_ws["D23"].value)
    secondary_etf = normalize_text(signal_ws["D24"].value)
    signal_date_raw = signal_ws["D27"].value

    daily_df = read_daily_data_wide(daily_ws)
    daily_date, prices = extract_latest_prices(daily_df)

    if signal_date_raw is not None:
        signal_date = str(pd.to_datetime(signal_date_raw).date())
    else:
        signal_date = daily_date

    return {
        "date": signal_date,
        "primary_etf": primary_etf,
        "secondary_etf": secondary_etf,
        "regime": determine_regime(primary_etf),
        "prices": prices,
    }


def load_positions() -> pd.DataFrame:
    cols = [
        "ticker",
        "regime",
        "entry_date",
        "entry_price",
        "shares",
        "highest_price",
        "trailing_stop",
    ]
    if POSITIONS_PATH.exists():
        df = pd.read_csv(POSITIONS_PATH)
        if df.empty:
            return pd.DataFrame(columns=cols)
        return df
    return pd.DataFrame(columns=cols)


def load_trade_log() -> pd.DataFrame:
    cols = [
        "ticker",
        "regime",
        "entry_date",
        "entry_price",
        "exit_date",
        "exit_price",
        "shares",
        "gross_pl",
        "return_pct",
        "exit_reason",
    ]
    if TRADE_LOG_PATH.exists():
        df = pd.read_csv(TRADE_LOG_PATH)
        if df.empty:
            return pd.DataFrame(columns=cols)
        return df
    return pd.DataFrame(columns=cols)


def save_positions(df: pd.DataFrame) -> None:
    cols = [
        "ticker",
        "regime",
        "entry_date",
        "entry_price",
        "shares",
        "highest_price",
        "trailing_stop",
    ]
    if df.empty:
        pd.DataFrame(columns=cols).to_csv(POSITIONS_PATH, index=False)
    else:
        df[cols].to_csv(POSITIONS_PATH, index=False)


def save_trade_log(df: pd.DataFrame) -> None:
    cols = [
        "ticker",
        "regime",
        "entry_date",
        "entry_price",
        "exit_date",
        "exit_price",
        "shares",
        "gross_pl",
        "return_pct",
        "exit_reason",
    ]
    if df.empty:
        pd.DataFrame(columns=cols).to_csv(TRADE_LOG_PATH, index=False)
    else:
        df[cols].to_csv(TRADE_LOG_PATH, index=False)


def save_performance(trade_log: pd.DataFrame) -> None:
    cols = [
        "total_trades",
        "win_rate",
        "loss_rate",
        "avg_gain_pct",
        "avg_loss_pct",
        "largest_gain_pct",
        "largest_loss_pct",
        "total_gross_pl",
        "expectancy_pct",
    ]

    if trade_log.empty:
        pd.DataFrame(
            [{
                "total_trades": 0,
                "win_rate": 0.0,
                "loss_rate": 0.0,
                "avg_gain_pct": 0.0,
                "avg_loss_pct": 0.0,
                "largest_gain_pct": 0.0,
                "largest_loss_pct": 0.0,
                "total_gross_pl": 0.0,
                "expectancy_pct": 0.0,
            }]
        )[cols].to_csv(PERFORMANCE_PATH, index=False)
        return

    wins = trade_log[trade_log["gross_pl"] > 0].copy()
    losses = trade_log[trade_log["gross_pl"] < 0].copy()

    total_trades = len(trade_log)
    win_rate = len(wins) / total_trades if total_trades else 0.0
    loss_rate = len(losses) / total_trades if total_trades else 0.0
    avg_gain_pct = wins["return_pct"].mean() if not wins.empty else 0.0
    avg_loss_pct = abs(losses["return_pct"].mean()) if not losses.empty else 0.0
    largest_gain_pct = wins["return_pct"].max() if not wins.empty else 0.0
    largest_loss_pct = losses["return_pct"].min() if not losses.empty else 0.0
    total_gross_pl = trade_log["gross_pl"].sum()
    expectancy_pct = (win_rate * avg_gain_pct) - (loss_rate * avg_loss_pct)

    pd.DataFrame(
        [{
            "total_trades": total_trades,
            "win_rate": round(win_rate, 6),
            "loss_rate": round(loss_rate, 6),
            "avg_gain_pct": round(float(avg_gain_pct), 6),
            "avg_loss_pct": round(float(avg_loss_pct), 6),
            "largest_gain_pct": round(float(largest_gain_pct), 6),
            "largest_loss_pct": round(float(largest_loss_pct), 6),
            "total_gross_pl": round(float(total_gross_pl), 6),
            "expectancy_pct": round(float(expectancy_pct), 6),
        }]
    )[cols].to_csv(PERFORMANCE_PATH, index=False)


def update_trailing_stops(
    positions: pd.DataFrame,
    prices: Dict[str, Dict[str, Optional[float]]],
) -> pd.DataFrame:
    if positions.empty:
        return positions

    out = positions.copy()

    for idx, row in out.iterrows():
        ticker = normalize_text(row["ticker"])
        px = prices.get(ticker, {})
        high_price = px.get("high")
        if high_price is None:
            continue

        current_highest = safe_float(row["highest_price"])
        if current_highest is None or high_price > current_highest:
            current_highest = high_price

        trailing_stop = current_highest * (1 - TRAILING_STOP_PCT)

        out.at[idx, "highest_price"] = round(current_highest, 6)
        out.at[idx, "trailing_stop"] = round(trailing_stop, 6)

    return out


def build_exit_list(
    positions: pd.DataFrame,
    current_regime: str,
    primary_etf: str,
    secondary_etf: str,
    prices: Dict[str, Dict[str, Optional[float]]],
) -> list[dict[str, str]]:
    exits: list[dict[str, str]] = []

    valid_targets = set()
    if primary_etf in ALL_ETFS and primary_etf != "WAIT":
        valid_targets.add(primary_etf)
    if secondary_etf in ALL_ETFS and secondary_etf != "WAIT":
        valid_targets.add(secondary_etf)

    for _, row in positions.iterrows():
        ticker = normalize_text(row["ticker"])
        held_regime = normalize_text(row["regime"]).lower()

        if current_regime != "neutral" and held_regime != current_regime:
            exits.append({"ticker": ticker, "reason": "regime_flip"})
            continue

        if ticker not in valid_targets:
            exits.append({"ticker": ticker, "reason": "signal_negative"})
            continue

        low_price = prices.get(ticker, {}).get("low")
        trailing_stop = safe_float(row["trailing_stop"])
        if low_price is not None and trailing_stop is not None and low_price <= trailing_stop:
            exits.append({"ticker": ticker, "reason": "trailing_stop"})
            continue

    dedup = {}
    for item in exits:
        dedup.setdefault(item["ticker"], item["reason"])
    return [{"ticker": k, "reason": v} for k, v in dedup.items()]


def apply_exits(
    positions: pd.DataFrame,
    exits: list[dict[str, str]],
    asof_date: str,
    prices: Dict[str, Dict[str, Optional[float]]],
    trade_log: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if positions.empty or not exits:
        return positions, trade_log

    exit_map = {x["ticker"]: x["reason"] for x in exits}
    keep_rows = []
    new_trades = []

    for _, row in positions.iterrows():
        ticker = normalize_text(row["ticker"])
        if ticker not in exit_map:
            keep_rows.append(row.to_dict())
            continue

        exit_price = prices.get(ticker, {}).get("open")
        if exit_price is None:
            keep_rows.append(row.to_dict())
            continue

        entry_price = safe_float(row["entry_price"])
        shares = int(row["shares"])
        gross_pl = (exit_price - entry_price) * shares
        return_pct = ((exit_price / entry_price) - 1) * 100 if entry_price else 0.0

        new_trades.append({
            "ticker": ticker,
            "regime": row["regime"],
            "entry_date": row["entry_date"],
            "entry_price": round(entry_price, 6),
            "exit_date": asof_date,
            "exit_price": round(exit_price, 6),
            "shares": shares,
            "gross_pl": round(gross_pl, 6),
            "return_pct": round(return_pct, 6),
            "exit_reason": exit_map[ticker],
        })

    remaining = pd.DataFrame(keep_rows)
    if remaining.empty:
        remaining = pd.DataFrame(columns=[
            "ticker", "regime", "entry_date", "entry_price",
            "shares", "highest_price", "trailing_stop"
        ])

    updated_log = pd.concat([trade_log, pd.DataFrame(new_trades)], ignore_index=True)

    return remaining, updated_log


def build_position_row(
    ticker: str,
    regime: str,
    asof_date: str,
    prices: Dict[str, Dict[str, Optional[float]]],
) -> Optional[dict[str, object]]:
    entry_price = prices.get(ticker, {}).get("open")
    high_price = prices.get(ticker, {}).get("high")

    if entry_price is None:
        return None
    if high_price is None:
        high_price = entry_price

    trailing_stop = high_price * (1 - TRAILING_STOP_PCT)

    return {
        "ticker": ticker,
        "regime": regime,
        "entry_date": asof_date,
        "entry_price": round(entry_price, 6),
        "shares": SHARES_PER_TRADE,
        "highest_price": round(high_price, 6),
        "trailing_stop": round(trailing_stop, 6),
    }


def apply_entries(
    positions: pd.DataFrame,
    current_regime: str,
    primary_etf: str,
    secondary_etf: str,
    asof_date: str,
    prices: Dict[str, Dict[str, Optional[float]]],
) -> pd.DataFrame:
    if current_regime == "neutral":
        return positions

    current = positions.copy()
    held = set(current["ticker"].astype(str).str.upper().tolist()) if not current.empty else set()

    desired = []
    if primary_etf in ALL_ETFS and primary_etf != "WAIT":
        desired.append(primary_etf)
    if secondary_etf in ALL_ETFS and secondary_etf != "WAIT":
        desired.append(secondary_etf)

    for ticker in desired:
        if len(current) >= MAX_TRADES:
            break
        if ticker in held:
            continue

        row = build_position_row(ticker, current_regime, asof_date, prices)
        if row is None:
            continue

        current = pd.concat([current, pd.DataFrame([row])], ignore_index=True)
        held.add(ticker)

    return current


def send_email_summary(asof_date: str, primary_etf: str, secondary_etf: str, 
                       regime: str, positions: pd.DataFrame, trade_log: pd.DataFrame,
                       new_entries: list = None, new_exits: list = None) -> None:
    """Send email summary with trading activity"""
    
    # Check if email credentials are available
    mail_username = os.environ.get("MAIL_USERNAME")
    mail_password = os.environ.get("MAIL_PASSWORD")
    mail_to = os.environ.get("MAIL_TO")
    
    if not mail_username or not mail_password or not mail_to:
        print("Email credentials not found. Skipping email notification.")
        return
    
    # Build email body
    body = f"""
═══════════════════════════════════════════════════════════
  ETF PAPER TRADING UPDATE - {asof_date}
═══════════════════════════════════════════════════════════

📊 MARKET REGIME: {regime.upper()}
🎯 SIGNAL: {primary_etf} / {secondary_etf}

"""
    
    # New entries
    if new_entries and len(new_entries) > 0:
        body += "🟢 NEW ENTRIES:\n"
        for entry in new_entries:
            body += f"   • {entry['ticker']}: {entry['shares']} shares @ ${entry['price']:.2f}\n"
            body += f"     Stop: ${entry['stop']:.2f}\n"
        body += "\n"
    
    # New exits (closed trades today)
    if new_exits and len(new_exits) > 0:
        body += "🔴 POSITIONS CLOSED:\n"
        for exit_trade in new_exits:
            pl_symbol = "+" if exit_trade['pl'] >= 0 else ""
            body += f"   • {exit_trade['ticker']}: {exit_trade['return_pct']:.1f}% ({pl_symbol}${exit_trade['pl']:.2f})\n"
            body += f"     Reason: {exit_trade['reason']}\n"
        body += "\n"
    
    # Current open positions
    if len(positions) > 0:
        body += "📈 CURRENT OPEN POSITIONS:\n"
        for _, row in positions.iterrows():
            entry_price = safe_float(row["entry_price"])
            shares = int(row["shares"])
            # Calculate unrealized P&L using current price (approximate)
            body += f"   • {row['ticker']}: {shares} shares @ ${entry_price:.2f}\n"
            body += f"     Stop: ${safe_float(row['trailing_stop']):.2f}\n"
        body += "\n"
    else:
        body += "📈 CURRENT OPEN POSITIONS: None (in cash)\n\n"
    
    # Recent trade summary
    if len(trade_log) > 0:
        recent = trade_log.tail(5)
        total_pl = trade_log["gross_pl"].sum() if "gross_pl" in trade_log.columns else 0
        winning_trades = len(trade_log[trade_log["gross_pl"] > 0]) if "gross_pl" in trade_log.columns else 0
        win_rate = (winning_trades / len(trade_log) * 100) if len(trade_log) > 0 else 0
        
        body += f"""
📊 PERFORMANCE SUMMARY:
   • Total Trades: {len(trade_log)}
   • Win Rate: {win_rate:.1f}%
   • Total P&L: ${total_pl:.2f}
   • Recent Trades: {len(recent)}

"""
    
    # Dashboard link
    body += f"""
═══════════════════════════════════════════════════════════
  📊 VIEW FULL DASHBOARD:
  https://mrpink970.github.io/4-etf-trading-plan/dashboard.html
═══════════════════════════════════════════════════════════

Generated by GitHub Actions - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
"""
    
    # Send email
    try:
        msg = EmailMessage()
        msg.set_content(body)
        msg["Subject"] = f"ETF Trading Update - {asof_date} ({regime.upper()})"
        msg["From"] = mail_username
        msg["To"] = mail_to
        
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
            smtp.login(mail_username, mail_password)
            smtp.send_message(msg)
        
        print(f"Email sent successfully to {mail_to}")
    except Exception as e:
        print(f"Failed to send email: {e}")


def main() -> None:
    print("=" * 50)
    print("ETF PAPER TRADING SYSTEM")
    print("=" * 50)
    
    # Load state
    state = load_workbook_state(WORKBOOK_PATH)
    asof_date = state["date"]
    primary_etf = state["primary_etf"]
    secondary_etf = state["secondary_etf"]
    current_regime = state["regime"]
    prices = state["prices"]
    
    # Load existing data
    old_positions = load_positions()
    old_trade_log = load_trade_log()
    
    print(f"Date: {asof_date}")
    print(f"Signal: {primary_etf} / {secondary_etf}")
    print(f"Regime: {current_regime}")
    print(f"Existing positions: {len(old_positions)}")
    
    # Track what changed for email
    new_entries = []
    new_exits = []
    
    # Update trailing stops
    positions = update_trailing_stops(old_positions, prices)
    
    # Process exits
    exits = build_exit_list(
        positions=positions,
        current_regime=current_regime,
        primary_etf=primary_etf,
        secondary_etf=secondary_etf,
        prices=prices,
    )
    
    # Track exits before applying
    if exits:
        for exit_item in exits:
            ticker = exit_item["ticker"]
            reason = exit_item["reason"]
            pos_row = positions[positions["ticker"] == ticker]
            if not pos_row.empty:
                entry_price = safe_float(pos_row.iloc[0]["entry_price"])
                shares = int(pos_row.iloc[0]["shares"])
                exit_price = prices.get(ticker, {}).get("open")
                if exit_price:
                    pl = (exit_price - entry_price) * shares
                    return_pct = ((exit_price / entry_price) - 1) * 100
                    new_exits.append({
                        "ticker": ticker,
                        "pl": pl,
                        "return_pct": return_pct,
                        "reason": reason,
                    })
    
    positions, trade_log = apply_exits(
        positions=positions,
        exits=exits,
        asof_date=asof_date,
        prices=prices,
        trade_log=old_trade_log,
    )
    
    # Process entries
    old_held = set(old_positions["ticker"].astype(str).str.upper().tolist()) if not old_positions.empty else set()
    
    if current_regime != "neutral":
        desired = []
        if primary_etf in ALL_ETFS and primary_etf != "WAIT":
            desired.append(primary_etf)
        if secondary_etf in ALL_ETFS and secondary_etf != "WAIT":
            desired.append(secondary_etf)
        
        for ticker in desired:
            if ticker not in old_held:
                entry_price = prices.get(ticker, {}).get("open")
                if entry_price:
                    new_entries.append({
                        "ticker": ticker,
                        "price": entry_price,
                        "shares": SHARES_PER_TRADE,
                        "stop": entry_price * (1 - TRAILING_STOP_PCT),
                    })
    
    positions = apply_entries(
        positions=positions,
        current_regime=current_regime,
        primary_etf=primary_etf,
        secondary_etf=secondary_etf,
        asof_date=asof_date,
        prices=prices,
    )
    
    # Save data
    save_positions(positions)
    save_trade_log(trade_log)
    save_performance(trade_log)
    
    # Send email notification
    send_email_summary(
        asof_date=asof_date,
        primary_etf=primary_etf,
        secondary_etf=secondary_etf,
        regime=current_regime,
        positions=positions,
        trade_log=trade_log,
        new_entries=new_entries,
        new_exits=new_exits,
    )
    
    # Print summary
    print("\n" + "=" * 50)
    print("SUMMARY")
    print("=" * 50)
    print(f"Date: {asof_date}")
    print(f"Primary ETF: {primary_etf}")
    print(f"Secondary ETF: {secondary_etf}")
    print(f"Regime: {current_regime}")
    print(f"Open positions: {len(positions)}")
    print(f"Closed trades logged: {len(trade_log)}")
    
    if new_entries:
        print(f"\n🟢 New entries: {len(new_entries)}")
        for e in new_entries:
            print(f"  • {e['ticker']} @ ${e['price']:.2f}")
    
    if new_exits:
        print(f"\n🔴 Positions closed: {len(new_exits)}")
        for e in new_exits:
            print(f"  • {e['ticker']}: {e['return_pct']:.1f}% (${e['pl']:.2f})")
    
    print("=" * 50)


if __name__ == "__main__":
    main()
