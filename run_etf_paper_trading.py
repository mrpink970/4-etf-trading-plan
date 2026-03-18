#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Optional

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

    # Fixed dashboard cells from your workbook
    primary_etf = normalize_text(signal_ws["D23"].value)
    secondary_etf = normalize_text(signal_ws["D24"].value)
    signal_date_raw = signal_ws["D27"].value

    rows = list(daily_ws.iter_rows(values_only=True))
    if not rows:
        raise ValueError("Daily_Data sheet is empty.")

    headers = [str(h).strip() if h is not None else "" for h in rows[0]]

    data_rows = []
    for row in rows[1:]:
        if all(v is None or str(v).strip() == "" for v in row):
            continue
        rec = {}
        for i, h in enumerate(headers):
            if h == "":
                continue
            rec[h] = row[i] if i < len(row) else None
        data_rows.append(rec)

    daily_df = pd.DataFrame(data_rows)
    if daily_df.empty:
        raise ValueError("Daily_Data has no usable data rows.")

    if "Date" not in daily_df.columns:
        raise ValueError("Daily_Data missing Date column.")

    daily_df = daily_df[daily_df["Date"].notna()].copy()
    daily_df["Date"] = pd.to_datetime(daily_df["Date"]).dt.date
    latest_row = daily_df.sort_values("Date").iloc[-1]

    if signal_date_raw is not None:
        signal_date = str(pd.to_datetime(signal_date_raw).date())
    else:
        signal_date = str(latest_row["Date"])

    prices: Dict[str, Dict[str, Optional[float]]] = {}
    for etf in sorted(ALL_ETFS):
        prices[etf] = {
            "open": safe_float(latest_row.get(f"{etf} Open")),
            "high": safe_float(latest_row.get(f"{etf} High")),
            "low": safe_float(latest_row.get(f"{etf} Low")),
            "close": safe_float(latest_row.get(f"{etf} Close")),
        }

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
        return df if not df.empty else pd.DataFrame(columns=cols)
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
        return df if not df.empty else pd.DataFrame(columns=cols)
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


def update_trailing_stops(positions: pd.DataFrame, prices: Dict[str, Dict[str, Optional[float]]]) -> pd.DataFrame:
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
) -> List[Dict[str, str]]:
    exits: List[Dict[str, str]] = []

    valid_targets = set()
    if primary_etf in ALL_ETFS:
        valid_targets.add(primary_etf)
    if secondary_etf in ALL_ETFS:
        valid_targets.add(secondary_etf)

    for _, row in positions.iterrows():
        ticker = normalize_text(row["ticker"])
        held_regime = normalize_text(row["regime"]).lower()

        # 1. Regime flip exits all opposite-direction holdings
        if current_regime != "neutral" and held_regime != current_regime:
            exits.append({"ticker": ticker, "reason": "regime_flip"})
            continue

        # 2. If signal no longer includes the ETF, exit
        if ticker not in valid_targets:
            exits.append({"ticker": ticker, "reason": "signal_negative"})
            continue

        # 3. Trailing stop
        px = prices.get(ticker, {})
        low_price = px.get("low")
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
    exits: List[Dict[str, str]],
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
    updated_log = pd.concat([trade_log, pd.DataFrame(new_trades)], ignore_index=True)

    return remaining, updated_log


def open_position_row(
    ticker: str,
    regime: str,
    asof_date: str,
    prices: Dict[str, Dict[str, Optional[float]]],
) -> Optional[Dict[str, object]]:
    px = prices.get(ticker, {})
    entry_price = px.get("open")
    high_price = px.get("high")

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
    # Only the ETF choices matter for entries
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

        row = open_position_row(ticker, current_regime, asof_date, prices)
        if row is None:
            continue

        current = pd.concat([current, pd.DataFrame([row])], ignore_index=True)
        held.add(ticker)

    return current


def main() -> None:
    state = load_workbook_state(WORKBOOK_PATH)

    asof_date = state["date"]
    primary_etf = state["primary_etf"]
    secondary_etf = state["secondary_etf"]
    current_regime = state["regime"]
    prices = state["prices"]

    positions = load_positions()
    trade_log = load_trade_log()

    positions = update_trailing_stops(positions, prices)

    exits = build_exit_list(
        positions=positions,
        current_regime=current_regime,
        primary_etf=primary_etf,
        secondary_etf=secondary_etf,
        prices=prices,
    )

    positions, trade_log = apply_exits(
        positions=positions,
        exits=exits,
        asof_date=asof_date,
        prices=prices,
        trade_log=trade_log,
    )

    positions = apply_entries(
        positions=positions,
        current_regime=current_regime,
        primary_etf=primary_etf,
        secondary_etf=secondary_etf,
        asof_date=asof_date,
        prices=prices,
    )

    save_positions(positions)
    save_trade_log(trade_log)
    save_performance(trade_log)

    print(f"ETF paper trading updated for {asof_date}")
    print(f"Primary ETF: {primary_etf}")
    print(f"Secondary ETF: {secondary_etf}")
    print(f"Regime: {current_regime}")
    print(f"Open positions: {len(positions)}")
    print(f"Closed trades logged: {len(trade_log)}")


if __name__ == "__main__":
    main()
