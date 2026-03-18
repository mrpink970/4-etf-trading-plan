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


def normalize_etf(value) -> str:
    if value is None:
        return ""
    return str(value).strip().upper()


def infer_regime(primary_etf: str) -> str:
    if primary_etf in BULL_ETFS:
        return "bull"
    if primary_etf in BEAR_ETFS:
        return "bear"
    return "neutral"


def safe_float(value) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except Exception:
        return None


def find_header_row(rows: List[tuple]) -> int:
    """
    Find the row index containing the Daily_Data headers.
    We identify it by the presence of 'Date' and at least one ETF Open column.
    """
    for idx, row in enumerate(rows):
        values = [str(v).strip() if v is not None else "" for v in row]
        if "Date" in values and any(v.endswith(" Open") for v in values):
            return idx
    raise ValueError("Could not find Daily_Data header row.")


def read_daily_data_wide(daily_ws) -> pd.DataFrame:
    rows = list(daily_ws.iter_rows(values_only=True))
    if not rows:
        raise ValueError("Daily_Data sheet is empty.")

    header_idx = find_header_row(rows)
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
        raise ValueError("Daily_Data has no data rows under the header row.")

    if "Date" not in df.columns:
        raise ValueError("Daily_Data missing Date column after parsing.")

    df = df[df["Date"].notna()].copy()
    df["Date"] = pd.to_datetime(df["Date"]).dt.date

    return df


def extract_latest_prices_from_wide(df: pd.DataFrame) -> tuple[str, Dict[str, Dict[str, float]]]:
    latest_date = df["Date"].max()
    latest_row = df[df["Date"] == latest_date].iloc[-1]

    price_map: Dict[str, Dict[str, float]] = {}

    for etf in sorted(ALL_ETFS):
        open_col = f"{etf} Open"
        high_col = f"{etf} High"
        low_col = f"{etf} Low"
        close_col = f"{etf} Close"

        price_map[etf] = {
            "open": safe_float(latest_row.get(open_col)),
            "high": safe_float(latest_row.get(high_col)),
            "low": safe_float(latest_row.get(low_col)),
            "close": safe_float(latest_row.get(close_col)),
        }

    return str(latest_date), price_map


def load_workbook_data(path: Path) -> Dict[str, object]:
    if not path.exists():
        raise FileNotFoundError(f"Workbook not found: {path}")

    wb = load_workbook(path, data_only=True)

    if "Signal" not in wb.sheetnames or "Daily_Data" not in wb.sheetnames:
        raise ValueError("Workbook must contain 'Signal' and 'Daily_Data' sheets.")

    signal_ws = wb["Signal"]
    daily_ws = wb["Daily_Data"]

    primary_etf = normalize_etf(signal_ws["D23"].value)
    secondary_etf = normalize_etf(signal_ws["D24"].value)
    signal_date_raw = signal_ws["D27"].value

    daily_df = read_daily_data_wide(daily_ws)
    daily_date, price_map = extract_latest_prices_from_wide(daily_df)

    if signal_date_raw is not None:
        signal_date = str(pd.to_datetime(signal_date_raw).date())
    else:
        signal_date = daily_date

    return {
        "date": signal_date,
        "primary_etf": primary_etf,
        "secondary_etf": secondary_etf,
        "regime": infer_regime(primary_etf),
        "prices": price_map,
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
        return pd.read_csv(POSITIONS_PATH)
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
        return pd.read_csv(TRADE_LOG_PATH)
    return pd.DataFrame(columns=cols)


def save_positions(df: pd.DataFrame) -> None:
    df.to_csv(POSITIONS_PATH, index=False)


def save_trade_log(df: pd.DataFrame) -> None:
    df.to_csv(TRADE_LOG_PATH, index=False)


def save_performance(trade_log: pd.DataFrame) -> None:
    if trade_log.empty:
        perf = pd.DataFrame(
            [
                {
                    "total_trades": 0,
                    "win_rate": 0.0,
                    "loss_rate": 0.0,
                    "avg_gain_pct": 0.0,
                    "avg_loss_pct": 0.0,
                    "largest_gain_pct": 0.0,
                    "largest_loss_pct": 0.0,
                    "total_gross_pl": 0.0,
                    "expectancy_pct": 0.0,
                }
            ]
        )
        perf.to_csv(PERFORMANCE_PATH, index=False)
        return

    wins = trade_log[trade_log["gross_pl"] > 0].copy()
    losses = trade_log[trade_log["gross_pl"] < 0].copy()

    total_trades = len(trade_log)
    win_rate = len(wins) / total_trades if total_trades else 0.0
    loss_rate = len(losses) / total_trades if total_trades else 0.0

    avg_gain = wins["return_pct"].mean() if not wins.empty else 0.0
    avg_loss = abs(losses["return_pct"].mean()) if not losses.empty else 0.0
    largest_gain = wins["return_pct"].max() if not wins.empty else 0.0
    largest_loss = losses["return_pct"].min() if not losses.empty else 0.0
    total_gross_pl = trade_log["gross_pl"].sum()

    expectancy = (win_rate * avg_gain) - (loss_rate * avg_loss)

    perf = pd.DataFrame(
        [
            {
                "total_trades": total_trades,
                "win_rate": round(win_rate, 6),
                "loss_rate": round(loss_rate, 6),
                "avg_gain_pct": round(float(avg_gain), 6),
                "avg_loss_pct": round(float(avg_loss), 6),
                "largest_gain_pct": round(float(largest_gain), 6),
                "largest_loss_pct": round(float(largest_loss), 6),
                "total_gross_pl": round(float(total_gross_pl), 6),
                "expectancy_pct": round(float(expectancy), 6),
            }
        ]
    )
    perf.to_csv(PERFORMANCE_PATH, index=False)


def update_trailing_stops(positions: pd.DataFrame, prices: Dict[str, Dict[str, float]]) -> pd.DataFrame:
    if positions.empty:
        return positions

    updated = positions.copy()

    for idx, row in updated.iterrows():
        ticker = normalize_etf(row["ticker"])
        px = prices.get(ticker)
        if not px:
            continue

        high_price = px.get("high")
        if high_price is None:
            continue

        current_highest = safe_float(row["highest_price"])
        if current_highest is None or high_price > current_highest:
            current_highest = high_price

        trailing_stop = current_highest * (1 - TRAILING_STOP_PCT)

        updated.at[idx, "highest_price"] = round(current_highest, 6)
        updated.at[idx, "trailing_stop"] = round(trailing_stop, 6)

    return updated


def build_exit_list(
    positions: pd.DataFrame,
    current_regime: str,
    primary_etf: str,
    secondary_etf: str,
    prices: Dict[str, Dict[str, float]],
) -> List[Dict[str, str]]:
    exits: List[Dict[str, str]] = []
    targets = {primary_etf, secondary_etf}
    targets.discard("")

    for _, row in positions.iterrows():
        ticker = normalize_etf(row["ticker"])
        held_regime = row["regime"]

        # 1. Regime flip exits everything
        if current_regime != "neutral" and held_regime != current_regime:
            exits.append({"ticker": ticker, "reason": "regime_flip"})
            continue

        # 2. Signal negative / no longer selected
        if ticker not in targets:
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
    for e in exits:
        dedup.setdefault(e["ticker"], e["reason"])
    return [{"ticker": k, "reason": v} for k, v in dedup.items()]


def exit_positions(
    positions: pd.DataFrame,
    exits: List[Dict[str, str]],
    asof_date: str,
    prices: Dict[str, Dict[str, float]],
    trade_log: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if positions.empty or not exits:
        return positions, trade_log

    remaining_rows = []
    trade_log_out = trade_log.copy()
    exit_map = {e["ticker"]: e["reason"] for e in exits}

    for _, row in positions.iterrows():
        ticker = normalize_etf(row["ticker"])
        if ticker not in exit_map:
            remaining_rows.append(row.to_dict())
            continue

        px = prices.get(ticker, {})
        exit_price = px.get("open")
        if exit_price is None:
            remaining_rows.append(row.to_dict())
            continue

        entry_price = safe_float(row["entry_price"])
        shares = int(row["shares"])
        gross_pl = (exit_price - entry_price) * shares
        return_pct = ((exit_price / entry_price) - 1) * 100 if entry_price else 0.0

        trade_log_out = pd.concat(
            [
                trade_log_out,
                pd.DataFrame(
                    [
                        {
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
                        }
                    ]
                ),
            ],
            ignore_index=True,
        )

    return pd.DataFrame(remaining_rows), trade_log_out


def build_entries(
    positions: pd.DataFrame,
    current_regime: str,
    primary_etf: str,
    secondary_etf: str,
    asof_date: str,
    prices: Dict[str, Dict[str, float]],
) -> pd.DataFrame:
    if current_regime == "neutral":
        return positions

    current = positions.copy()
    held = set(current["ticker"].astype(str).str.upper().tolist()) if not current.empty else set()

    desired = [normalize_etf(primary_etf), normalize_etf(secondary_etf)]
    desired = [d for d in desired if d in ALL_ETFS]

    for ticker in desired:
        if len(current) >= MAX_TRADES:
            break
        if ticker in held:
            continue

        px = prices.get(ticker, {})
        entry_price = px.get("open")
        if entry_price is None:
            continue

        highest_price = px.get("high") if px.get("high") is not None else entry_price
        trailing_stop = highest_price * (1 - TRAILING_STOP_PCT)

        new_row = pd.DataFrame(
            [
                {
                    "ticker": ticker,
                    "regime": current_regime,
                    "entry_date": asof_date,
                    "entry_price": round(entry_price, 6),
                    "shares": SHARES_PER_TRADE,
                    "highest_price": round(highest_price, 6),
                    "trailing_stop": round(trailing_stop, 6),
                }
            ]
        )

        current = pd.concat([current, new_row], ignore_index=True)
        held.add(ticker)

    return current


def main() -> None:
    data = load_workbook_data(WORKBOOK_PATH)

    asof_date = data["date"]
    primary_etf = data["primary_etf"]
    secondary_etf = data["secondary_etf"]
    current_regime = data["regime"]
    prices = data["prices"]

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

    positions, trade_log = exit_positions(
        positions=positions,
        exits=exits,
        asof_date=asof_date,
        prices=prices,
        trade_log=trade_log,
    )

    positions = build_entries(
        positions=positions,
        current_regime=current_regime,
        primary_etf=primary_etf,
        secondary_etf=secondary_etf,
        asof_date=asof_date,
        prices=prices,
    )

    if positions.empty:
        positions = pd.DataFrame(
            columns=[
                "ticker",
                "regime",
                "entry_date",
                "entry_price",
                "shares",
                "highest_price",
                "trailing_stop",
            ]
        )

    save_positions(positions)
    save_trade_log(trade_log)
    save_performance(trade_log)

    print(f"ETF paper trading updated for {asof_date}")
    print(f"Open positions: {len(positions)}")
    print(f"Closed trades logged: {len(trade_log)}")


if __name__ == "__main__":
    main()
