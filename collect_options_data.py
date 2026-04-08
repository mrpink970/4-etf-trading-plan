#!/usr/bin/env python3
"""
Options Data Collector for SOXL PMCC Strategy
Collects daily LEAPS prices, short-term call premiums, and implied volatility
Runs alongside existing ETF data collection
"""

import yfinance as yf
import pandas as pd
from datetime import datetime
import sys
import warnings
warnings.filterwarnings('ignore')

# Configuration - Adjust these based on your strategy
LEAPS_EXPIRATION = "2027-01-15"  # Target LEAPS expiration (Jan 2027)
LEAPS_STRIKE = 56  # Deep ITM call strike (adjust based on current price)
SHORT_STRIKE_PCT = 1.10  # Sell calls 10% above current price (110% of spot)
SHORT_DAYS_TO_EXPIRATION = 7  # Weekly calls (7 days)

# File to save options data
OPTIONS_DATA_PATH = "options_data.csv"


def get_nearest_expiration(expirations, target_days=7):
    """Get the expiration date closest to target days from now"""
    from datetime import datetime, timedelta
    
    today = datetime.now().date()
    target_date = today + timedelta(days=target_days)
    
    # Parse expiration dates and find closest to target
    exp_dates = []
    for exp in expirations:
        try:
            exp_date = datetime.strptime(exp, "%Y-%m-%d").date()
            if exp_date > today:
                exp_dates.append((exp_date, exp))
        except:
            continue
    
    if not exp_dates:
        return None
    
    # Find closest expiration to target date
    closest = min(exp_dates, key=lambda x: abs((x[0] - target_date).days))
    return closest[1]


def collect_options_data():
    """Collect current options data for SOXL"""
    
    print("=" * 60)
    print("OPTIONS DATA COLLECTOR - SOXL PMCC")
    print("=" * 60)
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Initialize ticker
    soxl = yf.Ticker("SOXL")
    
    # Get current stock price
    hist = soxl.history(period="2d")
    if hist.empty:
        print("ERROR: Could not fetch SOXL price data")
        return None
    
    current_price = hist["Close"].iloc[-1]
    prev_close = hist["Close"].iloc[-2] if len(hist) > 1 else current_price
    daily_change = ((current_price - prev_close) / prev_close) * 100
    
    current_date = datetime.now().strftime("%Y-%m-%d")
    
    print(f"\nSOXL Current Price: ${current_price:.2f} ({daily_change:+.2f}%)")
    
    # Get available expiration dates
    expirations = soxl.options
    if not expirations:
        print("ERROR: No option expiration dates available")
        return None
    
    print(f"Available expirations: {len(expirations)} dates")
    
    # ============================================================
    # 1. Get LEAPS data (long leg for PMCC)
    # ============================================================
    leaps_data = {
        "strike": LEAPS_STRIKE,
        "expiration": LEAPS_EXPIRATION,
        "bid": None,
        "ask": None,
        "mid": None,
        "implied_volatility": None,
        "open_interest": None,
        "volume": None,
        "available": False
    }
    
    if LEAPS_EXPIRATION in expirations:
        try:
            chain = soxl.option_chain(LEAPS_EXPIRATION)
            calls = chain.calls
            
            # Find the specific strike
            leaps = calls[calls["strike"] == LEAPS_STRIKE]
            
            if not leaps.empty:
                leaps_row = leaps.iloc[0]
                leaps_data["bid"] = round(leaps_row["bid"], 2) if leaps_row["bid"] > 0 else None
                leaps_data["ask"] = round(leaps_row["ask"], 2) if leaps_row["ask"] > 0 else None
                if leaps_data["bid"] and leaps_data["ask"]:
                    leaps_data["mid"] = round((leaps_data["bid"] + leaps_data["ask"]) / 2, 2)
                leaps_data["implied_volatility"] = round(leaps_row["impliedVolatility"] * 100, 2) if leaps_row["impliedVolatility"] else None
                leaps_data["open_interest"] = int(leaps_row["openInterest"]) if leaps_row["openInterest"] else 0
                leaps_data["volume"] = int(leaps_row["volume"]) if leaps_row["volume"] else 0
                leaps_data["available"] = True
                
                print(f"\n📊 LEAPS (Long Leg): {LEAPS_EXPIRATION} ${LEAPS_STRIKE} Call")
                print(f"   Bid: ${leaps_data['bid']} | Ask: ${leaps_data['ask']} | Mid: ${leaps_data['mid']}")
                print(f"   IV: {leaps_data['implied_volatility']}% | OI: {leaps_data['open_interest']}")
            else:
                print(f"\n⚠️ LEAPS strike ${LEAPS_STRIKE} not found for {LEAPS_EXPIRATION}")
                
        except Exception as e:
            print(f"Error fetching LEAPS data: {e}")
    else:
        print(f"\n⚠️ LEAPS expiration {LEAPS_EXPIRATION} not available")
        print(f"   Nearest expirations: {expirations[:5]}")
    
    # ============================================================
    # 2. Get short-term call premium (short leg for PMCC)
    # ============================================================
    short_data = {
        "expiration": None,
        "days_to_exp": None,
        "strike": None,
        "bid": None,
        "ask": None,
        "mid": None,
        "implied_volatility": None,
        "open_interest": None,
        "volume": None,
        "available": False
    }
    
    # Find nearest weekly expiration
    target_strike = round(current_price * SHORT_STRIKE_PCT, 1)
    nearest_exp = get_nearest_expiration(expirations, SHORT_DAYS_TO_EXPIRATION)
    
    if nearest_exp:
        try:
            chain = soxl.option_chain(nearest_exp)
            calls = chain.calls
            
            # Find OTM calls at or above target strike
            otm_calls = calls[calls["strike"] >= target_strike]
            
            if not otm_calls.empty:
                short_call = otm_calls.iloc[0]
                short_data["expiration"] = nearest_exp
                short_data["strike"] = short_call["strike"]
                short_data["bid"] = round(short_call["bid"], 2) if short_call["bid"] > 0 else None
                short_data["ask"] = round(short_call["ask"], 2) if short_call["ask"] > 0 else None
                if short_data["bid"] and short_data["ask"]:
                    short_data["mid"] = round((short_data["bid"] + short_data["ask"]) / 2, 2)
                short_data["implied_volatility"] = round(short_call["impliedVolatility"] * 100, 2) if short_call["impliedVolatility"] else None
                short_data["open_interest"] = int(short_call["openInterest"]) if short_call["openInterest"] else 0
                short_data["volume"] = int(short_call["volume"]) if short_call["volume"] else 0
                short_data["available"] = True
                
                # Calculate days to expiration
                exp_date = datetime.strptime(nearest_exp, "%Y-%m-%d")
                days = (exp_date.date() - datetime.now().date()).days
                short_data["days_to_exp"] = days
                
                print(f"\n📊 Short Call (Short Leg): {nearest_exp} ${short_data['strike']:.1f} Call")
                print(f"   Days to expiry: {days} days")
                print(f"   Bid: ${short_data['bid']} | Ask: ${short_data['ask']} | Mid: ${short_data['mid']}")
                print(f"   IV: {short_data['implied_volatility']}% | OI: {short_data['open_interest']}")
            else:
                print(f"\n⚠️ No OTM calls found at or above ${target_strike:.1f} for {nearest_exp}")
                
        except Exception as e:
            print(f"Error fetching short-term options data: {e}")
    else:
        print(f"\n⚠️ Could not find expiration near {SHORT_DAYS_TO_EXPIRATION} days")
    
    # ============================================================
    # 3. Get put option data for bearish scenarios
    # ============================================================
    put_data = {
        "strike": None,
        "bid": None,
        "ask": None,
        "mid": None,
        "implied_volatility": None,
        "available": False
    }
    
    if len(expirations) > 0:
        try:
            chain = soxl.option_chain(expirations[0])
            puts = chain.puts
            
            # Find OTM puts ~10% below current price
            target_put_strike = round(current_price * 0.90, 1)
            otm_puts = puts[puts["strike"] <= target_put_strike]
            
            if not otm_puts.empty:
                put = otm_puts.iloc[-1]  # Closest to target
                put_data["strike"] = put["strike"]
                put_data["bid"] = round(put["bid"], 2) if put["bid"] > 0 else None
                put_data["ask"] = round(put["ask"], 2) if put["ask"] > 0 else None
                if put_data["bid"] and put_data["ask"]:
                    put_data["mid"] = round((put_data["bid"] + put_data["ask"]) / 2, 2)
                put_data["implied_volatility"] = round(put["impliedVolatility"] * 100, 2) if put["impliedVolatility"] else None
                put_data["available"] = True
                
                print(f"\n📊 Put Reference: {expirations[0]} ${put_data['strike']:.1f} Put")
                print(f"   Bid: ${put_data['bid']} | Ask: ${put_data['ask']} | Mid: ${put_data['mid']}")
                print(f"   IV: {put_data['implied_volatility']}%")
                
        except Exception as e:
            print(f"Error fetching put data: {e}")
    
    # ============================================================
    # 4. Save all data to CSV
    # ============================================================
    
    # Build the data row
    data_row = {
        "date": current_date,
        "soxl_price": round(current_price, 2),
        "soxl_daily_pct": round(daily_change, 2),
        
        # LEAPS data
        "leaps_expiration": LEAPS_EXPIRATION,
        "leaps_strike": LEAPS_STRIKE,
        "leaps_bid": leaps_data["bid"],
        "leaps_ask": leaps_data["ask"],
        "leaps_mid": leaps_data["mid"],
        "leaps_iv": leaps_data["implied_volatility"],
        "leaps_oi": leaps_data["open_interest"],
        "leaps_volume": leaps_data["volume"],
        "leaps_available": leaps_data["available"],
        
        # Short call data
        "short_expiration": short_data["expiration"],
        "short_days": short_data["days_to_exp"],
        "short_strike": short_data["strike"],
        "short_bid": short_data["bid"],
        "short_ask": short_data["ask"],
        "short_mid": short_data["mid"],
        "short_iv": short_data["implied_volatility"],
        "short_oi": short_data["open_interest"],
        "short_volume": short_data["volume"],
        "short_available": short_data["available"],
        
        # Put reference data
        "put_strike": put_data["strike"],
        "put_bid": put_data["bid"],
        "put_ask": put_data["ask"],
        "put_mid": put_data["mid"],
        "put_iv": put_data["implied_volatility"],
        "put_available": put_data["available"],
    }
    
    # Create or append to CSV
    df_new = pd.DataFrame([data_row])
    
    try:
        existing = pd.read_csv(OPTIONS_DATA_PATH)
        df_combined = pd.concat([existing, df_new], ignore_index=True)
        df_combined.to_csv(OPTIONS_DATA_PATH, index=False)
        print(f"\n✅ Appended data to {OPTIONS_DATA_PATH}")
        print(f"   Total rows: {len(df_combined)}")
    except FileNotFoundError:
        df_new.to_csv(OPTIONS_DATA_PATH, index=False)
        print(f"\n✅ Created new file: {OPTIONS_DATA_PATH}")
    
    print("\n" + "=" * 60)
    print("OPTIONS DATA COLLECTION COMPLETE")
    print("=" * 60)
    
    return data_row


def main():
    try:
        collect_options_data()
    except Exception as e:
        print(f"ERROR: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
