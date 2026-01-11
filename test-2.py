import yfinance as yf
import pandas as pd
import numpy as np

# -----------------------------
# CONFIG
# -----------------------------
TICKER = "SPY"
START = "2012-01-01"
WINDOW = 252        # rolling window for Markov fit
VOL_WINDOW = 20
VWAP_WINDOW = 20

# -----------------------------
# 1. Download data
# -----------------------------
df = yf.download(TICKER, start=START, auto_adjust=True)
df = df[["Close", "Volume"]].dropna()

# -----------------------------
# 2. Features
# -----------------------------
df["ret"] = df["Close"].pct_change()

# VWAP (rolling)
df["vwap"] = (
    (df["Close"] * df["Volume"])
    .rolling(VWAP_WINDOW)
    .sum()
    / df["Volume"].rolling(VWAP_WINDOW).sum()
)

# Volume regime
df["vol_mean"] = df["Volume"].rolling(VOL_WINDOW).mean()

# -----------------------------
# 3. Define states
# -----------------------------
def state(row):
    direction = "BULL" if row["ret"] > 0 else "BEAR"
    vol = "HIGHVOL" if row["Volume"] > row["vol_mean"] else "LOWVOL"
    vwap = "ABOVE_VWAP" if row["Close"] > row["vwap"] else "BELOW_VWAP"
    return f"{direction}_{vol}_{vwap}"

df = df.dropna()
df["state"] = df.apply(state, axis=1)

# -----------------------------
# 4. 2nd-order Markov backtest
# -----------------------------
predictions = []
actuals = []

for i in range(WINDOW + 2, len(df) - 1):
    train = df.iloc[i - WINDOW:i]

    # Build transitions: (state[t-2], state[t-1]) -> state[t]
    transitions = {}

    for j in range(2, len(train)):
        key = (train["state"].iloc[j-2], train["state"].iloc[j-1])
        nxt = train["state"].iloc[j]

        transitions.setdefault(key, {})
        transitions[key][nxt] = transitions[key].get(nxt, 0) + 1

    prev2 = df["state"].iloc[i-2]
    prev1 = df["state"].iloc[i-1]
    key = (prev2, prev1)

    if key not in transitions:
        continue

    probs = transitions[key]
    total = sum(probs.values())

    bull_prob = sum(v for k, v in probs.items() if k.startswith("BULL")) / total
    bear_prob = 1 - bull_prob

    predictions.append(bull_prob > bear_prob)
    actuals.append(df["ret"].iloc[i+1] > 0)

# -----------------------------
# 5. Evaluation
# -----------------------------
predictions = np.array(predictions)
actuals = np.array(actuals)

accuracy = (predictions == actuals).mean()

# Strategy return
strategy_ret = df["ret"].iloc[-len(predictions):][predictions].sum()
buy_hold_ret = df["ret"].iloc[-len(predictions):].sum()

print("\n--- RESULTS ---")
print(f"Accuracy: {accuracy:.3f}")
print(f"Strategy return: {strategy_ret:.3f}")
print(f"Buy & Hold return: {buy_hold_ret:.3f}")
