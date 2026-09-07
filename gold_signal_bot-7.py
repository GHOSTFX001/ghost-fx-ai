"""
GHOST FX AI — Gold Signal Bot (V1)
-----------------------------------
Objective (V1): Replace the MT5-terminal data feed with a free Twelve Data
feed, re-implement the A-level + bearish engulfing confirmation pipeline,
and send confirmed signals to Telegram. Designed to run 24/7 as a free
Render Web Service (Flask keeps a port open; the actual work happens in
a background thread).

Strategy recap (from spec):
- Symbol: XAU/USD only
- A-level: on the 30M chart, scanning forward from gold session open
  (~21:00-22:00 UTC / 22:00-23:00 WAT), find a bullish candle immediately
  followed by a bearish candle. A-level price = OPEN of that bearish candle.
  Up to 5 A-levels tracked per day.
- A level is invalid if price never returns to touch it within 2 days.
- Once touched (wick or body) on the 1M chart, a single 20-candle window
  opens. A bearish engulfing pattern (2nd candle bearish, engulfs the 1st,
  and touches the level) must complete inside that window, or the level is
  permanently invalid (single touch only, no retest).
- Entry = close of the confirming (engulfing) candle.
- Stop loss = the higher high of the two engulfing candles.
- Take profit = fixed 1:7 risk/reward.
- Sell-side only for now. Max 5 signals/day (capped by the 5 A-levels/day).
"""

import os
import time
import threading
import datetime
import requests
from flask import Flask

# ---------------------------------------------------------------------------
# Config (set these as environment variables in Render — never hardcode keys)
# ---------------------------------------------------------------------------
TWELVE_DATA_API_KEY = os.environ.get("TWELVE_DATA_API_KEY", "")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

SYMBOL = "XAU/USD"
POLL_SECONDS = 180          # how often the loop checks for new candles (3 min)
SESSION_START_UTC_HOUR = 21  # ~21:00 UTC = ~22:00 WAT (adjust if your broker's
                             # session open differs — this is the one number
                             # you should double check against your own chart)
MAX_LEVELS_PER_DAY = 5
CONFIRM_WINDOW_CANDLES = 20
LEVEL_EXPIRY_DAYS = 2
RISK_REWARD = 7

# ---------------------------------------------------------------------------
# Twelve Data fetch
# ---------------------------------------------------------------------------
def fetch_candles(interval: str, outputsize: int = 30):
    """Returns candles oldest -> newest as a list of dicts."""
    url = "https://api.twelvedata.com/time_series"
    params = {
        "symbol": SYMBOL,
        "interval": interval,
        "outputsize": outputsize,
        "timezone": "UTC",
        "apikey": TWELVE_DATA_API_KEY,
    }
    try:
        resp = requests.get(url, params=params, timeout=15)
        data = resp.json()
    except Exception as e:
        print(f"[fetch_candles] request failed: {e}", flush=True)
        return []

    if "values" not in data:
        if data.get("status") == "error":
            print(f"[fetch_candles] TWELVE DATA ERROR: {data.get('message', data)}", flush=True)
        else:
            print(f"[fetch_candles] unexpected response: {data}", flush=True)
        return []

    candles = []
    for v in data["values"]:
        candles.append({
            "datetime": datetime.datetime.strptime(v["datetime"], "%Y-%m-%d %H:%M:%S")
                        .replace(tzinfo=datetime.timezone.utc),
            "open": float(v["open"]),
            "high": float(v["high"]),
            "low": float(v["low"]),
            "close": float(v["close"]),
        })
    candles.reverse()  # Twelve Data returns newest-first; we want oldest-first
    return candles

# ---------------------------------------------------------------------------
# Candle helpers
# ---------------------------------------------------------------------------
def is_bullish(c):
    return c["close"] > c["open"]

def is_bearish(c):
    return c["close"] < c["open"]

def engulfs(c2, c1):
    """c2 (bearish) engulfs c1 (bullish) body."""
    return c2["open"] >= c1["close"] and c2["close"] <= c1["open"]

def touches(c, price, tolerance=1.5):
    return (c["low"] - tolerance) <= price <= (c["high"] + tolerance)

def current_session_start(now_utc):
    start = now_utc.replace(hour=SESSION_START_UTC_HOUR, minute=0, second=0, microsecond=0)
    if now_utc < start:
        start -= datetime.timedelta(days=1)
    return start

# ---------------------------------------------------------------------------
# A-level detection (30M)
# ---------------------------------------------------------------------------
def detect_a_levels(candles_30m, existing_levels):
    now = datetime.datetime.now(datetime.timezone.utc)
    session_start = current_session_start(now)

    # Only look for NEW levels formed since the current session started,
    # and only if we haven't already hit today's cap.
    todays_levels = [l for l in existing_levels if l["formed_at"] >= session_start]
    if len(todays_levels) >= MAX_LEVELS_PER_DAY:
        return existing_levels

    session_candles = [c for c in candles_30m if c["datetime"] >= session_start]
    known_times = {l["formed_at"] for l in existing_levels}

    for i in range(len(session_candles) - 1):
        c1, c2 = session_candles[i], session_candles[i + 1]
        if is_bullish(c1) and is_bearish(c2) and c2["datetime"] not in known_times:
            existing_levels.append({
                "price": c2["open"],
                "formed_at": c2["datetime"],
                "touched": False,
                "touch_time": None,
                "confirmed": False,
                "invalid": False,
            })
            known_times.add(c2["datetime"])
            todays_levels = [l for l in existing_levels if l["formed_at"] >= session_start]
            if len(todays_levels) >= MAX_LEVELS_PER_DAY:
                break
    return existing_levels

# ---------------------------------------------------------------------------
# Confirmation logic (1M)
# ---------------------------------------------------------------------------
def check_confirmation(level, candles_1m):
    if level["invalid"] or level["confirmed"]:
        return None

    now = datetime.datetime.now(datetime.timezone.utc)

    # 2-day expiry if never touched
    if not level["touched"] and (now - level["formed_at"]) > datetime.timedelta(days=LEVEL_EXPIRY_DAYS):
        level["invalid"] = True
        return None

    relevant = [c for c in candles_1m if c["datetime"] >= level["formed_at"]]

    # Look for first touch
    if not level["touched"]:
        for idx, c in enumerate(relevant):
            if touches(c, level["price"]):
                level["touched"] = True
                level["touch_time"] = c["datetime"]
                break

    if not level["touched"]:
        return None

    # Within the 20-candle window after touch, look for the bearish engulfing
    window = [c for c in relevant if c["datetime"] >= level["touch_time"]]

    # Always scan the valid portion of the window for a confirming pattern first,
    # regardless of how much time has passed since the last check.
    scan_window = window[:CONFIRM_WINDOW_CANDLES + 1]
    for i in range(len(scan_window) - 1):
        c1, c2 = scan_window[i], scan_window[i + 1]
        if is_bullish(c1) and is_bearish(c2) and engulfs(c2, c1) and touches(c2, level["price"]):
            entry = c2["close"]
            sl = max(c1["high"], c2["high"])
            risk = sl - entry
            tp = entry - RISK_REWARD * risk
            level["confirmed"] = True
            return {
                "entry": round(entry, 2),
                "sl": round(sl, 2),
                "tp": round(tp, 2),
                "level_price": round(level["price"], 2),
                "time": c2["datetime"],
            }

    # Only now check whether the window has fully expired without a match.
    if len(window) > CONFIRM_WINDOW_CANDLES + 1:
        level["invalid"] = True

    return None

# ---------------------------------------------------------------------------
# Telegram
# ---------------------------------------------------------------------------
def send_telegram(message: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print(f"[telegram not configured] {message}")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        requests.post(url, data={"chat_id": TELEGRAM_CHAT_ID, "text": message}, timeout=10)
    except Exception as e:
        print(f"[send_telegram] failed: {e}")

def format_signal(sig):
    return (
        f"🔻 GHOST FX AI — SELL SIGNAL (XAUUSD)\n"
        f"A-level: {sig['level_price']}\n"
        f"Entry: {sig['entry']}\n"
        f"SL: {sig['sl']}\n"
        f"TP: {sig['tp']}\n"
        f"Confirmed: {sig['time'].strftime('%Y-%m-%d %H:%M UTC')}"
    )

# ---------------------------------------------------------------------------
# Main monitoring loop
# ---------------------------------------------------------------------------
active_levels = []

def monitoring_loop():
    global active_levels
    print("Monitoring loop started.", flush=True)
    send_telegram("✅ GHOST FX AI bot deployed and Telegram is connected. Watching XAUUSD now.")

    while True:
        try:
            candles_30m = fetch_candles("30min", 48)
            candles_1m = fetch_candles("1min", 600)

            if candles_30m:
                active_levels = detect_a_levels(candles_30m, active_levels)

            if candles_1m:
                for level in active_levels:
                    signal = check_confirmation(level, candles_1m)
                    if signal:
                        send_telegram(format_signal(signal))
                        print(f"Signal sent: {signal}", flush=True)

            now_str = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
            last_1m = candles_1m[-1] if candles_1m else None
            last_1m_str = (f"O:{last_1m['open']} H:{last_1m['high']} L:{last_1m['low']} C:{last_1m['close']} "
                            f"@ {last_1m['datetime'].strftime('%H:%M:%S UTC')}") if last_1m else "none"
            print(f"[heartbeat {now_str}] 30m candles: {len(candles_30m)} | 1m candles: {len(candles_1m)} | "
                  f"active levels: {len(active_levels)} | touched: {sum(1 for l in active_levels if l['touched'])} | "
                  f"confirmed: {sum(1 for l in active_levels if l['confirmed'])} | latest 1m: {last_1m_str}", flush=True)
            for i, lvl in enumerate(active_levels):
                print(f"  level {i+1}: price={round(lvl['price'], 2)} formed={lvl['formed_at'].strftime('%m-%d %H:%M UTC')} "
                      f"touched={lvl['touched']} touch_time={lvl['touch_time'].strftime('%H:%M:%S UTC') if lvl['touch_time'] else '-'} "
                      f"confirmed={lvl['confirmed']} invalid={lvl['invalid']}", flush=True)

            # trim old invalid/confirmed levels older than 3 days to keep memory tidy
            cutoff = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=3)
            active_levels = [l for l in active_levels if l["formed_at"] >= cutoff]

        except Exception as e:
            print(f"[monitoring_loop] error: {e}", flush=True)

        time.sleep(POLL_SECONDS)

# ---------------------------------------------------------------------------
# Flask app (keeps Render Web Service alive / pingable)
# ---------------------------------------------------------------------------
app = Flask(__name__)

@app.route("/")
def health():
    return "GHOST FX AI signal bot is running.", 200

if __name__ == "__main__":
    threading.Thread(target=monitoring_loop, daemon=True).start()
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
