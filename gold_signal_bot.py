"""
GHOST FX AI - Gold Signal Bot (V2, step 1)
Step 1 only: prove hosting, Telegram and the Deriv price feed all work.
  - sends "bot started" on every start/restart (shows if Render restarts it)
  - sends a daily "alive" message at 07:00 WAT with the latest gold price
  - sends "FEED LOST" if the Deriv feed fails for several checks in a row
  - sends "feed restored" when it comes back
No trade signals yet. Secrets come from Render environment variables only.
"""

import os
import json
import time
import threading
import datetime
import requests
import websocket
from flask import Flask, jsonify

# ---------------------------------------------------------------
# Config (set in Render > Environment, never in the code)
# ---------------------------------------------------------------
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

DERIV_URL = "wss://api.derivws.com/trading/v1/options/ws/public"
DERIV_SYMBOL = "frxXAUUSD"
POLL_SECONDS = 60
FAILS_BEFORE_ALERT = 5
ALIVE_HOUR_WAT = 7

app = Flask(__name__)

state = {
    "started_at": None,
    "feed_ok": None,
    "fail_count": 0,
    "last_price": None,
    "last_candle_wat": None,
    "last_check_wat": None,
}


def now_wat():
    return datetime.datetime.utcnow() + datetime.timedelta(hours=1)


def log(msg):
    print(msg, flush=True)


def send_telegram(text):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log("[telegram] token or chat id missing in Render environment")
        return False
    try:
        url = "https://api.telegram.org/bot" + TELEGRAM_BOT_TOKEN + "/sendMessage"
        r = requests.post(url, data={"chat_id": TELEGRAM_CHAT_ID, "text": text}, timeout=15)
        if r.status_code != 200:
            log("[telegram] error " + str(r.status_code) + " " + r.text[:200])
            return False
        return True
    except Exception as e:
        log("[telegram] failed: " + str(e))
        return False


# ---------------------------------------------------------------
# Deriv feed
# ---------------------------------------------------------------
def fetch_candles(granularity, count):
    """Returns candles oldest -> newest, or None if the feed failed."""
    try:
        ws = websocket.create_connection(DERIV_URL, timeout=15)
        ws.send(json.dumps({
            "ticks_history": DERIV_SYMBOL,
            "style": "candles",
            "granularity": granularity,
            "count": count,
            "end": "latest",
        }))
        data = json.loads(ws.recv())
        ws.close()
    except Exception as e:
        log("[deriv] request failed: " + str(e))
        return None

    if "candles" not in data:
        log("[deriv] unexpected reply: " + str(data)[:300])
        return None

    out = []
    for c in data["candles"]:
        out.append({
            "time": int(c["epoch"]),
            "open": float(c["open"]),
            "high": float(c["high"]),
            "low": float(c["low"]),
            "close": float(c["close"]),
        })
    return out


def candle_time_wat(epoch):
    return datetime.datetime.utcfromtimestamp(epoch) + datetime.timedelta(hours=1)


# ---------------------------------------------------------------
# Background worker
# ---------------------------------------------------------------
def worker():
    state["started_at"] = now_wat().strftime("%Y-%m-%d %H:%M")
    send_telegram("GHOST FX bot started (" + state["started_at"] + " WAT). Checking the Deriv gold feed every " + str(POLL_SECONDS) + "s.")
    last_alive_date = None

    while True:
        try:
            candles = fetch_candles(60, 3)
            state["last_check_wat"] = now_wat().strftime("%H:%M:%S")

            if candles is None or len(candles) == 0:
                state["fail_count"] += 1
                if state["fail_count"] >= FAILS_BEFORE_ALERT and state["feed_ok"] is not False:
                    state["feed_ok"] = False
                    send_telegram("FEED LOST: no gold data from Deriv for " + str(state["fail_count"]) + " checks in a row. Signals will not work until it is back.")
            else:
                if state["feed_ok"] is False:
                    send_telegram("Feed restored. Gold data is flowing again.")
                state["feed_ok"] = True
                state["fail_count"] = 0
                last = candles[-1]
                state["last_price"] = last["close"]
                state["last_candle_wat"] = candle_time_wat(last["time"]).strftime("%Y-%m-%d %H:%M")

            t = now_wat()
            if t.hour == ALIVE_HOUR_WAT and last_alive_date != t.date():
                last_alive_date = t.date()
                send_telegram("GHOST FX alive. Feed ok: " + str(state["feed_ok"]) + ". Gold " + str(state["last_price"]) + ", last candle " + str(state["last_candle_wat"]) + " WAT. (If the candle is old, the market may be closed.)")
        except Exception as e:
            log("[worker] error: " + str(e))

        time.sleep(POLL_SECONDS)


_worker_started = False


def start_worker():
    global _worker_started
    if _worker_started:
        return
    _worker_started = True
    threading.Thread(target=worker, daemon=True).start()


# ---------------------------------------------------------------
# Web routes (keeps Render awake and lets you check status)
# ---------------------------------------------------------------
@app.route("/")
def home():
    return "GHOST FX bot is running"


@app.route("/status")
def status():
    return jsonify(state)


start_worker()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)
