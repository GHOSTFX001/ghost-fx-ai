"""
GHOST FX AI - Gold Signal Bot (V2, step 2)
Step 1 (kept): "bot started", feed-lost / feed-restored warnings, daily alive message.
Step 2 (new): finds today's A levels from Deriv 30M candles, using the same rules as
the V4 TradingView indicator, and posts them to Telegram so you can compare with the chart.
  - session opens 23:00 WAT, closes 22:00 WAT next day
  - A level = open of a bearish 30M candle that directly follows a bullish 30M candle,
    both inside the same session
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

# session opens 23:00 WAT = 22:00 UTC
SESSION_OPEN_UTC_HOUR = 22
M30 = 1800

app = Flask(__name__)

state = {
    "started_at": None,
    "feed_ok": None,
    "fail_count": 0,
    "last_price": None,
    "last_candle_wat": None,
    "last_check_wat": None,
    "session_levels": [],
}

announced = set()
first_levels_sent = False


def now_wat():
    return datetime.datetime.utcnow() + datetime.timedelta(hours=1)


def to_wat(epoch):
    return datetime.datetime.utcfromtimestamp(epoch) + datetime.timedelta(hours=1)


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


# ---------------------------------------------------------------
# A-level rules (same as the V4 indicator)
# ---------------------------------------------------------------
def session_key(epoch):
    return (epoch - SESSION_OPEN_UTC_HOUR * 3600) // 86400


def find_levels(candles30, now_epoch):
    """All A levels in the candles given, numbered per session in order of formation."""
    done = [c for c in candles30 if c["time"] + M30 <= now_epoch]
    counts = {}
    levels = []
    for i in range(1, len(done)):
        prev = done[i - 1]
        cur = done[i]
        if not (cur["close"] < cur["open"] and prev["close"] > prev["open"]):
            continue
        s = session_key(cur["time"] + M30)
        if session_key(prev["time"]) != s:
            continue
        counts[s] = counts.get(s, 0) + 1
        levels.append({
            "session": s,
            "n": counts[s],
            "price": cur["open"],
            "candle_time": cur["time"],
        })
    return levels


def check_levels():
    """Fetch 30M candles, find this session's levels, announce new ones."""
    global first_levels_sent
    candles30 = fetch_candles(M30, 120)
    if candles30 is None:
        return
    now_epoch = int(time.time())
    cur_session = session_key(now_epoch)
    mine = [lv for lv in find_levels(candles30, now_epoch) if lv["session"] == cur_session]

    state["session_levels"] = [
        {"name": "A" + str(lv["n"]), "price": lv["price"], "candle_wat": to_wat(lv["candle_time"]).strftime("%m-%d %H:%M")}
        for lv in mine
    ]

    if not first_levels_sent:
        first_levels_sent = True
        for lv in mine:
            announced.add((lv["session"], lv["n"], lv["candle_time"]))
        if len(mine) == 0:
            send_telegram("No A levels yet in the current session (opened 23:00 WAT).")
        else:
            lines = ["A levels this session (opened 23:00 WAT):"]
            for lv in mine:
                lines.append("A" + str(lv["n"]) + "  " + format(lv["price"], ".2f") + "  (30M candle " + to_wat(lv["candle_time"]).strftime("%H:%M") + " WAT)")
            send_telegram("\n".join(lines))
        return

    for lv in mine:
        key = (lv["session"], lv["n"], lv["candle_time"])
        if key not in announced:
            announced.add(key)
            send_telegram("New A level: A" + str(lv["n"]) + "  " + format(lv["price"], ".2f") + "  (30M candle " + to_wat(lv["candle_time"]).strftime("%H:%M") + " WAT)")


# ---------------------------------------------------------------
# Background worker
# ---------------------------------------------------------------
def worker():
    state["started_at"] = now_wat().strftime("%Y-%m-%d %H:%M")
    send_telegram("GHOST FX bot started (" + state["started_at"] + " WAT). Step 2: posting A levels. Checking the Deriv gold feed every " + str(POLL_SECONDS) + "s.")
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
                state["last_candle_wat"] = to_wat(last["time"]).strftime("%Y-%m-%d %H:%M")
                check_levels()

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
