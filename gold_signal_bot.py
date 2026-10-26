"""
GHOST FX AI - Gold Signal Bot (V4, step 4)
Same rules as the V4 TradingView indicator, running on Deriv data:
  - session opens 23:00 WAT, closes 22:00 WAT next day
  - A level = open of a bearish 30M candle that directly follows a bullish 30M candle (same session)
  - when a 1M candle touches a level, that level gets its own 20-candle window
  - inside the window, a bearish engulfing whose bearish candle touches that same level
    (wick or body) is a SELL signal; that level is then finished
  - every level is separate; max 3 signals per session
  - entry = close of the engulfing candle, SL = higher high of the two engulfing candles,
    TP = 1:7, lot = risk % of balance / (SL distance x contract size)
Risk rules (step 4):
  - week target: stop for the week once the week reaches WEEK_TARGET_R (default 15R)
  - loss limit: after WEEK_LOSS_LIMIT losses (default 6) with no win in the week, stop for the week
  - month target: once the month reaches MONTH_TARGET_PERCENT (default 10%), no signals or levels
    for the rest of the month
  - a week starts at the Sunday 23:00 WAT session open
  - results are what the strategy would have done on the price feed (TP or SL hit first;
    if one candle touches both it counts as a loss), not what you actually traded
The bot rebuilds the week and month from Deriv history on every start, so a Render restart
cannot make it forget. Secrets and account settings come from Render environment variables only.
"""

import os
import json
import time
import bisect
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


def env_float(name, default):
    try:
        return float(os.environ.get(name, str(default)))
    except Exception:
        return float(default)


ACCOUNT_BALANCE = env_float("ACCOUNT_BALANCE", 10000)
RISK_PERCENT = env_float("RISK_PERCENT", 0.5)
WEEK_TARGET_R = env_float("WEEK_TARGET_R", 15)
MONTH_TARGET_PERCENT = env_float("MONTH_TARGET_PERCENT", 10)
WEEK_LOSS_LIMIT = int(env_float("WEEK_LOSS_LIMIT", 6))

DERIV_URL = "wss://api.derivws.com/trading/v1/options/ws/public"
DERIV_SYMBOL = "frxXAUUSD"
POLL_SECONDS = 30
FAILS_BEFORE_ALERT = 5
ALIVE_HOUR_WAT = 7

SESSION_OPEN_UTC_HOUR = 22   # 23:00 WAT
WEEK_OFFSET = 3 * 86400 + 22 * 3600   # first Sunday 22:00 UTC after 1970-01-01
M30 = 1800
M1 = 60
WINDOW_CANDLES = 20
MAX_SIGNALS = 3
RISK_REWARD = 7.0
CONTRACT_OZ = 100.0
STALE_SECONDS = 300          # never send a signal older than this

app = Flask(__name__)

state = {
    "started_at": None,
    "feed_ok": None,
    "fail_count": 0,
    "last_price": None,
    "last_candle_wat": None,
    "last_check_wat": None,
    "session_levels": [],
    "history_ready": False,
    "paused": None,
    "week_r": None,
    "week_wins_losses": None,
    "month_percent": None,
    "active_windows": [],
}

announced_levels = set()
announced_signals = set()
announced_results = set()
announced_pauses = set()
first_levels_sent = False
first_signals_done = False
candles1 = {}
last_candle_update = 0
history = {"session": None, "taken": [], "ready": False, "failed_since": None}


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
def fetch_candles(granularity, count, end="latest"):
    """Returns candles oldest -> newest, or None if the feed failed."""
    try:
        ws = websocket.create_connection(DERIV_URL, timeout=20)
        ws.send(json.dumps({
            "ticks_history": DERIV_SYMBOL,
            "style": "candles",
            "granularity": granularity,
            "count": count,
            "end": end,
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


def fetch_range(granularity, start_epoch, end_epoch):
    """Older history in chunks of 5000 candles. Returns oldest -> newest, or None if the first request fails."""
    got = {}
    end = end_epoch
    for i in range(25):
        cs = fetch_candles(granularity, 5000, end)
        if not cs:
            if i == 0:
                return None
            break
        for c in cs:
            got[c["time"]] = c
        earliest = min(c["time"] for c in cs)
        if earliest <= start_epoch:
            break
        end = earliest - 1
    return sorted(got.values(), key=lambda x: x["time"])


def update_candles1():
    """Keeps a local store of 1M candles: full fetch at start / after a gap, small top-ups otherwise."""
    global last_candle_update
    now_epoch = int(time.time())
    full = len(candles1) == 0 or (now_epoch - last_candle_update) > 240
    cs = fetch_candles(M1, 1500 if full else 6)
    if cs is None or len(cs) == 0:
        return False
    for c in cs:
        candles1[c["time"]] = c
    if len(candles1) > 2600:
        old = sorted(candles1.keys())[:len(candles1) - 2000]
        for t in old:
            del candles1[t]
    last_candle_update = now_epoch
    return True


# ---------------------------------------------------------------
# Time helpers
# ---------------------------------------------------------------
def session_key(epoch):
    return (epoch - SESSION_OPEN_UTC_HOUR * 3600) // 86400


def session_start_epoch(sess):
    return sess * 86400 + SESSION_OPEN_UTC_HOUR * 3600


def week_key(epoch):
    return (epoch - WEEK_OFFSET) // 604800


def week_start_epoch(wk):
    return wk * 604800 + WEEK_OFFSET


def month_key(epoch):
    d = to_wat(epoch)
    return d.year * 100 + d.month


def month_start_epoch(epoch):
    d = to_wat(epoch)
    first = datetime.datetime(d.year, d.month, 1)
    return int((first - datetime.datetime(1970, 1, 1)).total_seconds()) - 3600


# ---------------------------------------------------------------
# Strategy rules
# ---------------------------------------------------------------
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


def make_signal(lv, c, prev):
    entry = c["close"]
    sl = max(c["high"], prev["high"])
    risk_pts = sl - entry
    tp = entry - RISK_REWARD * risk_pts
    risk_usd = ACCOUNT_BALANCE * RISK_PERCENT / 100.0
    lot = 0.01
    if risk_pts > 0:
        lot = risk_usd / (risk_pts * CONTRACT_OZ)
    min_lot_note = lot < 0.01
    lot = max(0.01, round(lot, 2))
    return {
        "session": lv["session"],
        "level_n": lv["n"],
        "level_price": lv["price"],
        "candle_time": c["time"],
        "close_time": c["time"] + M1,
        "entry": entry,
        "sl": sl,
        "tp": tp,
        "lot": lot,
        "risk_usd": risk_usd,
        "min_lot_note": min_lot_note,
    }


def simulate(levels, closed, sess):
    """Replays the session's closed 1M candles (oldest -> newest) and returns the SELL signals."""
    sess_start = session_start_epoch(sess)
    st = []
    for lv in levels:
        st.append({
            "lv": lv,
            "born": lv["candle_time"] + M30,
            "win_end": None,
            "dead": False,
        })
    signals = []
    count = 0
    for idx in range(len(closed)):
        c = closed[idx]
        if c["time"] < sess_start:
            continue
        prev = closed[idx - 1] if idx > 0 else None

        # expire old windows, open a window on the first touch
        for s in st:
            if s["dead"] or c["time"] < s["born"]:
                continue
            if s["win_end"] is not None and idx > s["win_end"]:
                s["dead"] = True
                continue
            if s["win_end"] is None and c["high"] >= s["lv"]["price"]:
                s["win_end"] = idx + WINDOW_CANDLES - 1

        if prev is None:
            continue
        engulf = (c["close"] < c["open"] and prev["close"] > prev["open"]
                  and c["open"] >= prev["close"] and c["close"] <= prev["open"])
        if not engulf:
            continue

        for s in st:
            if s["dead"] or s["win_end"] is None or c["time"] < s["born"]:
                continue
            if idx > s["win_end"]:
                continue
            p = s["lv"]["price"]
            if c["high"] >= p and c["low"] <= p and count < MAX_SIGNALS:
                count += 1
                s["dead"] = True
                signals.append(make_signal(s["lv"], c, prev))
    return signals, st


# ---------------------------------------------------------------
# Results and risk rules
# ---------------------------------------------------------------
def resolve_outcome(sg, times, cl):
    """TP or SL first, after the signal candle. Same candle touching both counts as a loss."""
    i = bisect.bisect_right(times, sg["candle_time"])
    while i < len(cl):
        c = cl[i]
        if c["high"] >= sg["sl"]:
            return "loss", c["time"] + M1
        if c["low"] <= sg["tp"]:
            return "win", c["time"] + M1
        i += 1
    return "open", None


def stats_at(taken, t, wk, mo):
    """Resolved results up to time t: (week wins, week losses, week R, month R)."""
    wins = 0
    losses = 0
    r_month = 0.0
    for x in taken:
        if x["outcome"] == "open" or x["resolve_time"] is None or x["resolve_time"] > t:
            continue
        r = RISK_REWARD if x["outcome"] == "win" else -1.0
        if x["mo"] == mo:
            r_month += r
        if x["wk"] == wk:
            if x["outcome"] == "win":
                wins += 1
            else:
                losses += 1
    return wins, losses, wins * RISK_REWARD - losses, r_month


def pause_reason(wins, losses, r_week, r_month):
    if r_month * RISK_PERCENT >= MONTH_TARGET_PERCENT:
        return "month"
    if r_week >= WEEK_TARGET_R:
        return "week_target"
    if wins == 0 and losses >= WEEK_LOSS_LIMIT:
        return "week_losses"
    return None


def evaluate_chain(signals, prior_taken, times, cl):
    """Apply the week and month rules to signals in time order. Returns (newly taken, skipped)."""
    taken_all = list(prior_taken)
    new_taken = []
    skipped = []
    for sg in sorted(signals, key=lambda x: (x["close_time"], x["level_n"])):
        wk = week_key(sg["close_time"])
        mo = month_key(sg["close_time"])
        w, l, rw, rm = stats_at(taken_all, sg["close_time"], wk, mo)
        reason = pause_reason(w, l, rw, rm)
        if reason is not None:
            skipped.append((sg, reason))
            continue
        out, rt = resolve_outcome(sg, times, cl)
        item = {"sg": sg, "outcome": out, "resolve_time": rt, "wk": wk, "mo": mo}
        taken_all.append(item)
        new_taken.append(item)
    return new_taken, skipped


def refresh_open_outcomes(taken, times, cl):
    for x in taken:
        if len(times) > 0 and x["sg"]["close_time"] < times[0]:
            continue
        if x["outcome"] == "open":
            out, rt = resolve_outcome(x["sg"], times, cl)
            x["outcome"] = out
            x["resolve_time"] = rt


def build_history(now_epoch):
    """Rebuild this week's and month's results from Deriv history, up to the start of the current session."""
    cur_session = session_key(now_epoch)
    ws = week_start_epoch(week_key(now_epoch))
    ms = month_start_epoch(now_epoch)
    start = min(ws, ms) - 2 * 86400
    log("[history] building from " + to_wat(start).strftime("%Y-%m-%d"))
    c30 = fetch_range(M30, start, now_epoch)
    c1 = fetch_range(M1, start, now_epoch)
    if c30 is None or c1 is None or len(c1) == 0:
        return False
    closed = [c for c in c1 if c["time"] + M1 <= now_epoch]
    times = [c["time"] for c in closed]
    levels_all = find_levels(c30, now_epoch)
    first_session = session_key(min(ws, ms))
    taken = []
    for s in range(first_session, cur_session):
        lv_s = [lv for lv in levels_all if lv["session"] == s]
        if len(lv_s) == 0:
            continue
        lo = bisect.bisect_left(times, session_start_epoch(s) - 3600)
        hi = bisect.bisect_left(times, session_start_epoch(s + 1) + 3600)
        sub = closed[lo:hi]
        sigs, st = simulate(lv_s, sub, s)
        new_taken, skipped = evaluate_chain(sigs, taken, times, closed)
        taken.extend(new_taken)
    history["taken"] = taken
    history["session"] = cur_session
    history["ready"] = True
    history["failed_since"] = None
    log("[history] ready, " + str(len(taken)) + " earlier trades this month")
    return True


# ---------------------------------------------------------------
# Messages
# ---------------------------------------------------------------
def pause_text(reason, w, l, rw, rm):
    if reason == "month":
        return "MONTHLY TARGET REACHED: " + format(rm * RISK_PERCENT, "+.1f") + "% this month. No more signals or levels until the new month starts."
    if reason == "week_target":
        return "WEEKLY TARGET REACHED: " + format(rw, "+.0f") + "R this week. No more trades until the new week opens (Sunday 23:00 WAT)."
    return "WEEKLY LOSS LIMIT: " + str(l) + " losses and no win this week. No more trades until the new week opens (Sunday 23:00 WAT)."


def signal_text(sg, k, total, w, l, rw, rm):
    lines = []
    head = "SELL XAUUSD  (level A" + str(sg["level_n"]) + " " + format(sg["level_price"], ".2f") + ")"
    if total > 1:
        head = head + "  - trade " + str(k) + " of " + str(total) + " on this candle"
    lines.append(head)
    lines.append("Entry: " + format(sg["entry"], ".2f"))
    lines.append("SL: " + format(sg["sl"], ".2f"))
    lines.append("TP: " + format(sg["tp"], ".2f"))
    lines.append("Lot: " + format(sg["lot"], ".2f") + "  (risk " + format(RISK_PERCENT, "g") + "% of " + format(ACCOUNT_BALANCE, "g") + " = $" + format(sg["risk_usd"], ".2f") + ")")
    if sg["min_lot_note"]:
        lines.append("Note: the lot hit the 0.01 minimum, so this trade risks more than your set %.")
    lines.append("Week so far: " + format(rw, "+.0f") + "R (" + str(w) + " wins, " + str(l) + " losses) | Month: " + format(rm * RISK_PERCENT, "+.1f") + "%")
    lines.append("1M candle closed " + to_wat(sg["close_time"]).strftime("%H:%M") + " WAT. Prices are from the Deriv feed: check your Goat price before entering.")
    return "\n".join(lines)


def result_text(x, w, l, rw, rm):
    sg = x["sg"]
    word = "hit TP (+" + format(RISK_REWARD, "g") + "R)" if x["outcome"] == "win" else "hit SL (-1R)"
    return ("Result: the A" + str(sg["level_n"]) + " trade (entry " + format(sg["entry"], ".2f") + ") " + word + ".\n"
            "Week: " + format(rw, "+.0f") + "R (" + str(w) + " wins, " + str(l) + " losses) | Month: " + format(rm * RISK_PERCENT, "+.1f") + "%")


# ---------------------------------------------------------------
# Checks run by the worker
# ---------------------------------------------------------------
def get_levels():
    candles30 = fetch_candles(M30, 120)
    if candles30 is None:
        return None
    now_epoch = int(time.time())
    cur_session = session_key(now_epoch)
    return [lv for lv in find_levels(candles30, now_epoch) if lv["session"] == cur_session]


def post_levels(mine):
    global first_levels_sent
    state["session_levels"] = [
        {"name": "A" + str(lv["n"]), "price": lv["price"], "candle_wat": to_wat(lv["candle_time"]).strftime("%m-%d %H:%M")}
        for lv in mine
    ]
    if not first_levels_sent:
        first_levels_sent = True
        for lv in mine:
            announced_levels.add((lv["session"], lv["n"], lv["candle_time"]))
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
        if key not in announced_levels:
            announced_levels.add(key)
            send_telegram("New A level: A" + str(lv["n"]) + "  " + format(lv["price"], ".2f") + "  (30M candle " + to_wat(lv["candle_time"]).strftime("%H:%M") + " WAT)")


def run_checks(levels):
    """One full pass: signals, results, risk rules, messages."""
    global first_signals_done
    now_epoch = int(time.time())
    sess = session_key(now_epoch)
    wk = week_key(now_epoch)
    mo = month_key(now_epoch)
    closed = [c for c in sorted(candles1.values(), key=lambda x: x["time"]) if c["time"] + M1 <= now_epoch]
    times = [c["time"] for c in closed]

    prior = history["taken"]
    refresh_open_outcomes(prior, times, closed)
    signals, st = simulate(levels, closed, sess)
    new_taken, skipped = evaluate_chain(signals, prior, times, closed)
    all_taken = prior + new_taken
    refresh_open_outcomes(all_taken, times, closed)

    w, l, rw, rm = stats_at(all_taken, now_epoch, wk, mo)
    reason = pause_reason(w, l, rw, rm)
    state["week_r"] = rw
    state["week_wins_losses"] = str(w) + "W/" + str(l) + "L"
    state["month_percent"] = round(rm * RISK_PERCENT, 2)
    state["paused"] = reason
    state["active_windows"] = [
        {"level": "A" + str(s["lv"]["n"]), "price": s["lv"]["price"]}
        for s in st if (not s["dead"]) and s["win_end"] is not None
    ]

    # first pass after a start: remember what already happened, send nothing old
    if not first_signals_done:
        first_signals_done = True
        for x in all_taken:
            sg = x["sg"]
            announced_signals.add((sg["session"], sg["level_n"], sg["candle_time"]))
            if x["outcome"] != "open":
                announced_results.add((sg["session"], sg["level_n"], sg["candle_time"]))
        if len(new_taken) > 0:
            send_telegram("Already fired earlier this session: " + str(len(new_taken)) + " signal(s). They are not re-sent. Week: " + format(rw, "+.0f") + "R (" + str(w) + " wins, " + str(l) + " losses) | Month: " + format(rm * RISK_PERCENT, "+.1f") + "%")
        if reason is not None:
            announced_pauses.add((wk if reason != "month" else mo, reason))
            send_telegram("Status: " + pause_text(reason, w, l, rw, rm))
        return

    # results of earlier trades
    for x in all_taken:
        sg = x["sg"]
        key = (sg["session"], sg["level_n"], sg["candle_time"])
        if x["outcome"] != "open" and key not in announced_results:
            announced_results.add(key)
            ww, ll, rrw, rrm = stats_at(all_taken, now_epoch, wk, mo)
            send_telegram(result_text(x, ww, ll, rrw, rrm))

    # new signals (only if not paused at the time of the signal)
    by_candle = {}
    for x in new_taken:
        t = x["sg"]["candle_time"]
        by_candle[t] = by_candle.get(t, 0) + 1
    seen = {}
    for x in new_taken:
        sg = x["sg"]
        key = (sg["session"], sg["level_n"], sg["candle_time"])
        k = seen.get(sg["candle_time"], 0) + 1
        seen[sg["candle_time"]] = k
        if key in announced_signals:
            continue
        announced_signals.add(key)
        age = now_epoch - sg["close_time"]
        if age <= STALE_SECONDS:
            w0, l0, rw0, rm0 = stats_at(all_taken, sg["close_time"], x["wk"], x["mo"])
            send_telegram(signal_text(sg, k, by_candle[sg["candle_time"]], w0, l0, rw0, rm0))
        else:
            log("[signal] skipped stale signal, age " + str(age) + "s")

    # pause notice (once per week or month)
    if reason is not None:
        pkey = (wk if reason != "month" else mo, reason)
        if pkey not in announced_pauses:
            announced_pauses.add(pkey)
            send_telegram(pause_text(reason, w, l, rw, rm))


# ---------------------------------------------------------------
# Background worker
# ---------------------------------------------------------------
def worker():
    state["started_at"] = now_wat().strftime("%Y-%m-%d %H:%M")
    send_telegram("GHOST FX bot started (" + state["started_at"] + " WAT). Step 4: signals plus week/month rules. Risk " + format(RISK_PERCENT, "g") + "% of " + format(ACCOUNT_BALANCE, "g") + ", week target " + format(WEEK_TARGET_R, "g") + "R, month target " + format(MONTH_TARGET_PERCENT, "g") + "%, week stops after " + str(WEEK_LOSS_LIMIT) + " losses with no win.")
    last_alive_date = None

    while True:
        try:
            ok = update_candles1()
            state["last_check_wat"] = now_wat().strftime("%H:%M:%S")

            if not ok:
                state["fail_count"] += 1
                if state["fail_count"] >= FAILS_BEFORE_ALERT and state["feed_ok"] is not False:
                    state["feed_ok"] = False
                    send_telegram("FEED LOST: no gold data from Deriv for " + str(state["fail_count"]) + " checks in a row. Signals will not work until it is back.")
            else:
                if state["feed_ok"] is False:
                    send_telegram("Feed restored. Gold data is flowing again.")
                state["feed_ok"] = True
                state["fail_count"] = 0
                last = candles1[max(candles1.keys())]
                state["last_price"] = last["close"]
                state["last_candle_wat"] = to_wat(last["time"]).strftime("%Y-%m-%d %H:%M")

                now_epoch = int(time.time())
                if history["session"] != session_key(now_epoch):
                    history["ready"] = False
                    if not build_history(now_epoch):
                        if history["failed_since"] is None:
                            history["failed_since"] = now_epoch
                        elif now_epoch - history["failed_since"] > 900 and ("hist", 0) not in announced_pauses:
                            announced_pauses.add(("hist", 0))
                            send_telegram("WARNING: cannot load price history, so the week/month rules cannot be checked. No signals will be sent until it loads.")
                state["history_ready"] = history["ready"]

                if history["ready"]:
                    levels = get_levels()
                    if levels is not None:
                        paused_now = state["paused"] is not None
                        if not paused_now:
                            post_levels(levels)
                        run_checks(levels)

            t = now_wat()
            if t.hour == ALIVE_HOUR_WAT and last_alive_date != t.date():
                last_alive_date = t.date()
                send_telegram("GHOST FX alive. Feed ok: " + str(state["feed_ok"]) + ". Gold " + str(state["last_price"]) + ", last candle " + str(state["last_candle_wat"]) + " WAT. Week " + str(state["week_r"]) + "R, month " + str(state["month_percent"]) + "%, paused: " + str(state["paused"]) + ". (If the candle is old, the market may be closed.)")
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
