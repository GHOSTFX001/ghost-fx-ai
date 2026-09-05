# GHOST FX AI — Deploy Guide (Phone Only, No PC)

Everything below can be done from your phone's browser + the Telegram app.
Use "Desktop site" mode in your browser for GitHub/Render — makes the
buttons easier to tap.

## 1. Get a Twelve Data API key (free)
1. Go to twelvedata.com and sign up (no card needed)
2. Confirm your email
3. Copy your API key from the dashboard — you'll paste it into Render later

## 2. Create your Telegram bot
1. Open Telegram, search for **@BotFather**
2. Send `/newbot`, give it a name and a username
3. BotFather replies with a **bot token** — save it
4. Send any message to your new bot (so it can message you back)
5. Get your chat ID: search for **@userinfobot**, send `/start`, it replies
   with your numeric chat ID — save it

## 3. Put the code on GitHub
1. Go to github.com, sign up if you don't have an account
2. Tap "+" → "New repository", name it e.g. `ghost-fx-ai`, make it Public
3. Tap "uploading an existing file" and upload:
   - `gold_signal_bot.py`
   - `requirements.txt`
4. Commit the files

## 4. Deploy on Render
1. Go to render.com, sign up (can use your GitHub account to sign in)
2. Dashboard → "New +" → "Web Service"
3. Connect your `ghost-fx-ai` GitHub repo
4. Settings:
   - **Runtime:** Python 3
   - **Build Command:** `pip install -r requirements.txt`
   - **Start Command:** `python gold_signal_bot.py`
   - **Instance Type:** Free
5. Under "Environment", add these three variables:
   - `TWELVE_DATA_API_KEY` = (from step 1)
   - `TELEGRAM_BOT_TOKEN` = (from step 2)
   - `TELEGRAM_CHAT_ID` = (from step 2)
6. Click "Create Web Service" — Render will build and deploy it
7. Once live, Render gives you a URL like `https://ghost-fx-ai.onrender.com`
   — save this, you need it for step 5

## 5. Keep it awake 24/7 (free)
1. Go to cron-job.org, sign up free
2. Create a new cron job:
   - URL: your Render URL from step 4
   - Interval: every 10 minutes
3. Save. That's it — this alone keeps your bot from sleeping.

## Test checklist (V1)
- [ ] Render deploy succeeds with no errors in the logs
- [ ] Visiting your Render URL in a browser shows "GHOST FX AI signal bot is running."
- [ ] Render logs show "Monitoring loop started."
- [ ] After a bullish→bearish 30M pair forms during session hours, logs
      show the level being added (add a print if you want to verify this
      manually against your chart)
- [ ] When price returns to a level and a bearish engulfing pattern forms
      on 1M, you receive a Telegram message with entry/SL/TP
- [ ] Cross-check one live signal manually on your TradingView chart to
      confirm entry/SL/TP math matches what you'd calculate by hand

## Known V1 limitations / next-version goals
- Session start time is hardcoded as 21:00 UTC — double-check this against
  your broker's actual gold session open and adjust `SESSION_START_UTC_HOUR`
  in the code if needed
- No persistent storage — if Render restarts your bot, in-memory tracked
  levels reset (acceptable for now since levels only live 2 days max, but
  worth fixing in V2 with a small database)
- Buy-side mirror setup not yet added (sell-side only, as specified)
- No auto-execution yet — this version only sends alerts, matching the
  "signals first, auto-trading later" plan
