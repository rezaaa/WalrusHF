---
title: Walrus
emoji: 🚀
colorFrom: blue
colorTo: green
sdk: gradio
sdk_version: 5.0.0
app_file: app.py
pinned: false
---

# Walrus

Walrus is a Hugging Face Spaces app that runs a Telegram bot for transferring files to Rubika.

The Space exposes a small Gradio status page, but Telegram is the real control panel:

- send Telegram files to the bot
- send direct `http://` or `https://` file links
- watch download and upload progress in Telegram
- queue, cancel, retry, and clean transfers
- set up or change the Rubika account from Telegram
- choose Rubika Saved Messages or a recent Rubika channel as the upload destination

## Safety Notice

This project is for personal transfer workflows, research, and experimentation. Do not use it for spam, abuse, unauthorized access, privacy violations, or any unlawful activity. You are responsible for respecting platform rules, local laws, and other people's rights.

## Hugging Face Space Setup

Create a Python Gradio Space and push this repository. Hugging Face will run `app.py`.

Set these Space secrets:

```env
API_ID=your_telegram_api_id
API_HASH=your_telegram_api_hash
BOT_TOKEN=your_telegram_bot_token
OWNER_TELEGRAM_ID=123456789
```

Optional settings:

```env
TELEGRAM_SESSION=walrus
RUBIKA_SESSION=rubika_session
RUBIKA_TARGET=me
RUBIKA_TARGET_TITLE=Saved Messages
WALRUS_MAX_FILE_BYTES=8589934592
WALRUS_MIN_FREE_BYTES=536870912
```

`API_ID` and `API_HASH` come from https://my.telegram.org. `BOT_TOKEN` comes from BotFather.

Set `OWNER_TELEGRAM_ID` so only your Telegram account can use the bot.

## Runtime Storage

The app stores mutable runtime files outside the source checkout:

- `/data/walrus/sessions`
- `/data/walrus/downloads`
- `/data/walrus/queue`

If `/data` is unavailable, it falls back to `/tmp/walrus`.

Free Spaces use ephemeral runtime disk, so sessions, queued files, and retry data can disappear when the Space restarts. For reliable use, enable Hugging Face persistent storage. Persistent storage is mounted at `/data`.

## Rubika Session

You can create the Rubika session from Telegram:

1. Start the Space.
2. Open your Telegram bot.
3. Send `/start`.
4. Follow the Rubika phone, password, and OTP prompts.

You can also upload an existing `.rp` session file from the Space page, or set a base64 encoded session as a Space secret:

```env
RUBIKA_SESSION_B64=...
```

The app decodes `RUBIKA_SESSION_B64` into `/data/walrus/sessions` on startup.

## Transfer Limits

By default, Walrus rejects files larger than 8 GiB to avoid filling the Space disk.

Change the limit with:

```env
WALRUS_MAX_FILE_BYTES=8589934592
```

Set it to `0` to disable the app-level limit.

`file://` links are disabled by default for Spaces. Enable only if you know why:

```env
WALRUS_ALLOW_FILE_URLS=true
```

## Commands

- `/start` - open setup or main menu
- `/settings` - show Rubika account and destination
- `/set_rubika` - start Rubika login
- `/status` - show queue, active transfers, failures, and storage
- `/transfers` - list active, queued, and retryable transfers
- `/cleanup` - preview removable downloaded files
- `/cleanup confirm` - delete safe cleanup candidates
- `/cancel` - show cancel buttons
- `/retry <task_id>` - retry a failed transfer
- `/retry_all` - retry all retryable failed transfers

## Local Space-Style Run

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python app.py
```

The Gradio status page listens on `0.0.0.0:${PORT:-7860}`.
