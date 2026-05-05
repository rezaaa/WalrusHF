---
title: WalrusHF
emoji: ⛵
colorFrom: blue
colorTo: green
sdk: gradio
python_version: '3.11'
app_file: app.py
pinned: false
---

# WalrusHF

[فارسی](README.fa.md)

WalrusHF runs a Telegram bot inside a Hugging Face Space and uploads received files to Rubika. Telegram is the control panel; the Space page is a live dashboard for process health, queue state, storage, and logs.

## Features

- Accept Telegram files and direct `http://` or `https://` file links
- Download files inside the Space runtime
- Queue Rubika uploads so transfers do not overlap
- Upload to Rubika Saved Messages or a Rubika channel
- Show Telegram progress for download, queue, upload, retries, and failures
- Support cancel, cleanup, retry, retry-all, and Rubika login commands

## Install

<img width="1536" height="1024" alt="Generated image 1" src="https://github.com/user-attachments/assets/45187c3e-650a-4435-b9a4-4cd1bf44d4f3" />


### 1. Create A Hugging Face Space

Create a new Space with these settings:

- **Space SDK:** `Gradio`
- **Gradio template:** `Blank`
- **Hardware:** `CPU Basic`
- **Visibility:** `Private` is recommended
- **Space name:** any name, for example `walrushf`

### 2. Add A Storage Bucket

On the same Create Space page, enable **Mount a bucket to this Space**:

- **Bucket:** create a new private bucket, or mount an existing private bucket
- **Mount path:** `/data`
- **Access mode:** `Read & Write`

WalrusHF stores sessions, downloads, and queue data under `/data/walrus`. Without durable `/data`, the app falls back to `/tmp/walrus`, which can be lost when the Space restarts.

### 3. Deploy From GitHub

Clone this repo and push it to your Space:

```bash
git clone git@github.com:rezaaa/walrushf.git
cd walrushf
git remote add space https://huggingface.co/spaces/USERNAME/SPACE_NAME
git push space main:main
```

Replace `USERNAME/SPACE_NAME` with your Hugging Face Space path.

If the Space already has starter files and the push is rejected:

```bash
git push --force-with-lease space main:main
```

### 4. Finish Setup

1. Add the required secrets below in the Space settings.
2. Restart the Space.
3. Open your Telegram bot and send `/start`.

Hugging Face runs [app.py](app.py). It starts the Telegram bot, the Rubika upload worker, and the dashboard on port `7860`.

## Required Secrets

Add these in **Space settings -> Variables and secrets -> Secrets**:

```env
API_ID=123456
API_HASH=your_telegram_api_hash
BOT_TOKEN=123456:your_bot_token
OWNER_TELEGRAM_ID=123456789
```

Where to get them:

- `API_ID` and `API_HASH`: https://my.telegram.org
- `BOT_TOKEN`: create a Telegram bot with BotFather
- `OWNER_TELEGRAM_ID`: your numeric Telegram user ID

`OWNER_TELEGRAM_ID` is strongly recommended. If it is missing or invalid, anyone who can message the bot can use it.

## Optional Variables

Add these only if you want to change the defaults:

```env
TELEGRAM_SESSION=walrus
RUBIKA_SESSION=rubika_session
RUBIKA_TARGET=me
RUBIKA_TARGET_TITLE=Saved Messages
WALRUS_MAX_FILE_BYTES=8589934592
WALRUS_MIN_FREE_BYTES=536870912
```

Notes:

- `RUBIKA_TARGET=me` uploads to Rubika Saved Messages.
- `WALRUS_MAX_FILE_BYTES` defaults to 8 GiB.
- Set `WALRUS_MAX_FILE_BYTES=0` to disable the app-level file size limit.
- `file://` links are disabled by default. Enable them only if you understand the risk:

```env
WALRUS_ALLOW_FILE_URLS=true
```

## Rubika Login

The easiest setup is through Telegram:

1. Start the Space.
2. Open your Telegram bot.
3. Send `/start`.
4. If no Rubika session exists, WalrusHF asks for the Rubika phone number.
5. Send the OTP or password when prompted.

After login, the Rubika session is saved under `/data/walrus/sessions`.

## Bot Commands

- `/start` - open setup or main menu
- `/settings` - show Rubika account and destination
- `/set_rubika` - start Rubika login
- `/status` - show queue, active transfers, failures, and storage
- `/transfers` - list active, queued, and retryable transfers
- `/cleanup` - preview removable downloaded files
- `/cleanup confirm` - delete safe cleanup candidates
- `/cancel` - show cancel buttons
- `/retry <task_id>` - retry one failed transfer
- `/retry_all` - retry all retryable failed transfers

## Dashboard

The Space page updates live every 2 seconds. Useful endpoints:

```text
/health
/status.json
```

Useful dashboard checks:

- `Telegram bot: running` means the Telegram process is alive.
- `Rubika worker: running` means the upload worker is alive.
- `Config: ok` means required secrets are present.
- `Queue` shows waiting upload jobs.
- `Active upload` shows the current Rubika worker task.

## Troubleshooting

If the bot does not respond:

- Check the Space logs.
- Confirm `API_ID`, `API_HASH`, and `BOT_TOKEN` are real values, not placeholders.
- Confirm `OWNER_TELEGRAM_ID` is numeric.
- Restart the Space after changing secrets.

If uploads stay queued:

- Check dashboard logs for `Rubika worker`.
- Run `/transfers` in Telegram.
- Make sure Rubika login has completed with `/start` or `/set_rubika`.
- Confirm the bucket or persistent storage is mounted at `/data`.

If the main progress message does not update:

- The Rubika worker writes progress events locally.
- The Telegram bot applies those events through Pyrogram.
- Check logs for `Telegram event bridge failed`.

## Local Test

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python app.py
```

Then open:

```text
http://localhost:7860
```

## Safety

This project is for personal transfer workflows, research, and experimentation. Do not use it for spam, abuse, unauthorized access, privacy violations, or unlawful activity. You are responsible for respecting platform rules, local laws, and other people's rights.
