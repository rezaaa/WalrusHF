from __future__ import annotations

import base64
import html
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
import subprocess
import sys
import threading
import time
from collections import deque
from pathlib import Path
from urllib.parse import urlsplit

from dotenv import load_dotenv

from task_store import (
    DATA_DIR,
    DOWNLOAD_DIR,
    FAILED_FILE,
    QUEUE_DIR,
    QUEUE_FILE,
    SESSION_DIR,
    clear_worker_pid,
    ensure_storage_dirs,
    human_size,
    load_processing,
    load_runtime_settings,
    queue_size,
    read_failed_entries,
    runtime_path,
)


load_dotenv()
ensure_storage_dirs()

BASE_DIR = Path(__file__).resolve().parent
LOG_LINES: deque[str] = deque(maxlen=250)
STATE_LOCK = threading.Lock()
STOP_EVENT = threading.Event()

telegram_proc: subprocess.Popen | None = None
rubika_proc: subprocess.Popen | None = None
supervisor_started = False


def append_log(source: str, text: str) -> None:
    line = text.rstrip()
    if not line:
        return
    timestamp = time.strftime("%H:%M:%S")
    formatted = f"[{timestamp}] {source}: {line}"
    print(formatted, flush=True)
    with STATE_LOCK:
        LOG_LINES.append(formatted)


def decode_secret_file(env_name: str, output_path: Path) -> None:
    encoded = os.getenv(env_name, "").strip()
    if not encoded or output_path.exists():
        return

    try:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_bytes(base64.b64decode(encoded))
        append_log("setup", f"decoded {env_name} to {output_path.name}")
    except Exception as error:
        append_log("setup", f"failed to decode {env_name}: {error}")


def decode_session_secrets() -> None:
    settings = load_runtime_settings()
    rubika_session = runtime_path(settings["rubika_session"], SESSION_DIR)
    if rubika_session.suffix == "":
        rubika_session = rubika_session.with_suffix(".rp")

    telegram_session = runtime_path(
        os.getenv("TELEGRAM_SESSION", "walrus").strip() or "walrus",
        SESSION_DIR,
    )
    if telegram_session.suffix == "":
        telegram_session = telegram_session.with_suffix(".session")

    decode_secret_file("RUBIKA_SESSION_B64", rubika_session)
    decode_secret_file("TELEGRAM_SESSION_B64", telegram_session)


def stream_process_output(name: str, proc: subprocess.Popen) -> None:
    if proc.stdout is None:
        return

    for line in proc.stdout:
        append_log(name, line)


def start_process(script_name: str, name: str) -> subprocess.Popen:
    env = {**os.environ, "PYTHONUNBUFFERED": "1"}
    proc = subprocess.Popen(
        [sys.executable, "-u", str(BASE_DIR / script_name)],
        cwd=str(BASE_DIR),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    threading.Thread(target=stream_process_output, args=(name, proc), daemon=True).start()
    append_log("supervisor", f"started {name} with pid {proc.pid}")
    return proc


def required_env_status() -> list[str]:
    checks = {
        "API_ID": os.getenv("API_ID", "").strip(),
        "API_HASH": os.getenv("API_HASH", "").strip(),
        "BOT_TOKEN": os.getenv("BOT_TOKEN", "").strip(),
    }
    missing = []
    for name, value in checks.items():
        if not value or value == "0" or value == name or value.startswith("your_"):
            missing.append(name)
    return missing


def supervisor_loop() -> None:
    global telegram_proc, rubika_proc

    decode_session_secrets()
    logged_missing: tuple[str, ...] | None = None

    while not STOP_EVENT.is_set():
        missing = tuple(required_env_status())
        if missing:
            if missing != logged_missing:
                append_log("setup", f"missing required secrets: {', '.join(missing)}")
                logged_missing = missing
            time.sleep(5)
            continue

        logged_missing = None

        if telegram_proc is None:
            telegram_proc = start_process("telegram_bot.py", "telegram")
        elif telegram_proc.poll() is not None:
            append_log("telegram", f"exited with code {telegram_proc.returncode}")
            telegram_proc = None

        if rubika_proc is None:
            rubika_proc = start_process("rubika_worker.py", "rubika")
        elif rubika_proc.poll() is not None:
            append_log("rubika", f"exited with code {rubika_proc.returncode}; restarting")
            clear_worker_pid()
            rubika_proc = None

        time.sleep(2)


def ensure_supervisor() -> None:
    global supervisor_started
    if supervisor_started:
        return

    supervisor_started = True
    threading.Thread(target=supervisor_loop, daemon=True).start()


def proc_label(proc: subprocess.Popen | None) -> str:
    if proc is None:
        return "not started"
    code = proc.poll()
    if code is None:
        return f"running (pid {proc.pid})"
    return f"stopped (exit {code})"


def storage_size(path: Path) -> int:
    if not path.exists():
        return 0
    if path.is_file():
        return path.stat().st_size
    return sum(item.stat().st_size for item in path.rglob("*") if item.is_file())


def dashboard_text() -> tuple[str, str]:
    ensure_supervisor()
    settings = load_runtime_settings()
    processing = load_processing()
    failed_count = len(read_failed_entries()) if FAILED_FILE.exists() else 0
    queue_count = queue_size() if QUEUE_FILE.exists() else 0

    active = "none"
    if processing:
        active = (
            f"{processing.get('file_name') or Path(processing.get('path', '')).name} "
            f"({int(processing.get('upload_percent', 0) or 0)}%)"
        )

    missing = required_env_status()
    config_text = "ok" if not missing else f"missing {', '.join(missing)}"
    runtime_storage = (
        storage_size(DOWNLOAD_DIR)
        + storage_size(QUEUE_DIR)
        + storage_size(SESSION_DIR)
    )
    status = "\n".join(
        [
            f"Telegram bot: {proc_label(telegram_proc)}",
            f"Rubika worker: {proc_label(rubika_proc)}",
            f"Config: {config_text}",
            f"Rubika session: {settings['rubika_session']}",
            f"Destination: {settings['rubika_target_title']} ({settings['rubika_target']})",
            f"Data dir: {DATA_DIR}",
            f"Queue: {queue_count}",
            f"Active upload: {active}",
            f"Failed transfers: {failed_count}",
            f"Runtime storage: {human_size(runtime_storage)}",
        ]
    )

    with STATE_LOCK:
        logs = "\n".join(LOG_LINES) or "No logs yet."
    return status, logs


def dashboard_payload() -> dict:
    status, logs = dashboard_text()
    return {
        "status": status,
        "logs": logs,
        "updated_at": time.strftime("%H:%M:%S"),
    }


def render_dashboard() -> bytes:
    payload = dashboard_payload()
    page = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Walrus Telegram Bot</title>
  <style>
    :root {{
      color-scheme: dark;
      --bg: #101314;
      --panel: #171c1f;
      --line: #2a3236;
      --text: #f3f7f5;
      --muted: #9ba8a3;
      --accent: #79d69e;
      --warn: #f6c66a;
    }}
    body {{
      margin: 0;
      font-family: ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      background: var(--bg);
      color: var(--text);
    }}
    main {{
      max-width: 980px;
      margin: 0 auto;
      padding: 40px 20px;
    }}
    h1 {{
      margin: 0 0 8px;
      font-size: 32px;
      letter-spacing: 0;
    }}
    p {{
      color: var(--muted);
      margin: 0 0 24px;
    }}
    .topline {{
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 16px;
      margin-bottom: 24px;
    }}
    .topline p {{
      margin: 0;
    }}
    .live {{
      flex: 0 0 auto;
      color: var(--accent);
      border: 1px solid var(--line);
      border-radius: 999px;
      padding: 7px 10px;
      font-size: 12px;
      font-weight: 700;
    }}
    .live[data-state="stale"] {{
      color: var(--warn);
    }}
    section {{
      border: 1px solid var(--line);
      background: var(--panel);
      border-radius: 8px;
      margin-top: 16px;
      overflow: hidden;
    }}
    h2 {{
      font-size: 14px;
      font-weight: 700;
      color: var(--accent);
      margin: 0;
      padding: 14px 16px;
      border-bottom: 1px solid var(--line);
      text-transform: uppercase;
      letter-spacing: 0;
    }}
    pre {{
      margin: 0;
      padding: 16px;
      overflow: auto;
      white-space: pre-wrap;
      font: 13px/1.55 ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
    }}
    a {{ color: var(--accent); }}
  </style>
</head>
<body>
  <main>
    <h1>Walrus Telegram Bot</h1>
    <div class="topline">
      <p>This Space keeps the Telegram bot and Rubika upload worker running. Use Telegram as the control panel.</p>
      <span id="live" class="live">Live</span>
    </div>
    <section>
      <h2>Status</h2>
      <pre id="status">{html.escape(payload["status"])}</pre>
    </section>
    <section>
      <h2>Logs</h2>
      <pre id="logs">{html.escape(payload["logs"])}</pre>
    </section>
    <noscript>
      <p>JavaScript is disabled. Refresh the page to update status.</p>
    </noscript>
  </main>
  <script>
    const statusEl = document.getElementById("status");
    const logsEl = document.getElementById("logs");
    const liveEl = document.getElementById("live");

    async function refreshDashboard() {{
      try {{
        const response = await fetch("/status.json", {{ cache: "no-store" }});
        if (!response.ok) throw new Error(`HTTP ${{response.status}}`);
        const data = await response.json();
        statusEl.textContent = data.status || "";
        logsEl.textContent = data.logs || "";
        liveEl.textContent = `Live · ${{data.updated_at || "--:--:--"}}`;
        liveEl.dataset.state = "live";
      }} catch (error) {{
        liveEl.textContent = "Live paused";
        liveEl.dataset.state = "stale";
      }}
    }}

    refreshDashboard();
    setInterval(refreshDashboard, 2000);
  </script>
</body>
</html>
"""
    return page.encode("utf-8")


class DashboardHandler(BaseHTTPRequestHandler):
    def send_body(self, body: bytes, content_type: str) -> None:
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:
        path = urlsplit(self.path).path
        if path == "/health":
            self.send_body(b"ok\n", "text/plain; charset=utf-8")
            return
        if path == "/status.json":
            self.send_body(
                json.dumps(dashboard_payload()).encode("utf-8"),
                "application/json; charset=utf-8",
            )
            return

        self.send_body(render_dashboard(), "text/html; charset=utf-8")

    def do_HEAD(self) -> None:
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.end_headers()

    def log_message(self, _format: str, *_args) -> None:
        return


if __name__ == "__main__":
    ensure_supervisor()
    port = int(os.getenv("PORT", "7860"))
    server = ThreadingHTTPServer(("0.0.0.0", port), DashboardHandler)
    append_log("web", f"serving dashboard on 0.0.0.0:{port}")
    server.serve_forever()
