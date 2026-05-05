from __future__ import annotations

import base64
import os
import subprocess
import sys
import threading
import time
from collections import deque
from pathlib import Path

import gradio as gr
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
    save_runtime_settings,
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
    with STATE_LOCK:
        LOG_LINES.append(f"[{timestamp}] {source}: {line}")


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
    proc = subprocess.Popen(
        [sys.executable, str(BASE_DIR / script_name)],
        cwd=str(BASE_DIR),
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
    return [name for name, value in checks.items() if not value or value == "0"]


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


def dashboard() -> tuple[str, str]:
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


def save_uploaded_session(session_file: str | None, session_name: str) -> str:
    if not session_file:
        return "Upload a Rubika .rp session file first."

    name = (
        session_name or os.getenv("RUBIKA_SESSION", "rubika_session")
    ).strip() or "rubika_session"
    target = runtime_path(name, SESSION_DIR)
    if target.suffix == "":
        target = target.with_suffix(".rp")

    target.write_bytes(Path(session_file).read_bytes())
    current_settings = load_runtime_settings()
    save_runtime_settings(
        {
            **current_settings,
            "rubika_session": str(target.with_suffix("") if target.suffix == ".rp" else target),
        }
    )
    append_log("setup", f"saved Rubika session to {target.name}")
    return (
        f"Saved Rubika session to {target.name}. "
        "Restart the Space if the worker was already using another session."
    )


with gr.Blocks(title="Walrus Telegram Bot") as demo:
    gr.Markdown("# Walrus Telegram Bot")
    gr.Markdown(
        "This Space keeps the Telegram bot and Rubika upload worker running. "
        "Use Telegram as the control panel."
    )

    with gr.Row():
        status_box = gr.Textbox(label="Status", lines=10)
        log_box = gr.Textbox(label="Logs", lines=10)

    refresh = gr.Button("Refresh")
    refresh.click(dashboard, outputs=[status_box, log_box])
    demo.load(dashboard, outputs=[status_box, log_box])

    with gr.Accordion("Rubika session upload", open=False):
        session_file = gr.File(label="Rubika .rp session file", type="filepath")
        session_name = gr.Textbox(
            label="Session name",
            value=os.getenv("RUBIKA_SESSION", "rubika_session"),
        )
        setup_output = gr.Textbox(label="Setup status")
        save_session = gr.Button("Save session")
        save_session.click(
            save_uploaded_session,
            inputs=[session_file, session_name],
            outputs=setup_output,
        )


if __name__ == "__main__":
    ensure_supervisor()
    demo.queue().launch(
        server_name="0.0.0.0",
        server_port=int(os.getenv("PORT", "7860")),
    )
