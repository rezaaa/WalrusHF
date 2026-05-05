from __future__ import annotations

import atexit
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import gradio as gr
from dotenv import load_dotenv

from task_store import (
    DOWNLOAD_DIR,
    QUEUE_DIR,
    ensure_storage_dirs,
    has_rubika_session,
    human_size,
    load_processing,
    load_runtime_settings,
    read_failed_entries,
    read_queue_tasks,
)


BASE_DIR = Path(__file__).resolve().parent
LOG_DIR = BASE_DIR / "logs"
REQUIRED_ENV_VARS = ("API_ID", "API_HASH", "BOT_TOKEN")
PROCESSES: dict[str, subprocess.Popen] = {}


load_dotenv()
ensure_storage_dirs()
LOG_DIR.mkdir(parents=True, exist_ok=True)


def missing_environment() -> list[str]:
    return [name for name in REQUIRED_ENV_VARS if not os.getenv(name, "").strip()]


def process_is_running(proc: subprocess.Popen | None) -> bool:
    return bool(proc and proc.poll() is None)


def start_process(name: str, script_name: str) -> subprocess.Popen:
    existing = PROCESSES.get(name)
    if process_is_running(existing):
        return existing

    log_path = LOG_DIR / f"{name}.log"
    log_file = open(log_path, "a", encoding="utf-8")
    proc = subprocess.Popen(
        [sys.executable, str(BASE_DIR / script_name)],
        cwd=str(BASE_DIR),
        stdout=log_file,
        stderr=subprocess.STDOUT,
        text=True,
    )
    proc._walrus_log_file = log_file  # type: ignore[attr-defined]
    PROCESSES[name] = proc
    return proc


def start_services() -> str:
    missing = missing_environment()
    if missing:
        return (
            "Missing required Hugging Face Space secrets: "
            + ", ".join(f"`{name}`" for name in missing)
        )

    start_process("rubika_worker", "rubika_worker.py")
    start_process("telegram_bot", "telegram_bot.py")
    return "Walrus services are running."


def stop_services() -> str:
    stopped = []
    for name, proc in list(PROCESSES.items()):
        if process_is_running(proc):
            proc.terminate()
            stopped.append(name)

    deadline = time.time() + 5
    for proc in list(PROCESSES.values()):
        while process_is_running(proc) and time.time() < deadline:
            time.sleep(0.1)
        if process_is_running(proc):
            proc.kill()

        log_file = getattr(proc, "_walrus_log_file", None)
        if log_file:
            log_file.close()

    PROCESSES.clear()
    return "Stopped: " + ", ".join(stopped) if stopped else "No running services to stop."


def stop_and_refresh() -> tuple[str, str]:
    stop_message = stop_services()
    status, logs = dashboard_status()
    return f"{status}\n\n{stop_message}", logs


def shutdown_services() -> None:
    for proc in list(PROCESSES.values()):
        if process_is_running(proc):
            proc.send_signal(signal.SIGTERM)


atexit.register(shutdown_services)


def process_status(name: str) -> str:
    proc = PROCESSES.get(name)
    if process_is_running(proc):
        return f"running (pid {proc.pid})"
    if proc is None:
        return "not started"
    return f"stopped (exit {proc.poll()})"


def directory_size(path: Path) -> int:
    if not path.exists():
        return 0
    return sum(file.stat().st_size for file in path.rglob("*") if file.is_file())


def recent_log(name: str, max_lines: int = 80) -> str:
    path = LOG_DIR / f"{name}.log"
    if not path.exists():
        return ""

    lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    return "\n".join(lines[-max_lines:])


def dashboard_status() -> tuple[str, str]:
    settings = load_runtime_settings()
    queued = read_queue_tasks()
    processing = load_processing()
    failed = read_failed_entries()
    session_name = settings["rubika_session"]

    rows = [
        "# Walrus Space",
        "",
        f"- Telegram bot: {process_status('telegram_bot')}",
        f"- Rubika worker: {process_status('rubika_worker')}",
        f"- Queue: {len(queued)} pending",
        f"- Active upload: {processing.get('file_name', 'yes') if processing else 'none'}",
        f"- Failed transfers: {len(failed)}",
        f"- Downloads size: {human_size(directory_size(DOWNLOAD_DIR))}",
        f"- Queue data size: {human_size(directory_size(QUEUE_DIR))}",
        f"- Rubika session: {session_name}",
        f"- Rubika session file found: {'yes' if has_rubika_session(session_name) else 'no'}",
        f"- Destination: {settings.get('rubika_target_title') or 'Saved Messages'}",
    ]

    missing = missing_environment()
    if missing:
        rows.extend(
            [
                "",
                "## Setup needed",
                "Add these Space secrets, then restart the Space:",
                ", ".join(f"`{name}`" for name in missing),
            ]
        )

    logs = "\n\n".join(
        part
        for part in (
            "telegram_bot.py\n" + recent_log("telegram_bot"),
            "rubika_worker.py\n" + recent_log("rubika_worker"),
        )
        if part.strip()
    )
    return "\n".join(rows), logs or "No logs yet."


def refresh() -> tuple[str, str]:
    start_services()
    return dashboard_status()


start_services()


with gr.Blocks(title="Walrus") as demo:
    gr.Markdown(
        """
        # Walrus
        Keep this Space running, then use your Telegram bot as the control panel.
        """
    )
    with gr.Row():
        start_button = gr.Button("Start", variant="primary")
        stop_button = gr.Button("Stop")
        refresh_button = gr.Button("Refresh")

    status = gr.Markdown()
    logs = gr.Textbox(label="Recent logs", lines=18, interactive=False)

    start_button.click(fn=refresh, outputs=[status, logs])
    stop_button.click(fn=stop_and_refresh, outputs=[status, logs])
    refresh_button.click(fn=dashboard_status, outputs=[status, logs])
    demo.load(fn=refresh, outputs=[status, logs])


if __name__ == "__main__":
    demo.launch()
