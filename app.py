from __future__ import annotations

import base64
import html
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
import re
import shutil
import subprocess
import sys
import threading
import time
import uuid
from collections import deque
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlsplit

from dotenv import load_dotenv
import requests

from task_store import (
    DATA_DIR,
    DOWNLOAD_DIR,
    FAILED_FILE,
    QUEUE_DIR,
    QUEUE_FILE,
    SESSION_DIR,
    append_task,
    apply_runtime_settings,
    clear_worker_pid,
    ensure_storage_dirs,
    human_size,
    is_cancelled,
    load_processing,
    load_runtime_settings,
    normalize_upload_filename,
    processing_task_is_active,
    queue_size,
    read_completed_entries,
    read_failed_entries,
    read_queue_tasks,
    remove_queued_task,
    runtime_path,
    safe_filename,
    mark_cancelled,
    cleanup_local_file,
)


load_dotenv()
ensure_storage_dirs()

BASE_DIR = Path(__file__).resolve().parent
LOG_LINES: deque[str] = deque(maxlen=250)
STATE_LOCK = threading.Lock()
STOP_EVENT = threading.Event()
WEB_DOWNLOADS: dict[str, dict] = {}
WEB_DOWNLOAD_LOCK = threading.Lock()


def env_int(name: str, default: int) -> int:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        print(f"ignoring invalid integer value for {name}: {raw!r}", flush=True)
        return default


DIRECT_URL_TIMEOUT = env_int("WALRUS_DIRECT_URL_TIMEOUT", 30)
DIRECT_URL_CHUNK_SIZE = 1024 * 512
MAX_FILE_BYTES = env_int("WALRUS_MAX_FILE_BYTES", 8 * 1024 * 1024 * 1024)
MIN_FREE_BYTES = env_int("WALRUS_MIN_FREE_BYTES", 512 * 1024 * 1024)

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


def interrupt_rubika_worker_for_cancel(task_id: str) -> None:
    global rubika_proc
    proc = rubika_proc
    if proc is None or proc.poll() is not None:
        return

    append_log("supervisor", f"stopping rubika worker to cancel active upload id={task_id}")
    proc.terminate()
    try:
        proc.wait(timeout=3)
    except subprocess.TimeoutExpired:
        append_log("supervisor", f"killing rubika worker after cancel timeout id={task_id}")
        proc.kill()
        try:
            proc.wait(timeout=3)
        except subprocess.TimeoutExpired:
            append_log("supervisor", f"rubika worker did not stop after kill id={task_id}")


def storage_size(path: Path) -> int:
    if not path.exists():
        return 0
    if path.is_file():
        return path.stat().st_size
    return sum(item.stat().st_size for item in path.rglob("*") if item.is_file())


def clean_old_web_downloads() -> None:
    cutoff = time.time() - 10 * 60
    with WEB_DOWNLOAD_LOCK:
        old_task_ids = [
            task_id
            for task_id, item in WEB_DOWNLOADS.items()
            if item.get("finished_at")
            and float(item["finished_at"]) < cutoff
            and item.get("status") in {"completed", "failed", "cancelled"}
        ]
        for task_id in old_task_ids:
            WEB_DOWNLOADS.pop(task_id, None)


def failed_task_by_id() -> dict[str, dict]:
    failed = {}
    for entry in read_failed_entries():
        task = entry.get("task") or {}
        task_id = task.get("task_id")
        if task_id:
            failed[task_id] = entry
    return failed


def completed_task_by_id() -> dict[str, dict]:
    completed = {}
    for entry in read_completed_entries():
        task = entry.get("task") or {}
        task_id = task.get("task_id")
        if task_id:
            completed[task_id] = entry
    return completed


def enrich_web_download(item: dict) -> dict:
    task_id = item.get("task_id", "")
    if not task_id:
        return item

    if item.get("cancel_requested") or is_cancelled(task_id):
        item.update(
            {
                "status": "cancelled",
                "note": item.get("note") or "Cancel requested.",
                "finished_at": item.get("finished_at") or time.time(),
            }
        )
        return item

    queued = {task.get("task_id"): task for task in read_queue_tasks()}
    processing = load_processing()
    completed = completed_task_by_id()
    failed = failed_task_by_id()
    now = time.time()

    if item.get("status") == "downloading":
        return item

    if task_id in completed:
        entry = completed[task_id]
        task = entry.get("task") or {}
        item.update(
            {
                "status": "completed",
                "download_percent": 100,
                "upload_percent": 100,
                "note": "Uploaded to Rubika.",
                "file_name": task.get("file_name") or item.get("file_name"),
                "size": human_size(int(task.get("file_size", 0) or 0)),
                "finished_at": item.get("finished_at") or entry.get("completed_at") or now,
            }
        )
        return item

    processing_active = (
        processing
        and processing.get("task_id") == task_id
        and processing_task_is_active(processing)
    )
    if processing_active:
        item.update(
            {
                "status": "uploading",
                "upload_percent": int(processing.get("upload_percent", 0) or 0),
                "note": processing.get("attempt_text") or "Uploading to Rubika.",
                "file_name": processing.get("file_name") or item.get("file_name"),
                "size": human_size(int(processing.get("file_size", 0) or 0)),
                "finished_at": None,
            }
        )
        return item

    if task_id in queued:
        task = queued[task_id]
        item.update(
            {
                "status": "queued",
                "upload_percent": 0,
                "note": "Waiting for Rubika worker.",
                "file_name": task.get("file_name") or item.get("file_name"),
                "size": human_size(int(task.get("file_size", 0) or 0)),
                "finished_at": None,
            }
        )
        return item

    if task_id in failed:
        entry = failed[task_id]
        task = entry.get("task") or {}
        item.update(
            {
                "status": "failed",
                "upload_percent": int(task.get("upload_percent", 0) or 0),
                "note": entry.get("error") or "Upload failed.",
                "file_name": task.get("file_name") or item.get("file_name"),
                "size": human_size(int(task.get("file_size", 0) or 0)),
                "finished_at": item.get("finished_at") or now,
            }
        )
        return item

    if item.get("status") in {"queued", "uploading"}:
        item.update(
            {
                "status": "completed",
                "upload_percent": 100,
                "note": "Uploaded to Rubika.",
                "finished_at": item.get("finished_at") or now,
            }
        )

    return item


def web_download_snapshot() -> list[dict]:
    clean_old_web_downloads()
    with WEB_DOWNLOAD_LOCK:
        items = [dict(item) for item in WEB_DOWNLOADS.values()]

    enriched = [enrich_web_download(item) for item in items]
    with WEB_DOWNLOAD_LOCK:
        for item in enriched:
            task_id = item.get("task_id")
            if task_id in WEB_DOWNLOADS:
                WEB_DOWNLOADS[task_id].update(item)

    status_order = {
        "downloading": 0,
        "queued": 1,
        "uploading": 2,
        "failed": 3,
        "cancelled": 4,
        "completed": 5,
    }
    return sorted(
        enriched,
        key=lambda item: (
            status_order.get(str(item.get("status")), 9),
            -float(item.get("started_at") or 0),
        ),
    )


def web_task_cancel_requested(task_id: str) -> bool:
    with WEB_DOWNLOAD_LOCK:
        item = WEB_DOWNLOADS.get(task_id) or {}
        return bool(item.get("cancel_requested"))


def update_web_download(task_id: str, **updates) -> None:
    with WEB_DOWNLOAD_LOCK:
        current = WEB_DOWNLOADS.setdefault(
            task_id,
            {
                "task_id": task_id,
                "status": "starting",
                "file_name": "file",
                "download_percent": 0,
                "upload_percent": 0,
                "size": "unknown",
                "url": "",
                "note": "",
                "cancel_requested": False,
            },
        )
        current.update(updates)


def parse_content_disposition_filename(value: str | None) -> str | None:
    if not value:
        return None
    match = re.search(r"filename\*=UTF-8''([^;]+)", value, flags=re.IGNORECASE)
    if match:
        return unquote(match.group(1).strip().strip('"'))
    match = re.search(r'filename="([^"]+)"', value, flags=re.IGNORECASE)
    if match:
        return match.group(1).strip()
    match = re.search(r"filename=([^;]+)", value, flags=re.IGNORECASE)
    if match:
        return match.group(1).strip().strip('"')
    return None


def direct_url_filename(url: str, response: requests.Response) -> str:
    header_name = parse_content_disposition_filename(response.headers.get("content-disposition"))
    if header_name:
        return safe_filename(header_name, "download.bin")

    path_name = Path(unquote(urlsplit(url).path)).name
    return safe_filename(path_name or "download.bin", "download.bin")


def unique_download_path(filename: str) -> Path:
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    path = DOWNLOAD_DIR / filename
    if not path.exists():
        return path

    stem = path.stem
    suffix = path.suffix
    for index in range(1, 1000):
        candidate = DOWNLOAD_DIR / f"{stem} {index}{suffix}"
        if not candidate.exists():
            return candidate

    return DOWNLOAD_DIR / f"{stem} {uuid.uuid4().hex[:8]}{suffix}"


def ensure_download_allowed(total_size: int | None) -> None:
    if total_size and MAX_FILE_BYTES > 0 and total_size > MAX_FILE_BYTES:
        raise RuntimeError(
            f"File is too large ({human_size(total_size)}). Limit is {human_size(MAX_FILE_BYTES)}."
        )

    free_bytes = shutil.disk_usage(DATA_DIR).free
    required_free = (total_size or 0) + MIN_FREE_BYTES
    if total_size and free_bytes < required_free:
        raise RuntimeError(
            f"Not enough free storage. Need {human_size(required_free)}, have {human_size(free_bytes)}."
        )


def download_url_for_upload(task_id: str, url: str) -> None:
    parsed = urlsplit(url)
    if parsed.scheme not in {"http", "https"}:
        update_web_download(
            task_id,
            status="failed",
            note="Only http:// and https:// URLs are supported.",
            finished_at=time.time(),
        )
        return

    started_at = time.time()
    update_web_download(task_id, status="downloading", url=url, started_at=started_at)
    append_log("web-url", f"started download id={task_id} url={url}")

    download_path: Path | None = None
    try:
        if web_task_cancel_requested(task_id):
            raise RuntimeError("Cancelled.")

        with requests.get(url, stream=True, timeout=DIRECT_URL_TIMEOUT) as response:
            response.raise_for_status()
            total_size = int(response.headers.get("content-length") or 0)
            ensure_download_allowed(total_size or None)
            filename = normalize_upload_filename(direct_url_filename(url, response), "download.bin")
            download_path = unique_download_path(filename)
            downloaded = 0

            update_web_download(
                task_id,
                file_name=filename,
                size=human_size(total_size) if total_size else "unknown",
                path=str(download_path),
                download_percent=0,
                upload_percent=0,
            )

            with download_path.open("wb") as file:
                for chunk in response.iter_content(chunk_size=DIRECT_URL_CHUNK_SIZE):
                    if web_task_cancel_requested(task_id):
                        raise RuntimeError("Cancelled.")
                    if not chunk:
                        continue
                    file.write(chunk)
                    downloaded += len(chunk)
                    if MAX_FILE_BYTES > 0 and downloaded > MAX_FILE_BYTES:
                        raise RuntimeError(
                            f"File exceeded limit of {human_size(MAX_FILE_BYTES)}."
                        )
                    percent = int((downloaded * 100) / total_size) if total_size else 0
                    update_web_download(
                        task_id,
                        download_percent=max(0, min(100, percent)),
                        size=human_size(total_size or downloaded),
                    )

        file_size = download_path.stat().st_size
        task = {
            "task_id": task_id,
            "type": "local_file",
            "path": str(download_path),
            "caption": "",
            "file_name": normalize_upload_filename(download_path.name, "download.bin"),
            "file_size": file_size,
            "media_type": "document",
            "started_at": started_at,
            "source": "space_ui",
            "source_url": url,
        }
        apply_runtime_settings(task)
        append_task(task)
        update_web_download(
            task_id,
            status="queued",
            download_percent=100,
            upload_percent=0,
            size=human_size(file_size),
            note="Queued for Rubika upload.",
            finished_at=None,
        )
        append_log("web-url", f"queued upload id={task_id} file={download_path.name}")
    except Exception as error:
        if download_path and download_path.exists():
            try:
                download_path.unlink()
            except OSError:
                pass
        update_web_download(
            task_id,
            status="cancelled" if "cancelled" in str(error).lower() else "failed",
            note=str(error),
            finished_at=time.time(),
        )
        append_log("web-url", f"failed id={task_id} error={error}")


def start_web_url_download(url: str) -> str:
    task_id = uuid.uuid4().hex[:10]
    update_web_download(task_id, status="starting", url=url, started_at=time.time())
    thread = threading.Thread(
        target=download_url_for_upload,
        args=(task_id, url),
        daemon=True,
    )
    thread.start()
    return task_id


def cancel_web_task(task_id: str) -> bool:
    did_cancel = False
    item_status = ""
    with WEB_DOWNLOAD_LOCK:
        item = WEB_DOWNLOADS.get(task_id)
        if item:
            item_status = str(item.get("status") or "")
            item["cancel_requested"] = True
            item["note"] = "Cancel requested."
            did_cancel = True
            if item_status in {"starting", "downloading"}:
                item["status"] = "cancelled"
                item["finished_at"] = time.time()

    queued_task = remove_queued_task(task_id)
    if queued_task:
        cleanup_local_file(queued_task.get("path", ""))
        update_web_download(
            task_id,
            status="cancelled",
            note="Removed from upload queue.",
            finished_at=time.time(),
        )
        did_cancel = True

    processing = load_processing()
    if processing and processing.get("task_id") == task_id:
        mark_cancelled(task_id)
        interrupt_rubika_worker_for_cancel(task_id)
        update_web_download(
            task_id,
            status="cancelled",
            note="Active upload stopped.",
            finished_at=time.time(),
        )
        did_cancel = True
    elif not queued_task and item_status in {"queued", "uploading"}:
        mark_cancelled(task_id)
        update_web_download(
            task_id,
            status="cancelled",
            note="Cancel requested before upload started.",
            finished_at=time.time(),
        )
        did_cancel = True

    if did_cancel:
        append_log("web-url", f"cancel requested id={task_id}")
    return did_cancel


def clear_web_tasks() -> int:
    web_download_snapshot()
    with WEB_DOWNLOAD_LOCK:
        removable = [
            task_id
            for task_id, item in WEB_DOWNLOADS.items()
            if item.get("status") in {"completed", "failed", "cancelled"}
        ]
        for task_id in removable:
            WEB_DOWNLOADS.pop(task_id, None)
    append_log("web-url", f"cleared {len(removable)} web transfer item(s)")
    return len(removable)


def dashboard_snapshot() -> dict:
    ensure_supervisor()
    settings = load_runtime_settings()
    processing = load_processing()
    completed = completed_task_by_id()
    failed_count = len(read_failed_entries()) if FAILED_FILE.exists() else 0
    queue_count = queue_size() if QUEUE_FILE.exists() else 0
    web_downloads = web_download_snapshot()

    upload_percent = 0
    active = "none"
    stale_processing = bool(
        processing
        and (
            not processing_task_is_active(processing)
            or is_cancelled(processing.get("task_id", ""))
            or processing.get("task_id", "") in completed
        )
    )
    if processing and not stale_processing:
        upload_percent = int(processing.get("upload_percent", 0) or 0)
        active = (
            f"{processing.get('file_name') or Path(processing.get('path', '')).name} "
            f"({upload_percent}%)"
        )

    missing = required_env_status()
    config_text = "ok" if not missing else f"missing {', '.join(missing)}"
    telegram_label = proc_label(telegram_proc)
    rubika_label = proc_label(rubika_proc)
    runtime_storage = (
        storage_size(DOWNLOAD_DIR)
        + storage_size(QUEUE_DIR)
        + storage_size(SESSION_DIR)
    )
    status = "\n".join(
        [
            f"Telegram bot: {telegram_label}",
            f"Rubika worker: {rubika_label}",
            f"Config: {config_text}",
            f"Rubika session: {settings['rubika_session']}",
            f"Destination: {settings['rubika_target_title']} ({settings['rubika_target']})",
            f"Data dir: {DATA_DIR}",
            f"Queue: {queue_count}",
            f"Active upload: {active}",
            f"Stale upload state: {'yes, run /cleanup confirm' if stale_processing else 'no'}",
            f"Failed transfers: {failed_count}",
            f"Runtime storage: {human_size(runtime_storage)}",
        ]
    )

    with STATE_LOCK:
        logs = "\n".join(LOG_LINES) or "No logs yet."
    return {
        "status": status,
        "logs": logs,
        "updated_at": time.strftime("%H:%M:%S"),
        "metrics": {
            "telegram": telegram_label,
            "rubika": rubika_label,
            "config": config_text,
            "rubika_session": settings["rubika_session"],
            "destination": f"{settings['rubika_target_title']} ({settings['rubika_target']})",
            "data_dir": str(DATA_DIR),
            "queue": queue_count,
            "active_upload": active,
            "failed": failed_count,
            "runtime_storage": human_size(runtime_storage),
            "upload_percent": upload_percent,
            "stale_processing": stale_processing,
            "web_downloads": web_downloads,
        },
    }


def dashboard_text() -> tuple[str, str]:
    snapshot = dashboard_snapshot()
    return snapshot["status"], snapshot["logs"]


def dashboard_payload() -> dict:
    return dashboard_snapshot()


def render_dashboard() -> bytes:
    payload = dashboard_payload()
    page = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>WalrusHF</title>
  <style>
    :root {{
      color-scheme: dark;
      --bg: #080808;
      --bg-2: #11110f;
      --panel: rgba(18, 18, 16, 0.88);
      --panel-strong: rgba(22, 22, 19, 0.96);
      --line: rgba(255, 255, 255, 0.13);
      --line-strong: rgba(255, 255, 255, 0.26);
      --text: #f5f2e8;
      --muted: #9b9a91;
      --accent: #ff7a18;
      --accent-2: #f5f2e8;
      --danger: #ff7a7a;
      --warn: #ffb84d;
      --glow: rgba(255, 122, 24, 0.28);
      --shadow: 0 26px 90px rgba(0, 0, 0, 0.48);
    }}
    * {{
      box-sizing: border-box;
    }}
    body {{
      margin: 0;
      min-height: 100vh;
      font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      font-feature-settings: "cv02", "cv03", "cv04", "cv11";
      background:
        radial-gradient(circle at 12% 7%, rgba(255, 122, 24, 0.14), transparent 27rem),
        radial-gradient(circle at 86% 0%, rgba(245, 242, 232, 0.08), transparent 29rem),
        linear-gradient(135deg, var(--bg), var(--bg-2) 48%, #0b0b09);
      color: var(--text);
    }}
    body::before {{
      content: "";
      position: fixed;
      inset: 0;
      pointer-events: none;
      background-image:
        linear-gradient(rgba(255, 255, 255, 0.045) 1px, transparent 1px),
        linear-gradient(90deg, rgba(255, 255, 255, 0.035) 1px, transparent 1px),
        radial-gradient(circle, rgba(255,255,255,0.08) 1px, transparent 1.4px);
      background-size: 34px 34px;
      background-position: 0 0, 0 0, 0 0;
      mask-image: linear-gradient(to bottom, rgba(0,0,0,0.85), transparent 78%);
    }}
    body::after {{
      content: "";
      position: fixed;
      inset: 0;
      pointer-events: none;
      background:
        repeating-linear-gradient(0deg, rgba(255,255,255,0.035) 0 1px, transparent 1px 4px),
        linear-gradient(180deg, transparent, rgba(255, 122, 24, 0.035), transparent);
      background-size: 100% 220px;
      animation: scan 7s linear infinite;
      opacity: 0.4;
    }}
    @keyframes scan {{
      from {{ background-position: 0 -220px; }}
      to {{ background-position: 0 100vh; }}
    }}
    main {{
      position: relative;
      max-width: 1180px;
      margin: 0 auto;
      padding: 28px 20px 46px;
    }}
    .hero {{
      position: relative;
      display: grid;
      grid-template-columns: auto 1fr auto;
      align-items: center;
      gap: 22px;
      min-height: 168px;
      padding: 18px 28px 30px;
      border: 1px solid var(--line-strong);
      border-radius: 8px;
      background:
        linear-gradient(135deg, rgba(17, 17, 15, 0.98), rgba(9, 9, 8, 0.86)),
        repeating-linear-gradient(120deg, rgba(255,255,255,0.035) 0 1px, transparent 1px 18px);
      box-shadow: var(--shadow);
      overflow: hidden;
    }}
    .hero::before {{
      content: "";
      position: absolute;
      inset: 0;
      border-radius: inherit;
      background:
        linear-gradient(90deg, transparent, rgba(255, 122, 24, 0.11), transparent),
        linear-gradient(180deg, rgba(255,255,255,0.04), transparent 35%);
      transform: translateX(-68%);
      animation: sweep 8s ease-in-out infinite;
      pointer-events: none;
    }}
    .hero::after {{
      content: "";
      position: absolute;
      width: 360px;
      height: 360px;
      right: max(-120px, -9vw);
      top: -145px;
      border: 1px solid rgba(255, 122, 24, 0.16);
      border-radius: 8px;
      box-shadow:
        inset 0 0 0 36px rgba(255, 122, 24, 0.018),
        0 0 80px rgba(255, 122, 24, 0.08);
      pointer-events: none;
      transform: rotate(18deg);
    }}
    @keyframes sweep {{
      0%, 54% {{ transform: translateX(-75%); opacity: 0; }}
      64% {{ opacity: 1; }}
      100% {{ transform: translateX(82%); opacity: 0; }}
    }}
    .chrome {{
      position: relative;
      z-index: 1;
      grid-column: 1 / -1;
      display: flex;
      align-items: center;
      gap: 8px;
      min-height: 24px;
      margin: -4px -10px 10px;
      padding: 0 0 13px;
      border-bottom: 1px solid rgba(255,255,255,0.12);
    }}
    .chrome span {{
      width: 9px;
      height: 9px;
      border-radius: 50%;
      background: var(--accent);
      box-shadow: 17px 0 #f5f2e8, 34px 0 #74736b;
    }}
    .chrome b {{
      margin-left: auto;
      color: #6f6e67;
      font-family: "SF Mono", "Cascadia Code", ui-monospace, Menlo, Consolas, monospace;
      font-size: 10px;
      font-weight: 700;
      letter-spacing: 0.12em;
      text-transform: uppercase;
    }}
    .mark {{
      position: relative;
      z-index: 1;
      display: grid;
      place-items: center;
      width: 78px;
      height: 78px;
      border: 1px solid rgba(255, 122, 24, 0.48);
      border-radius: 8px;
      background:
        radial-gradient(circle at 50% 42%, rgba(255, 122, 24, 0.16), transparent 58%),
        linear-gradient(145deg, rgba(255, 122, 24, 0.13), rgba(255, 255, 255, 0.04));
      font-size: 38px;
      box-shadow:
        0 0 28px rgba(255, 122, 24, 0.18),
        inset 0 0 22px rgba(255, 122, 24, 0.08);
    }}
    .kicker {{
      margin: 0 0 7px;
      color: var(--accent);
      font-size: 11px;
      font-weight: 760;
      letter-spacing: 0.18em;
      text-transform: uppercase;
    }}
    h1 {{
      margin: 0 0 8px;
      font-size: clamp(42px, 6.8vw, 74px);
      line-height: 0.95;
      font-weight: 860;
      letter-spacing: -0.035em;
      text-shadow: 0 0 28px rgba(255, 122, 24, 0.08);
    }}
    p {{
      color: var(--muted);
      margin: 0;
      max-width: 660px;
      line-height: 1.6;
    }}
    .live {{
      position: relative;
      z-index: 1;
      display: flex;
      align-items: center;
      gap: 9px;
      flex: 0 0 auto;
      align-self: start;
      color: var(--text);
      border: 1px solid rgba(255, 122, 24, 0.38);
      border-radius: 8px;
      padding: 10px 12px;
      background: rgba(255, 122, 24, 0.08);
      font-size: 12px;
      font-weight: 800;
      text-transform: uppercase;
    }}
    .live::before {{
      content: "";
      width: 8px;
      height: 8px;
      border-radius: 50%;
      background: var(--accent);
      box-shadow: 0 0 18px var(--accent);
    }}
    .live[data-state="stale"] {{
      color: var(--warn);
      border-color: rgba(246, 198, 106, 0.35);
      background: rgba(246, 198, 106, 0.08);
    }}
    .live[data-state="stale"]::before {{
      background: var(--warn);
      box-shadow: 0 0 18px var(--warn);
    }}
    .status-grid {{
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 12px;
      margin: 16px 0;
    }}
    .url-panel {{
      position: relative;
      margin: 16px 0;
      padding: 16px;
      border: 1px solid var(--line);
      border-radius: 8px;
      background:
        linear-gradient(135deg, rgba(255, 122, 24, 0.08), transparent 44%),
        var(--panel);
      box-shadow: 0 12px 42px rgba(0, 0, 0, 0.24);
      backdrop-filter: blur(12px);
      overflow: hidden;
    }}
    .url-panel::before {{
      content: "";
      position: absolute;
      inset: 0;
      pointer-events: none;
      background:
        linear-gradient(90deg, rgba(255,255,255,0.05), transparent 38%),
        repeating-linear-gradient(90deg, rgba(255,255,255,0.025) 0 1px, transparent 1px 18px);
    }}
    .url-panel h2 {{
      position: relative;
      padding: 0;
      border-bottom: 0;
      background: transparent;
    }}
    .url-form {{
      position: relative;
      display: grid;
      grid-template-columns: minmax(0, 1fr) auto;
      gap: 10px;
      margin-top: 14px;
    }}
    .url-form input {{
      width: 100%;
      min-height: 44px;
      border: 1px solid var(--line-strong);
      border-radius: 8px;
      padding: 0 13px;
      color: var(--text);
      background: rgba(0, 0, 0, 0.28);
      font: inherit;
      outline: none;
    }}
    .url-form input:focus {{
      border-color: rgba(255, 122, 24, 0.7);
      box-shadow: 0 0 0 3px rgba(255, 122, 24, 0.12);
    }}
    .url-form button {{
      min-height: 44px;
      border: 1px solid rgba(255, 122, 24, 0.6);
      border-radius: 8px;
      padding: 0 16px;
      color: #14110e;
      background: var(--accent);
      font: inherit;
      font-weight: 820;
      cursor: pointer;
    }}
    .web-downloads {{
      position: relative;
      display: grid;
      gap: 8px;
      margin-top: 12px;
    }}
    .url-tools {{
      position: relative;
      display: flex;
      justify-content: flex-end;
      margin-top: 10px;
    }}
    .url-tools[hidden] {{
      display: none;
    }}
    .mini-button, .web-download button {{
      border: 1px solid rgba(255, 255, 255, 0.18);
      border-radius: 8px;
      padding: 7px 10px;
      color: var(--text);
      background: rgba(0, 0, 0, 0.24);
      font: inherit;
      font-size: 12px;
      font-weight: 720;
      cursor: pointer;
    }}
    .mini-button:hover, .web-download button:hover {{
      border-color: rgba(255, 122, 24, 0.7);
      color: var(--accent);
    }}
    .web-download {{
      display: grid;
      grid-template-columns: minmax(0, 1fr) auto;
      gap: 12px;
      align-items: center;
      padding: 12px;
      border: 1px solid rgba(255, 255, 255, 0.1);
      border-radius: 8px;
      background: rgba(0, 0, 0, 0.18);
    }}
    .web-download > form {{
      display: flex;
      align-items: center;
      min-height: 32px;
      margin: 0;
    }}
    .web-download[data-status="failed"] {{
      border-color: rgba(255, 122, 122, 0.34);
    }}
    .web-download[data-status="completed"] {{
      border-color: rgba(88, 214, 141, 0.36);
      background: rgba(20, 96, 56, 0.1);
    }}
    .web-download[data-status="cancelled"] {{
      border-color: rgba(255, 207, 112, 0.32);
      background: rgba(141, 95, 24, 0.09);
    }}
    .web-download[data-status="uploading"] {{
      border-color: rgba(255, 122, 24, 0.34);
      box-shadow: inset 0 0 0 1px rgba(255, 122, 24, 0.1);
    }}
    .web-head {{
      display: flex;
      gap: 10px;
      align-items: center;
      justify-content: space-between;
      min-width: 0;
    }}
    .web-download strong {{
      display: block;
      overflow-wrap: anywhere;
      font-size: 13px;
      line-height: 1.35;
    }}
    .web-download span {{
      color: var(--muted);
      font-family: "SF Mono", "Cascadia Code", ui-monospace, Menlo, Consolas, monospace;
      font-size: 11px;
      text-transform: uppercase;
    }}
    .web-download .status-pill {{
      flex: 0 0 auto;
      display: inline-flex;
      align-items: center;
      min-height: 32px;
      border: 1px solid rgba(255, 255, 255, 0.16);
      border-radius: 999px;
      padding: 0 10px;
      color: var(--text);
      background: rgba(255, 255, 255, 0.06);
      font-family: Inter, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      font-size: 10px;
      font-weight: 820;
      letter-spacing: 0;
      line-height: 1.2;
    }}
    .web-download button {{
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-height: 32px;
      padding: 0 12px;
    }}
    .status-pill[data-status="completed"] {{
      border-color: rgba(88, 214, 141, 0.42);
      color: #99f2bd;
      background: rgba(88, 214, 141, 0.12);
    }}
    .status-pill[data-status="cancelled"] {{
      border-color: rgba(255, 207, 112, 0.42);
      color: #ffd890;
      background: rgba(255, 207, 112, 0.12);
    }}
    .status-pill[data-status="failed"] {{
      border-color: rgba(255, 122, 122, 0.42);
      color: #ffabab;
      background: rgba(255, 122, 122, 0.12);
    }}
    .status-pill[data-status="uploading"], .status-pill[data-status="downloading"] {{
      border-color: rgba(255, 122, 24, 0.48);
      color: var(--accent);
      background: rgba(255, 122, 24, 0.12);
    }}
    .web-progress {{
      display: grid;
      gap: 5px;
      margin-top: 9px;
    }}
    .web-progress label {{
      display: grid;
      grid-template-columns: 74px 1fr 38px;
      gap: 8px;
      align-items: center;
      color: var(--muted);
      font-size: 11px;
      text-transform: uppercase;
    }}
    .web-progress i {{
      height: 7px;
      border: 1px solid rgba(255, 122, 24, 0.2);
      border-radius: 999px;
      overflow: hidden;
      background: rgba(0, 0, 0, 0.28);
    }}
    .web-progress b {{
      display: block;
      width: 0%;
      height: 100%;
      background: linear-gradient(90deg, var(--accent), var(--accent-2));
      transition: width 300ms ease;
    }}
    .tile, section {{
      border: 1px solid var(--line);
      background: var(--panel);
      border-radius: 8px;
      box-shadow: 0 12px 42px rgba(0, 0, 0, 0.24);
      backdrop-filter: blur(12px);
    }}
    .tile {{
      position: relative;
      overflow: hidden;
    }}
    .tile::before {{
      content: "";
      position: absolute;
      inset: 0;
      pointer-events: none;
      background: linear-gradient(135deg, rgba(255, 122, 24, 0.08), transparent 34%);
      opacity: 0.75;
    }}
    .tile {{
      min-height: 116px;
      padding: 16px;
      display: flex;
      flex-direction: column;
      justify-content: space-between;
    }}
    .tile span, .row span {{
      color: var(--muted);
      font-size: 11px;
      font-weight: 760;
      letter-spacing: 0.13em;
      line-height: 1.2;
      text-transform: uppercase;
    }}
    .tile strong {{
      display: block;
      margin-top: 14px;
      color: var(--text);
      font-size: 18px;
      font-weight: 720;
      line-height: 1.2;
      overflow-wrap: anywhere;
    }}
    .tile[data-good="true"] strong {{
      color: var(--accent);
    }}
    .tile[data-warn="true"] strong {{
      color: var(--warn);
    }}
    .deck-grid {{
      display: grid;
      grid-template-columns: minmax(0, 0.92fr) minmax(0, 1.08fr);
      gap: 16px;
      align-items: stretch;
    }}
    section {{
      overflow: hidden;
    }}
    h2 {{
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      font-size: 13px;
      font-weight: 780;
      color: var(--accent);
      margin: 0;
      padding: 14px 16px;
      border-bottom: 1px solid var(--line);
      text-transform: uppercase;
      letter-spacing: 0.14em;
      background:
        linear-gradient(90deg, rgba(255, 122, 24, 0.08), transparent 72%),
        rgba(255, 255, 255, 0.018);
    }}
    .panel-body {{
      padding: 10px 16px 16px;
    }}
    .hero-copy {{
      position: relative;
      z-index: 1;
    }}
    .row {{
      display: grid;
      grid-template-columns: 138px minmax(0, 1fr);
      gap: 14px;
      align-items: center;
      min-height: 53px;
      padding: 12px 0;
      border-bottom: 1px solid rgba(180, 221, 209, 0.1);
    }}
    .row:last-child {{
      border-bottom: 0;
    }}
    .row strong {{
      overflow-wrap: anywhere;
      font-size: 14px;
      line-height: 1.25;
    }}
    .progress-shell {{
      height: 12px;
      margin-top: 12px;
      border-radius: 999px;
      overflow: hidden;
      border: 1px solid rgba(255, 122, 24, 0.28);
      background: rgba(0, 0, 0, 0.28);
    }}
    .progress-fill {{
      width: 0%;
      height: 100%;
      background: linear-gradient(90deg, var(--accent), var(--accent-2));
      box-shadow: 0 0 22px rgba(255, 122, 24, 0.42);
      transition: width 350ms ease;
    }}
    pre {{
      margin: 0;
      padding: 16px;
      max-height: 460px;
      overflow: auto;
      white-space: pre-wrap;
      color: #d6e8ec;
      background:
        linear-gradient(180deg, rgba(0, 0, 0, 0.24), rgba(0, 0, 0, 0.08));
      font: 13px/1.6 "SF Mono", "Cascadia Code", ui-monospace, Menlo, Consolas, monospace;
    }}
    .raw-status {{
      margin-top: 16px;
    }}
    .footer {{
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 14px;
      margin-top: 18px;
      padding: 16px 2px 0;
      color: var(--muted);
      font-size: 13px;
    }}
    .footer strong {{
      color: var(--text);
      font-weight: 750;
    }}
    .footer a {{
      color: var(--accent);
      text-decoration: none;
      border-bottom: 1px solid rgba(255, 122, 24, 0.35);
    }}
    .footer a:hover {{
      border-bottom-color: var(--accent);
    }}
    a {{ color: var(--accent); }}
    @media (max-width: 860px) {{
      .hero {{
        grid-template-columns: 1fr;
        min-height: auto;
      }}
      .mark {{
        width: 58px;
        height: 58px;
        font-size: 30px;
      }}
      .chrome {{
        margin-bottom: 4px;
      }}
      .live {{
        justify-self: start;
      }}
      .status-grid, .deck-grid {{
        grid-template-columns: 1fr;
      }}
      .url-form, .web-download {{
        grid-template-columns: 1fr;
      }}
      .row {{
        grid-template-columns: 1fr;
        align-items: start;
        min-height: 0;
        gap: 4px;
      }}
      .footer {{
        align-items: flex-start;
        flex-direction: column;
      }}
    }}
  </style>
</head>
<body>
  <main>
    <header class="hero">
      <div class="chrome" aria-hidden="true"><span></span><b>walrushf.space</b></div>
      <div class="mark" aria-hidden="true">⛵</div>
      <div class="hero-copy">
        <p class="kicker">Hugging Face Control Deck</p>
        <h1>WalrusHF</h1>
        <p>This Space keeps the Telegram bot and Rubika upload worker running. Use Telegram as the control panel while this deck watches the machinery.</p>
      </div>
      <span id="live" class="live">Live</span>
    </header>

    <section class="url-panel">
      <h2>Direct URL Upload</h2>
      <form class="url-form" method="post" action="/submit-url">
        <input name="url" type="url" inputmode="url" placeholder="https://example.com/file.mp4" required>
        <button type="submit">Queue URL</button>
      </form>
      <div id="web-downloads" class="web-downloads" aria-live="polite"></div>
      <form id="clear-web-form" class="url-tools" method="post" action="/clear-web-tasks" hidden>
        <button class="mini-button" type="submit">Clear Done</button>
      </form>
    </section>

    <div class="status-grid" aria-label="Service status">
      <article class="tile" id="telegram-card">
        <span>Telegram</span>
        <strong id="telegram-value">{html.escape(payload["metrics"]["telegram"])}</strong>
      </article>
      <article class="tile" id="rubika-card">
        <span>Rubika Worker</span>
        <strong id="rubika-value">{html.escape(payload["metrics"]["rubika"])}</strong>
      </article>
      <article class="tile" id="queue-card">
        <span>Queue</span>
        <strong id="queue-value">{html.escape(str(payload["metrics"]["queue"]))}</strong>
      </article>
      <article class="tile" id="failed-card">
        <span>Failed</span>
        <strong id="failed-value">{html.escape(str(payload["metrics"]["failed"]))}</strong>
      </article>
    </div>

    <div class="deck-grid">
      <section>
        <h2>Ship Systems</h2>
        <div class="panel-body">
          <div class="row"><span>Config</span><strong id="config-value">{html.escape(payload["metrics"]["config"])}</strong></div>
          <div class="row"><span>Session</span><strong id="session-value">{html.escape(payload["metrics"]["rubika_session"])}</strong></div>
          <div class="row"><span>Destination</span><strong id="destination-value">{html.escape(payload["metrics"]["destination"])}</strong></div>
          <div class="row"><span>Storage</span><strong id="storage-value">{html.escape(payload["metrics"]["runtime_storage"])}</strong></div>
          <div class="row"><span>Stale State</span><strong id="stale-value">{"yes - run /cleanup confirm" if payload["metrics"]["stale_processing"] else "none"}</strong></div>
          <div class="row"><span>Data Dir</span><strong id="data-dir-value">{html.escape(payload["metrics"]["data_dir"])}</strong></div>
        </div>
      </section>

      <section>
        <h2>Active Upload</h2>
        <div class="panel-body">
          <div class="row"><span>Task</span><strong id="active-value">{html.escape(payload["metrics"]["active_upload"])}</strong></div>
          <div class="row"><span>Progress</span><strong id="upload-percent-value">{html.escape(str(payload["metrics"]["upload_percent"]))}%</strong></div>
          <div class="progress-shell" aria-hidden="true"><div id="upload-bar" class="progress-fill"></div></div>
        </div>
      </section>
    </div>

    <section class="raw-status">
      <h2>Raw Status</h2>
      <pre id="status">{html.escape(payload["status"])}</pre>
    </section>

    <section>
      <h2>Logs</h2>
      <pre id="logs">{html.escape(payload["logs"])}</pre>
    </section>
    <noscript>
      <p>JavaScript is disabled. Refresh the page to update status.</p>
    </noscript>
    <footer class="footer">
      <strong>WalrusHF</strong>
      <a href="https://github.com/rezaaa/WalrusHF" target="_blank" rel="noreferrer">github.com/rezaaa/WalrusHF</a>
    </footer>
  </main>
  <script>
    const statusEl = document.getElementById("status");
    const logsEl = document.getElementById("logs");
    const liveEl = document.getElementById("live");
    const webDownloadsEl = document.getElementById("web-downloads");
    const clearWebForm = document.getElementById("clear-web-form");
    const doneWebStatuses = new Set(["completed", "failed", "cancelled"]);
    const fields = {{
      telegram: document.getElementById("telegram-value"),
      rubika: document.getElementById("rubika-value"),
      queue: document.getElementById("queue-value"),
      failed: document.getElementById("failed-value"),
      config: document.getElementById("config-value"),
      session: document.getElementById("session-value"),
      destination: document.getElementById("destination-value"),
      storage: document.getElementById("storage-value"),
      stale: document.getElementById("stale-value"),
      dataDir: document.getElementById("data-dir-value"),
      active: document.getElementById("active-value"),
      uploadPercent: document.getElementById("upload-percent-value"),
    }};
    const cards = {{
      telegram: document.getElementById("telegram-card"),
      rubika: document.getElementById("rubika-card"),
      queue: document.getElementById("queue-card"),
      failed: document.getElementById("failed-card"),
    }};
    const uploadBar = document.getElementById("upload-bar");

    function setText(element, value) {{
      if (element) element.textContent = value ?? "";
    }}

    function updateCardState(card, value, goodTest) {{
      if (!card) return;
      const text = String(value ?? "");
      card.dataset.good = goodTest(text) ? "true" : "false";
      card.dataset.warn = !goodTest(text) && text !== "0" ? "true" : "false";
    }}

    function renderWebDownloads(downloads) {{
      if (!webDownloadsEl) return;
      const hasClearable = (downloads || []).some(item => doneWebStatuses.has(item.status || ""));
      if (clearWebForm) clearWebForm.hidden = !hasClearable;
      if (!downloads || downloads.length === 0) {{
        const empty = document.createElement("div");
        empty.className = "web-download";
        empty.dataset.status = "empty";
        const title = document.createElement("strong");
        title.textContent = "No dashboard URL transfers.";
        const meta = document.createElement("span");
        meta.textContent = "Paste a direct file URL above.";
        empty.append(title, meta);
        webDownloadsEl.replaceChildren(empty);
        return;
      }}
      webDownloadsEl.replaceChildren(...downloads.slice(0, 5).map(item => {{
        const row = document.createElement("div");
        row.className = "web-download";
        row.dataset.status = item.status || "pending";
        const body = document.createElement("div");
        const head = document.createElement("div");
        head.className = "web-head";
        const title = document.createElement("strong");
        title.textContent = item.file_name || item.url || item.task_id || "file";
        const badge = document.createElement("span");
        badge.className = "status-pill";
        badge.dataset.status = item.status || "pending";
        badge.textContent = (item.status || "pending").toUpperCase();
        head.append(title, badge);
        const meta = document.createElement("span");
        const downloadPercent = Math.max(0, Math.min(100, item.download_percent || 0));
        const uploadPercent = Math.max(0, Math.min(100, item.upload_percent || 0));
        const note = item.note ? ` · ${{item.note}}` : "";
        meta.textContent = `${{item.size || "unknown"}} · ${{item.task_id || "-"}}${{note}}`;
        const progress = document.createElement("div");
        progress.className = "web-progress";
        for (const [label, value] of [["Download", downloadPercent], ["Upload", uploadPercent]]) {{
          const line = document.createElement("label");
          const name = document.createElement("span");
          const bar = document.createElement("i");
          const fill = document.createElement("b");
          const valueEl = document.createElement("span");
          name.textContent = label;
          fill.style.width = `${{value}}%`;
          valueEl.textContent = `${{value}}%`;
          bar.append(fill);
          line.append(name, bar, valueEl);
          progress.append(line);
        }}
        body.append(head, meta, progress);
        row.append(body);
        if (!doneWebStatuses.has(item.status || "")) {{
          const form = document.createElement("form");
          form.method = "post";
          form.action = "/cancel-web-task";
          const input = document.createElement("input");
          input.type = "hidden";
          input.name = "task_id";
          input.value = item.task_id || "";
          const button = document.createElement("button");
          button.type = "submit";
          button.textContent = "Cancel";
          form.append(input, button);
          row.append(form);
        }}
        return row;
      }}));
    }}

    async function refreshDashboard() {{
      try {{
        const response = await fetch("/status.json", {{ cache: "no-store" }});
        if (!response.ok) throw new Error(`HTTP ${{response.status}}`);
        const data = await response.json();
        const metrics = data.metrics || {{}};
        statusEl.textContent = data.status || "";
        logsEl.textContent = data.logs || "";
        setText(fields.telegram, metrics.telegram);
        setText(fields.rubika, metrics.rubika);
        setText(fields.queue, metrics.queue);
        setText(fields.failed, metrics.failed);
        setText(fields.config, metrics.config);
        setText(fields.session, metrics.rubika_session);
        setText(fields.destination, metrics.destination);
        setText(fields.storage, metrics.runtime_storage);
        setText(fields.stale, metrics.stale_processing ? "yes - run /cleanup confirm" : "none");
        setText(fields.dataDir, metrics.data_dir);
        setText(fields.active, metrics.active_upload);
        setText(fields.uploadPercent, `${{metrics.upload_percent || 0}}%`);
        uploadBar.style.width = `${{Math.max(0, Math.min(100, metrics.upload_percent || 0))}}%`;
        renderWebDownloads(metrics.web_downloads || []);
        updateCardState(cards.telegram, metrics.telegram, value => value.includes("running"));
        updateCardState(cards.rubika, metrics.rubika, value => value.includes("running"));
        updateCardState(cards.queue, metrics.queue, value => Number(value) === 0);
        updateCardState(cards.failed, metrics.failed, value => Number(value) === 0);
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

    def redirect_home(self) -> None:
        self.send_response(303)
        self.send_header("Location", "/")
        self.send_header("Cache-Control", "no-store")
        self.end_headers()

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

    def do_POST(self) -> None:
        path = urlsplit(self.path).path

        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            length = 0

        if length < 0 or length > 8192:
            self.redirect_home()
            return

        body = self.rfile.read(length).decode("utf-8", errors="replace") if length else ""
        values = parse_qs(body)

        if path == "/submit-url":
            url = (values.get("url") or [""])[0].strip()
            if url:
                task_id = start_web_url_download(url)
                append_log("web", f"accepted direct URL task id={task_id}")
            self.redirect_home()
            return

        if path == "/cancel-web-task":
            task_id = (values.get("task_id") or [""])[0].strip()
            if task_id:
                cancel_web_task(task_id)
            self.redirect_home()
            return

        if path == "/clear-web-tasks":
            clear_web_tasks()
            self.redirect_home()
            return

        self.send_response(404)
        self.end_headers()

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
