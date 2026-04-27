from __future__ import annotations

import asyncio
import atexit
from html import escape
import os
import time
from pathlib import Path

import requests
from dotenv import load_dotenv
from rubpy import Client as RubikaClient

from task_store import (
    append_failed,
    build_status_text,
    clear_cancelled,
    clear_processing,
    clear_worker_pid,
    cleanup_local_file,
    ensure_storage_dirs,
    has_rubika_session,
    human_duration,
    human_speed,
    is_cancelled,
    load_runtime_settings,
    load_processing,
    normalize_runtime_settings,
    normalize_upload_filename,
    pop_first_task,
    save_worker_pid,
    save_processing,
    safe_filename,
)


load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()

MAX_RETRIES = 5
RETRY_DELAY = 3
ERROR_TEXT_LIMIT = 220
RUBIKA_CONNECT_TIMEOUT = int(os.getenv("RUBIKA_CONNECT_TIMEOUT", "25") or 25)
RUBIKA_FINALIZE_RETRIES = int(os.getenv("RUBIKA_FINALIZE_RETRIES", "3") or 3)
RUBIKA_FINALIZE_RETRY_DELAY = float(os.getenv("RUBIKA_FINALIZE_RETRY_DELAY", "2") or 2)
RUBIKA_UPLOAD_CHUNK_SIZE = int(os.getenv("RUBIKA_UPLOAD_CHUNK_SIZE", "1048576") or 1048576)
RUBIKA_CHUNK_RETRIES = int(os.getenv("RUBIKA_CHUNK_RETRIES", "30") or 30)
RUBIKA_CHUNK_RETRY_DELAY = float(os.getenv("RUBIKA_CHUNK_RETRY_DELAY", "2") or 2)
RUBIKA_CHUNK_RETRY_DELAY_MAX = float(os.getenv("RUBIKA_CHUNK_RETRY_DELAY_MAX", "45") or 45)
RUBIKA_REINIT_GRACE_RETRIES = int(os.getenv("RUBIKA_REINIT_GRACE_RETRIES", "5") or 5)
RUBIKA_UPLOAD_STATE_KEY = "rubika_upload_session"
RUBIKA_UPLOADED_FILE_KEY = "rubika_uploaded_file"

ensure_storage_dirs()


UPLOAD_EXTENSIONS = {
    ".mp4", ".mkv", ".avi", ".mov", ".webm", ".flv", ".m4v",
    ".jpg", ".jpeg", ".png", ".webp", ".gif", ".bmp",
    ".mp3", ".wav", ".ogg", ".m4a", ".flac", ".aac",
    ".pdf", ".txt", ".csv", ".json",
    ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx",
    ".zip", ".rar", ".7z", ".tar", ".gz", ".bz2", ".xz",
}
VIDEO_EXTENSIONS = {".mp4", ".mkv", ".avi", ".mov", ".webm", ".flv", ".m4v"}


class CancelledTaskError(RuntimeError):
    pass


class RubikaConnectTimeoutError(TimeoutError):
    pass


class MissingRubikaSessionError(RuntimeError):
    pass


class RubikaUploadResetRequired(RuntimeError):
    pass


def ensure_session(session_name: str) -> None:
    if has_rubika_session(session_name):
        return

    raise MissingRubikaSessionError(
        "Rubika account is not set up. Open the Telegram bot and run /start or /set_rubika."
    )


def resolve_task_settings(task: dict) -> dict:
    current_settings = load_runtime_settings()
    return normalize_runtime_settings(
        {
            "rubika_session": task.get("rubika_session") or current_settings["rubika_session"],
            "rubika_target": task.get("rubika_target") or current_settings["rubika_target"],
            "rubika_target_title": (
                task.get("rubika_target_title") or current_settings["rubika_target_title"]
            ),
            "rubika_target_type": (
                task.get("rubika_target_type") or current_settings["rubika_target_type"]
            ),
        }
    )


def format_destination_label(settings: dict) -> str:
    return str(settings.get("rubika_target_title") or "Saved Messages")


def should_keep_extension(filename: str) -> bool:
    return Path(filename).suffix.lower() in UPLOAD_EXTENSIONS


def update_telegram_status(
    task: dict,
    stage: str,
    upload_status: str,
    note: str | None = None,
    attempt_text: str | None = None,
    action: str | None = "cancel",
) -> None:
    if not BOT_TOKEN:
        return

    chat_id = task.get("chat_id")
    status_message_id = task.get("status_message_id")
    if not chat_id or not status_message_id:
        return

    payload = {
        "chat_id": chat_id,
        "message_id": status_message_id,
        "text": build_status_text(
            task_id=task.get("task_id", "-"),
            file_name=task.get("file_name", Path(task.get("path", "")).name or "file"),
            file_size=int(task.get("file_size", 0) or 0),
            stage=stage,
            download_percent=100,
            upload_percent=int(task.get("upload_percent", 0) or 0),
            upload_status=upload_status,
            note=note,
            attempt_text=attempt_text or task.get("attempt_text"),
            speed_text=task.get("speed_text"),
            eta_text=task.get("eta_text"),
        ),
        "parse_mode": "HTML",
    }

    task_id = task.get("task_id", "")
    if action and task_id:
        label = "🔁 Retry" if action == "retry" else "🛑 Cancel"
        payload["reply_markup"] = {
            "inline_keyboard": [
                [{"text": label, "callback_data": f"{action}:{task_id}"}]
            ]
        }
    else:
        payload["reply_markup"] = {"inline_keyboard": []}

    try:
        requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/editMessageText",
            json=payload,
            timeout=15,
        )
    except Exception:
        pass


def send_telegram_message(
    chat_id: int,
    text: str,
    reply_to_message_id: int | None = None,
) -> None:
    if not BOT_TOKEN or not chat_id:
        return

    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
    }

    if reply_to_message_id:
        payload["reply_to_message_id"] = reply_to_message_id

    try:
        requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            json=payload,
            timeout=15,
        )
    except Exception:
        pass


def format_duration(seconds: float | int | None) -> str:
    total_seconds = max(0, int(seconds or 0))
    hours, remainder = divmod(total_seconds, 3600)
    minutes, secs = divmod(remainder, 60)

    if hours:
        return f"{hours}h {minutes}m {secs}s"
    if minutes:
        return f"{minutes}m {secs}s"
    return f"{secs}s"


def task_elapsed_text(task: dict) -> str | None:
    started_at = task.get("started_at")
    if started_at is None:
        return None

    try:
        started_at_value = float(started_at)
    except (TypeError, ValueError):
        return None

    return format_duration(time.time() - started_at_value)


def notify_transfer_complete(task: dict, elapsed_text: str | None, settings: dict) -> None:
    chat_id = task.get("chat_id")
    if not chat_id:
        return

    file_name = task.get("file_name", Path(task.get("path", "")).name or "file")
    lines = [
        "<b>✅ Transfer Complete</b>",
        f"📄 <b>File:</b> <code>{escape(file_name)}</code>",
        f"📬 <b>Destination:</b> <code>{escape(format_destination_label(settings))}</code>",
    ]

    if elapsed_text:
        lines.append(f"⏱ <b>Time:</b> <code>{escape(elapsed_text)}</code>")

    send_telegram_message(
        int(chat_id),
        "\n".join(lines),
        reply_to_message_id=task.get("status_message_id"),
    )


def update_field(update, key: str, default=None):
    if isinstance(update, dict):
        return update.get(key, default)
    return getattr(update, key, default)


async def sleep_with_cancel(task_id: str, seconds: float) -> None:
    remaining = max(0.0, seconds)
    while remaining > 0:
        if is_cancelled(task_id):
            raise CancelledTaskError("Cancelled by user.")
        interval = min(1.0, remaining)
        await asyncio.sleep(interval)
        remaining -= interval


def valid_upload_state(state: dict, file_path: str, file_name: str, file_size: int) -> bool:
    required = {"file_id", "dc_id", "upload_url", "access_hash_send", "next_part"}
    return (
        isinstance(state, dict)
        and required.issubset(state)
        and state.get("file_path") == file_path
        and state.get("file_name") == file_name
        and int(state.get("file_size", 0) or 0) == file_size
        and int(state.get("chunk_size", 0) or 0) == RUBIKA_UPLOAD_CHUNK_SIZE
    )


def valid_uploaded_file(uploaded: dict, file_path: str, file_name: str) -> bool:
    try:
        file_size = Path(file_path).stat().st_size
    except OSError:
        return False
    required = {"dc_id", "file_id", "access_hash_rec", "file_name", "size", "mime"}
    return (
        isinstance(uploaded, dict)
        and required.issubset(uploaded)
        and uploaded.get("file_name") == file_name
        and int(uploaded.get("size", 0) or 0) == file_size
    )


async def request_upload_session(client, task: dict, file_path: str, file_name: str, file_size: int, mime: str) -> dict:
    result = await client.request_send_file(file_name, file_size, mime)
    state = {
        "file_path": file_path,
        "file_name": file_name,
        "file_size": file_size,
        "mime": mime,
        "chunk_size": RUBIKA_UPLOAD_CHUNK_SIZE,
        "total_parts": max(1, (file_size + RUBIKA_UPLOAD_CHUNK_SIZE - 1) // RUBIKA_UPLOAD_CHUNK_SIZE),
        "next_part": 1,
        "file_id": str(update_field(result, "id") or update_field(result, "file_id") or ""),
        "dc_id": update_field(result, "dc_id"),
        "upload_url": update_field(result, "upload_url"),
        "access_hash_send": update_field(result, "access_hash_send"),
    }
    if not state["file_id"] or not state["upload_url"] or not state["access_hash_send"]:
        raise RuntimeError("Rubika did not return upload session metadata.")
    task[RUBIKA_UPLOAD_STATE_KEY] = state
    save_processing(task)
    return state


async def post_upload_chunk(client, state: dict, data: bytes, part_number: int) -> dict:
    async with client.connection.session.post(
        url=state["upload_url"],
        headers={
            "auth": client.auth,
            "file-id": state["file_id"],
            "total-part": str(state["total_parts"]),
            "part-number": str(part_number),
            "chunk-size": str(len(data)),
            "access-hash-send": state["access_hash_send"],
        },
        data=data,
        proxy=getattr(client, "proxy", None),
    ) as response:
        if response.status >= 500:
            body = " ".join((await response.text()).split())
            raise RuntimeError(f"{response.status} server error while uploading chunk: {body[:120]}")
        try:
            payload = await response.json(content_type=None)
        except TypeError:
            payload = await response.json()
        if response.status >= 400:
            raise RuntimeError(f"{response.status} error while uploading chunk: {payload}")
        return payload


async def upload_chunk_with_retry(client, task: dict, state: dict, data: bytes, part_number: int) -> dict:
    task_id = task.get("task_id", "")
    last_error = None
    for attempt in range(1, RUBIKA_CHUNK_RETRIES + 1):
        if is_cancelled(task_id):
            raise CancelledTaskError("Cancelled by user.")
        try:
            return await post_upload_chunk(client, state, data, part_number)
        except Exception as error:
            if isinstance(error, CancelledTaskError):
                raise
            last_error = error
            delay = min(RUBIKA_CHUNK_RETRY_DELAY_MAX, RUBIKA_CHUNK_RETRY_DELAY * attempt)
            task["speed_text"] = None
            task["eta_text"] = None
            save_processing(task)
            update_telegram_status(
                task,
                stage="⚠️ Upload Paused",
                upload_status=(
                    f"Rubika chunk {part_number}/{state['total_parts']} failed "
                    f"(retry {attempt}/{RUBIKA_CHUNK_RETRIES}). Waiting {int(delay)}s."
                ),
                note=compact_error_text(error),
            )
            if attempt >= RUBIKA_CHUNK_RETRIES:
                break
            await sleep_with_cancel(task_id, delay)
    raise last_error if last_error else RuntimeError("Chunk upload failed.")


async def emit_upload_progress(callback, total: int, current: int) -> None:
    if not callback:
        return
    result = callback(total, current)
    if asyncio.iscoroutine(result):
        await result


async def resilient_upload_file(client, task: dict, file_path: str, file_name: str, callback=None) -> dict:
    path = Path(file_path)
    file_size = path.stat().st_size
    mime = path.suffix.lower().lstrip(".") or "bin"
    state = task.get(RUBIKA_UPLOAD_STATE_KEY, {})
    if not valid_upload_state(state, file_path, file_name, file_size):
        state = await request_upload_session(client, task, file_path, file_name, file_size, mime)

    task_id = task.get("task_id", "")
    last_result: dict = {}
    reinit_retries = 0

    with path.open("rb") as source:
        while int(state["next_part"]) <= int(state["total_parts"]):
            if is_cancelled(task_id):
                raise CancelledTaskError("Cancelled by user.")

            part_number = int(state["next_part"])
            source.seek((part_number - 1) * RUBIKA_UPLOAD_CHUNK_SIZE)
            data = source.read(RUBIKA_UPLOAD_CHUNK_SIZE)
            if not data:
                break

            last_result = await upload_chunk_with_retry(client, task, state, data, part_number)
            status = str(last_result.get("status") or "").upper()
            if status == "ERROR_TRY_AGAIN":
                reinit_retries += 1
                delay = min(RUBIKA_CHUNK_RETRY_DELAY_MAX, RUBIKA_CHUNK_RETRY_DELAY * reinit_retries)
                if reinit_retries <= RUBIKA_REINIT_GRACE_RETRIES:
                    update_telegram_status(
                        task,
                        stage="⚠️ Upload Paused",
                        upload_status=(
                            f"Rubika asked to reinitialize at chunk {part_number}/{state['total_parts']}. "
                            f"Retrying the same chunk before restarting."
                        ),
                    )
                    await sleep_with_cancel(task_id, delay)
                    continue

                update_telegram_status(
                    task,
                    stage="⚠️ Restarting Upload",
                    upload_status="Rubika forced a new upload session, so the upload must restart from part 1.",
                )
                task.pop(RUBIKA_UPLOAD_STATE_KEY, None)
                save_processing(task)
                raise RubikaUploadResetRequired(
                    "Rubika forced a new upload session; restarting upload from part 1."
                )
            if status and status != "OK":
                raise RuntimeError(f"Rubika rejected chunk {part_number}: {last_result}")

            reinit_retries = 0
            completed = min(part_number * RUBIKA_UPLOAD_CHUNK_SIZE, file_size)
            state["next_part"] = part_number + 1
            state["completed_bytes"] = completed
            task[RUBIKA_UPLOAD_STATE_KEY] = state
            save_processing(task)
            await emit_upload_progress(callback, file_size, completed)

    if str(last_result.get("status") or "").upper() == "OK" and str(last_result.get("status_det") or "").upper() == "OK":
        uploaded = {
            "mime": mime,
            "size": file_size,
            "dc_id": state["dc_id"],
            "file_id": state["file_id"],
            "file_name": file_name,
            "access_hash_rec": last_result.get("data", {}).get("access_hash_rec"),
        }
        if not uploaded["access_hash_rec"]:
            raise RuntimeError("Rubika upload finished without access hash.")
        task[RUBIKA_UPLOADED_FILE_KEY] = uploaded
        task.pop(RUBIKA_UPLOAD_STATE_KEY, None)
        save_processing(task)
        return uploaded

    raise RuntimeError(f"Rubika upload failed: {last_result}")


async def send_document(
    session_name: str,
    target: str,
    file_path: str,
    caption: str = "",
    callback=None,
    file_name: str | None = None,
    task: dict | None = None,
):
    client = RubikaClient(name=session_name)
    entered = False
    task = task or {}
    upload_name = file_name or Path(file_path).name
    try:
        await asyncio.wait_for(client.__aenter__(), timeout=RUBIKA_CONNECT_TIMEOUT)
        entered = True
    except asyncio.TimeoutError as exc:
        raise RubikaConnectTimeoutError(
            f"Rubika connection timed out after {RUBIKA_CONNECT_TIMEOUT}s."
        ) from exc

    try:
        uploaded = task.get(RUBIKA_UPLOADED_FILE_KEY)
        if not valid_uploaded_file(uploaded, file_path, upload_name):
            uploaded = await resilient_upload_file(client, task, file_path, upload_name, callback)

        file_inline = dict(uploaded) if isinstance(uploaded, dict) else uploaded.to_dict
        inline_type = rubika_inline_type(task, file_path, upload_name)
        finalize_variants = build_file_inline_variants(file_inline, inline_type)

        last_error = None
        for strategy, candidate_file_inline in finalize_variants:
            for attempt in range(1, RUBIKA_FINALIZE_RETRIES + 1):
                try:
                    result = await client.send_message(
                        object_guid=target,
                        text=caption.strip() if caption and caption.strip() else None,
                        file_inline=candidate_file_inline,
                    )
                    task.pop(RUBIKA_UPLOADED_FILE_KEY, None)
                    task.pop(RUBIKA_UPLOAD_STATE_KEY, None)
                    save_processing(task)
                    return result
                except Exception as error:
                    last_error = error
                    error_text = compact_error_text(error)
                    transient = is_transient_upload_error(error_text.lower())
                    try_next_strategy = (
                        not transient
                        and attempt == 1
                        and strategy != finalize_variants[-1][0]
                    )
                    if try_next_strategy:
                        break
                    if attempt >= RUBIKA_FINALIZE_RETRIES:
                        break
                    if not transient:
                        break
                    await asyncio.sleep(RUBIKA_FINALIZE_RETRY_DELAY * attempt)

                if last_error and not transient:
                    break

        raise last_error if last_error else RuntimeError("Rubika finalization failed.")
    finally:
        if entered:
            await client.__aexit__(None, None, None)


def is_transient_upload_error(error_text: str) -> bool:
    return any(
        key in error_text
        for key in [
            "500",
            "502",
            "503",
            "504",
            "bad gateway",
            "gateway",
            "service unavailable",
            "timeout",
            "timed out",
            "read timed out",
            "connect timeout",
            "connection timed out",
            "cannot connect",
            "connection reset",
            "connection aborted",
            "remote end closed connection",
            "server disconnected",
            "broken pipe",
            "ssl",
            "protocolerror",
            "temporarily unavailable",
            "temporary failure",
            "network is unreachable",
            "error uploading chunk",
            "chunk upload failed",
            "error_try_again",
            "error message try",
            "error_message_try",
            "forced a new upload session",
            "restarting upload",
            "too_requests",
            "too requests",
            "internal_problem",
            "no_connection",
        ]
    )


def wait_with_cancel(task_id: str, seconds: int) -> None:
    for _ in range(seconds):
        if is_cancelled(task_id):
            raise CancelledTaskError("Cancelled by user.")
        time.sleep(1)


def normalize_failed_progress(task: dict) -> None:
    current_percent = int(task.get("upload_percent", 0) or 0)
    task["upload_percent"] = min(current_percent, 99)


def compact_error_text(error: Exception | str) -> str:
    if isinstance(error, Exception):
        name = type(error).__name__
        raw = " ".join(str(error).split()).strip()
        if raw:
            text = f"{name}: {raw}"
        else:
            fallback = " ".join(repr(error).split()).strip()
            text = fallback if fallback and fallback != f"{name}()" else name
    else:
        text = " ".join(str(error or "").split()).strip()

    if not text:
        return "Unknown upload error."

    if len(text) <= ERROR_TEXT_LIMIT:
        return text
    return text[: ERROR_TEXT_LIMIT - 3].rstrip() + "..."


def build_fallback_upload_name(task: dict, file_path: str, current_name: str | None = None) -> str:
    original_suffix = Path(current_name or file_path).suffix.lower()
    suffix = original_suffix if original_suffix in UPLOAD_EXTENSIONS else ".bin"
    task_id = (task.get("task_id") or "file").strip()[:16] or "file"
    return safe_filename(f"{task_id}{suffix}", f"{task_id}.bin")


def rubika_inline_type(task: dict, file_path: str, file_name: str | None = None) -> str:
    suffix = Path(file_name or file_path).suffix.lower()
    media_type = str(task.get("media_type") or "").lower()
    if media_type == "video" or suffix in VIDEO_EXTENSIONS:
        return "Video"
    if media_type == "photo" or suffix in {".jpg", ".jpeg", ".png", ".webp", ".gif", ".bmp"}:
        return "Image"
    if media_type in {"audio", "voice"} or suffix in {".mp3", ".wav", ".ogg", ".m4a", ".flac", ".aac"}:
        return "Music"
    return "File"


def build_file_inline_payload(uploaded_file: dict, inline_type: str) -> dict:
    payload = dict(uploaded_file)
    payload.update(
        {
            "type": inline_type,
            "time": 1,
            "width": 200,
            "height": 200,
            "music_performer": "",
            "is_spoil": False,
        }
    )
    return payload


def build_file_inline_variants(uploaded_file: dict, preferred_type: str) -> list[tuple[str, dict]]:
    variants = [(preferred_type.lower(), build_file_inline_payload(uploaded_file, preferred_type))]
    if preferred_type != "File":
        variants.append(("file", build_file_inline_payload(uploaded_file, "File")))
    return variants


def make_upload_progress_callback(task: dict, attempt: int):
    state = {
        "last_percent": -1,
        "last_update": 0.0,
        "last_bytes": 0,
        "last_sample_at": time.monotonic(),
        "speed_bps": 0.0,
    }
    task_id = task.get("task_id", "")

    async def callback(total: int, current: int) -> None:
        if is_cancelled(task_id):
            raise CancelledTaskError("Cancelled by user.")

        if total <= 0:
            return

        raw_percent = min(100, max(0, int((current * 100) / total)))
        percent = min(raw_percent, 99)
        if state["last_percent"] >= 0 and percent < state["last_percent"]:
            return

        now = time.monotonic()
        delta_bytes = max(0, current - state["last_bytes"])
        delta_time = max(0.0, now - state["last_sample_at"])
        if delta_bytes > 0 and delta_time > 0:
            instant_speed = delta_bytes / delta_time
            state["speed_bps"] = (
                instant_speed
                if state["speed_bps"] <= 0
                else (state["speed_bps"] * 0.65) + (instant_speed * 0.35)
            )
            state["last_bytes"] = current
            state["last_sample_at"] = now

        should_emit = (
            raw_percent == 100
            or state["last_percent"] < 0
            or percent - state["last_percent"] >= 5
            or now - state["last_update"] >= 2
        )

        if not should_emit:
            return

        state["last_percent"] = percent
        state["last_update"] = now
        task["upload_percent"] = percent
        task["attempt_text"] = f"{attempt} of {MAX_RETRIES}"
        task["speed_text"] = human_speed(state["speed_bps"]) if state["speed_bps"] > 0 else None
        remaining = max(0, total - current)
        task["eta_text"] = (
            human_duration(remaining / state["speed_bps"])
            if remaining > 0 and state["speed_bps"] > 0
            else None
        )
        save_processing(task)
        update_telegram_status(
            task,
            stage="🚀 Uploading",
            upload_status=(
                "Finalizing the upload in Rubika."
                if raw_percent == 100
                else "Sending file to Rubika."
            ),
            attempt_text=task["attempt_text"],
        )

    return callback


def send_with_retry(
    task: dict,
    session_name: str,
    target: str,
    file_path: str,
    caption: str = "",
    file_name: str | None = None,
):
    task_id = task.get("task_id", "")
    last_error = None
    upload_name = normalize_upload_filename(
        task.get("upload_file_name") or file_name or Path(file_path).name,
        Path(file_path).name,
    )
    task["upload_file_name"] = upload_name
    used_fallback_name = False

    for attempt in range(1, MAX_RETRIES + 1):
        if is_cancelled(task_id):
            raise CancelledTaskError("Cancelled by user.")

        resuming_upload = bool(task.get(RUBIKA_UPLOAD_STATE_KEY) or task.get(RUBIKA_UPLOADED_FILE_KEY))
        if not resuming_upload:
            task["upload_percent"] = 0
        task["attempt_text"] = f"{attempt} of {MAX_RETRIES}"
        task["speed_text"] = None
        task["eta_text"] = None
        save_processing(task)
        update_telegram_status(
            task,
            stage="🚀 Starting Upload",
            upload_status=(
                "Resuming the Rubika upload."
                if resuming_upload
                else "Connecting to Rubika."
            ),
            attempt_text=task["attempt_text"],
        )

        try:
            result = asyncio.run(
                send_document(
                    session_name,
                    target,
                    file_path,
                    caption,
                    callback=make_upload_progress_callback(task, attempt),
                    file_name=upload_name,
                    task=task,
                )
            )

            if is_cancelled(task_id):
                raise CancelledTaskError("Cancelled by user.")

            return result
        except Exception as e:
            if isinstance(e, CancelledTaskError):
                raise

            last_error = e
            error_text = compact_error_text(e).lower()
            task["attempt_text"] = f"{attempt} of {MAX_RETRIES}"
            task["speed_text"] = None
            task["eta_text"] = None
            normalize_failed_progress(task)
            save_processing(task)

            transient = is_transient_upload_error(error_text)
            near_complete = int(task.get("upload_percent", 0) or 0) >= 95
            has_upload_state = bool(task.get(RUBIKA_UPLOAD_STATE_KEY))
            has_uploaded_file = bool(task.get(RUBIKA_UPLOADED_FILE_KEY))
            fallback_name_retry = (
                not used_fallback_name
                and not transient
                and not has_upload_state
                and not has_uploaded_file
            )
            retry_allowed = attempt < MAX_RETRIES and (
                transient or near_complete or fallback_name_retry
            )

            if fallback_name_retry:
                upload_name = build_fallback_upload_name(task, file_path, upload_name)
                used_fallback_name = True
                task["upload_file_name"] = upload_name
                task.pop(RUBIKA_UPLOAD_STATE_KEY, None)
                task.pop(RUBIKA_UPLOADED_FILE_KEY, None)

            if retry_allowed:
                delay = RETRY_DELAY * attempt
                next_attempt_text = f"{attempt + 1} of {MAX_RETRIES}"
                will_resume_upload = bool(
                    task.get(RUBIKA_UPLOAD_STATE_KEY) or task.get(RUBIKA_UPLOADED_FILE_KEY)
                )
                if not will_resume_upload:
                    task["upload_percent"] = 0
                task["attempt_text"] = next_attempt_text
                task["speed_text"] = None
                task["eta_text"] = None
                save_processing(task)
                reason = (
                    "temporary network issue"
                    if transient
                    else "retrying with safe filename"
                    if fallback_name_retry
                    else "failure happened near upload completion"
                )
                extra = " Retrying with a short safe filename." if fallback_name_retry else ""
                update_telegram_status(
                    task,
                    stage="⚠️ Retrying",
                    upload_status=(
                        f"Attempt {attempt} failed ({reason}). Next retry in {delay}s.{extra}"
                    ),
                    attempt_text=next_attempt_text,
                )
                wait_with_cancel(task_id, delay)
                continue

            break

    raise last_error if last_error else RuntimeError("Upload failed.")


def process_task(task: dict) -> None:
    task_type = task.get("type")
    if task_type != "local_file":
        raise RuntimeError("Unknown task type.")

    task_id = task.get("task_id", "")
    caption = task.get("caption", "")
    original_path = Path(task.get("path", ""))
    if not original_path.exists():
        raise RuntimeError("Local file not found.")

    settings = resolve_task_settings(task)
    task["rubika_session"] = settings["rubika_session"]
    task["rubika_target"] = settings["rubika_target"]
    task["rubika_target_title"] = settings["rubika_target_title"]
    task["rubika_target_type"] = settings["rubika_target_type"]
    send_path = original_path
    send_name = normalize_upload_filename(task.get("file_name") or original_path.name, original_path.name)

    try:
        if is_cancelled(task_id):
            raise CancelledTaskError("Cancelled before upload started.")

        ensure_session(settings["rubika_session"])
        update_telegram_status(
            task,
            stage="📤 Upload Queue",
            upload_status=f"Preparing the file for upload to {format_destination_label(settings)}.",
        )

        task["file_name"] = send_name
        save_processing(task)

        send_with_retry(
            task,
            settings["rubika_session"],
            settings["rubika_target"],
            str(send_path),
            caption,
            file_name=send_name,
        )
    except CancelledTaskError:
        cleanup_local_file(str(send_path))
        clear_cancelled(task_id)
        update_telegram_status(
            task,
            stage="🛑 Cancelled",
            upload_status="Transfer stopped.",
            attempt_text=task.get("attempt_text"),
            action=None,
        )
        return
    except Exception:
        clear_cancelled(task_id)
        raise

    cleanup_local_file(str(send_path))
    clear_cancelled(task_id)
    task["upload_percent"] = 100
    task["speed_text"] = None
    task["eta_text"] = None
    save_processing(task)
    elapsed_text = task_elapsed_text(task)
    update_telegram_status(
        task,
        stage="✅ Uploaded",
        upload_status=(
            f"File uploaded to {format_destination_label(settings)} successfully in {elapsed_text}."
            if elapsed_text
            else f"File uploaded to {format_destination_label(settings)} successfully."
        ),
        attempt_text=task.get("attempt_text"),
        action=None,
    )
    notify_transfer_complete(task, elapsed_text, settings)


def recover_cancelled_processing_task() -> None:
    task = load_processing()
    if not task:
        return

    task_id = task.get("task_id", "")
    if not task_id or not is_cancelled(task_id):
        return

    cleanup_local_file(task.get("path", ""))
    clear_cancelled(task_id)
    update_telegram_status(
        task,
        stage="🛑 Cancelled",
        upload_status="Transfer stopped.",
        attempt_text=task.get("attempt_text"),
        action=None,
    )
    clear_processing()


def worker_loop():
    save_worker_pid(os.getpid())
    atexit.register(clear_worker_pid)
    recover_cancelled_processing_task()
    print("Rubika worker started.")

    while True:
        task = pop_first_task()

        if not task:
            time.sleep(0.2)
            continue

        save_processing(task)

        try:
            process_task(task)
        except CancelledTaskError:
            processing_task = load_processing() or task
            clear_cancelled(processing_task.get("task_id", ""))
            update_telegram_status(
                processing_task,
                stage="🛑 Cancelled",
                upload_status="Transfer stopped.",
                attempt_text=processing_task.get("attempt_text"),
                action=None,
            )
        except Exception as e:
            processing_task = load_processing() or task
            processing_task["attempt_text"] = f"{MAX_RETRIES} of {MAX_RETRIES}"
            normalize_failed_progress(processing_task)
            save_processing(processing_task)
            error_text = compact_error_text(e)
            append_failed(processing_task, error_text)
            update_telegram_status(
                processing_task,
                stage="❌ Upload Failed",
                upload_status=(
                    f"Failed after {MAX_RETRIES} attempts. Last error: {error_text}"
                ),
                attempt_text=processing_task.get("attempt_text"),
                action="retry",
            )
        finally:
            clear_processing()


if __name__ == "__main__":
    worker_loop()
