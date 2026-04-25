import os
import subprocess
import sys
import time
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent


def maybe_reexec_with_venv() -> None:
    venv_python = BASE_DIR / "venv" / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
    if not venv_python.exists():
        return

    if Path(sys.executable).resolve() == venv_python.resolve():
        return

    os.execv(str(venv_python), [str(venv_python), *sys.argv])


maybe_reexec_with_venv()

telegram_file = BASE_DIR / "telegram_bot.py"
rubika_file = BASE_DIR / "rubika_worker.py"

telegram_proc = None
rubika_proc = None


def start_process(path: Path):
    return subprocess.Popen([sys.executable, str(path)])


try:
    rubika_proc = start_process(rubika_file)
    telegram_proc = start_process(telegram_file)

    while True:
        if telegram_proc.poll() is not None:
            break

        if rubika_proc.poll() is not None:
            rubika_proc = start_process(rubika_file)

        time.sleep(1)

except KeyboardInterrupt:
    pass
finally:
    for proc in [telegram_proc, rubika_proc]:
        if proc and proc.poll() is None:
            proc.terminate()
