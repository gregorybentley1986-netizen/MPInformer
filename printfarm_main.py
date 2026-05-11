"""
Точка входа для запуска PrintFarm через uvicorn из корня репозитория MPInformer.

Использует общий каталог `app/` этого проекта. Отдельное приложение PrintFarm
лежит рядом: `Documents/Cursor Project/PrintFarm` — там свои `run.py` и
`printfarm_main.py` (рабочий каталог — корень того репозитория).

Служебная логика (ожидание PID при перезапуске) повторяет `main.py` из MPInformer,
но привязывается к `app.printfarm_main:app`.
"""

from __future__ import annotations

import multiprocessing
import os
import sys
import time
from pathlib import Path

import uvicorn

from app.config import settings


if __name__ == "__main__":
    print("[PrintFarm] Запуск...", flush=True)

    if hasattr(multiprocessing, "set_start_method"):
        try:
            multiprocessing.set_start_method("spawn", force=True)
        except RuntimeError:
            pass

    project_root = Path(__file__).resolve().parent
    pid_file = project_root / "restart_pid_printfarm.txt"

    if pid_file.exists():
        os.environ["PRINTFARM_DELAY_TELEGRAM"] = "1"
        try:
            old_pid_str = pid_file.read_text(encoding="utf-8").strip()
            pid_file.unlink(missing_ok=True)
            old_pid = int(old_pid_str)
        except (ValueError, TypeError, OSError):
            old_pid = None
        else:
            print(
                f"[PrintFarm] Ожидание завершения процесса {old_pid} (перезапуск)...",
                flush=True,
            )

            def _process_alive(pid: int) -> bool:
                if sys.platform == "win32":
                    import ctypes

                    kernel32 = ctypes.windll.kernel32  # type: ignore[attr-defined]
                    SYNCHRONIZE = 0x100000
                    h = kernel32.OpenProcess(SYNCHRONIZE, 0, pid)
                    if h:
                        kernel32.CloseHandle(h)
                        return True
                    return False
                try:
                    os.kill(pid, 0)
                    return True
                except ProcessLookupError:
                    return False
                except PermissionError:
                    return False

            wait_start = time.monotonic()
            timeout_sec = 20.0
            while _process_alive(old_pid):
                if time.monotonic() - wait_start > timeout_sec:
                    print(f"[PrintFarm] Таймаут ожидания процесса {old_pid}, продолжаем...", flush=True)
                    break
                time.sleep(0.3)
            else:
                print("[PrintFarm] Процесс завершён, ждём 3 сек...", flush=True)
                time.sleep(3.0)

    host = "0.0.0.0"
    port = settings.server_port
    max_bind_retries = 10

    sock = None
    for attempt in range(max_bind_retries):
        try:
            import socket as sock_mod

            s = sock_mod.socket(sock_mod.AF_INET, sock_mod.SOCK_STREAM)
            s.setsockopt(sock_mod.SOL_SOCKET, sock_mod.SO_REUSEADDR, 1)
            s.bind((host, port))
            sock = s
            if attempt > 0:
                print(f"[PrintFarm] Порт {port} свободен (попытка {attempt + 1}).", flush=True)
            break
        except OSError as e:
            port_in_use = (
                getattr(e, "winerror", None) == 10048
                or getattr(e, "errno", None) in (98, 48, 10048)
                or "10048" in str(e)
                or "address already in use" in str(e).lower()
                or "использование адреса сокета" in str(e).lower()
            )
            if attempt < max_bind_retries - 1:
                print(f"[PrintFarm] Порт {port} недоступен: {e}", flush=True)
                print(f"[PrintFarm] Повтор через 2 сек (попытка {attempt + 1}/{max_bind_retries})...", flush=True)
                time.sleep(2.0)
                continue
            print(f"[PrintFarm] Не удалось занять порт {port} после {max_bind_retries} попыток.", flush=True)
            raise

    if sock is None:
        raise RuntimeError("Не удалось привязаться к порту")

    config = uvicorn.Config(
        "app.printfarm_main:app",
        host=host,
        port=port,
        reload=False,
        log_level=settings.log_level.lower(),
    )
    server = uvicorn.Server(config)
    server.run(sockets=[sock])

