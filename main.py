"""
Точка входа приложения
"""
import sys
from pathlib import Path

# Добавляем корневую директорию проекта в PYTHONPATH
project_root = Path(__file__).parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

import uvicorn
from app.config import settings

if __name__ == "__main__":
    print("[MPInformer] Запуск...", flush=True)
    # Используем spawn метод для multiprocessing в Windows
    import multiprocessing
    if hasattr(multiprocessing, 'set_start_method'):
        try:
            multiprocessing.set_start_method('spawn', force=True)
        except RuntimeError:
            pass  # Уже установлен
    
    # При перезапуске из админки старый процесс записывает свой PID в restart_pid.txt
    # Читаем файл и ждём завершения того процесса, затем даём ОС освободить порт
    import time
    import os
    project_root = Path(__file__).resolve().parent
    pid_file = project_root / "restart_pid.txt"
    if pid_file.exists():
        os.environ["MPINFORMER_DELAY_TELEGRAM"] = "1"  # новый процесс задержит запуск бота, чтобы старый успел освободить getUpdates
        try:
            old_pid_str = pid_file.read_text(encoding="utf-8").strip()
            pid_file.unlink(missing_ok=True)  # удаляем сразу, чтобы не мешать следующим запускам
            old_pid = int(old_pid_str)
        except (ValueError, TypeError, OSError):
            old_pid = None
        else:
            print(f"[MPInformer] Ожидание завершения процесса {old_pid} (перезапуск из админки), макс. 20 сек...", flush=True)
            def _process_alive(pid):
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
            # Ждём завершения старого процесса, но не более 20 сек (чтобы не зависнуть)
            wait_start = time.monotonic()
            timeout_sec = 20.0
            while _process_alive(old_pid):
                if time.monotonic() - wait_start > timeout_sec:
                    print(f"[MPInformer] Таймаут ожидания процесса {old_pid}, продолжаем...", flush=True)
                    break
                time.sleep(0.3)
            else:
                print("[MPInformer] Процесс завершён, ждём 3 сек перед привязкой к порту...", flush=True)
                time.sleep(3.0)  # даём ОС освободить порт (на Windows может быть задержка)
    
    # Запуск uvicorn: привязка к порту с повторными попытками (ручной перезапуск или после перезапуска из админки)
    import socket as sock_mod
    host = "0.0.0.0"
    port = settings.server_port
    max_bind_retries = 10
    sock = None
    for attempt in range(max_bind_retries):
        try:
            s = sock_mod.socket(sock_mod.AF_INET, sock_mod.SOCK_STREAM)
            s.setsockopt(sock_mod.SOL_SOCKET, sock_mod.SO_REUSEADDR, 1)
            s.bind((host, port))
            sock = s
            if attempt > 0:
                print(f"[MPInformer] Порт {port} свободен (попытка {attempt + 1}).", flush=True)
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
                print(f"[MPInformer] Порт {port} недоступен: {e}", flush=True)
                print(f"[MPInformer] Повтор через 2 сек (попытка {attempt + 1}/{max_bind_retries})...", flush=True)
                time.sleep(2.0)
                continue
            print(f"[MPInformer] Не удалось занять порт {port} после {max_bind_retries} попыток.", flush=True)
            raise
    if sock is None:
        raise RuntimeError("Не удалось привязаться к порту")
    config = uvicorn.Config(
        "app.main:app",
        host=host,
        port=port,
        reload=False,
        log_level=settings.log_level.lower(),
    )
    server = uvicorn.Server(config)
    server.run(sockets=[sock])
