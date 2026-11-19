"""Запуск движка с автоматическим перезапуском.

    python run_forever.py

Запускает run_engine.py и следит за ним:
  • код выхода 3 — сторож запросил жёсткий перезапуск: через 5 с движок запускается заново;
  • любая другая ошибка (упал) — перезапуск с паузой 10 с, при повторных падениях подряд пауза растёт до 2 мин;
  • Ctrl+C или нормальная остановка — выход; на Ctrl+C сначала ждём, пока движок штатно остановится.
Все перезапуски пишутся в logs/run_forever.log.
"""
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent
LOG = ROOT / "logs" / "run_forever.log"
RESET_CODE = 3


def note(msg: str) -> None:
    line = f"{datetime.now():%d.%m %H:%M:%S} {msg}"
    print(line, flush=True)
    LOG.parent.mkdir(parents=True, exist_ok=True)
    with open(LOG, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def main() -> None:
    crashes = 0
    while True:
        started = time.time()
        note("запуск движка")
        proc = subprocess.Popen([sys.executable, str(ROOT / "run_engine.py")], cwd=ROOT)
        try:
            code = proc.wait()
        except KeyboardInterrupt:
            # Ctrl+C получают оба процесса (общая консоль). Движок останавливается штатно: сбрасывает очередь
            # записи и закрывает соединения — ждём его, а не обрываем.
            note("Ctrl+C — жду, пока движок штатно остановится (до 30 с)")
            deadline = time.time() + 30
            while proc.poll() is None and time.time() < deadline:
                try:
                    proc.wait(timeout=1)
                except (KeyboardInterrupt, subprocess.TimeoutExpired):
                    pass
            if proc.poll() is None:
                proc.kill()
                note("движок не остановился за 30 с — завершён принудительно")
            note("остановлено (Ctrl+C)")
            return
        if code == 0:
            note("движок остановлен штатно — выхожу")
            return
        if code == RESET_CODE:
            crashes = 0
            note("жёсткий перезапуск по сигналу сторожа — запускаю заново через 5 с")
            time.sleep(5)
            continue
        crashes = crashes + 1 if time.time() - started < 600 else 1
        pause = min(10 * 2 ** (crashes - 1), 120)
        note(f"движок упал (код {code}) — перезапуск через {pause} с")
        try:
            time.sleep(pause)
        except KeyboardInterrupt:
            note("остановлено (Ctrl+C)")
            return


if __name__ == "__main__":
    main()
