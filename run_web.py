"""Веб TraderBot v0.1. Отдельный процесс: перезапуск веба не трогает движок.
Запуск:  python run_web.py   → http://127.0.0.1:5000"""
import logging

from engine import config
from web.app import create_app

if __name__ == "__main__":
    cfg = config.load()
    w = cfg.get("web", {})
    logging.getLogger("werkzeug").setLevel(logging.WARNING)
    app = create_app()
    print(f"Веб: http://{w.get('host', '127.0.0.1')}:{w.get('port', 5000)}")
    app.run(host=w.get("host", "127.0.0.1"), port=int(w.get("port", 5000)),
            threaded=True, debug=False, use_reloader=False)
