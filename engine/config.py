import hashlib
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def load(path: str | None = None) -> dict:
    p = Path(path) if path else ROOT / "config.toml"
    raw = p.read_bytes()
    cfg = tomllib.loads(raw.decode("utf-8"))
    cfg["_hash"] = hashlib.sha256(raw).hexdigest()[:12]
    cfg["_root"] = ROOT
    cfg["_secrets"] = load_secrets()
    return cfg


def load_secrets() -> dict:
    """Секреты — в отдельном файле secrets.toml рядом с config.toml (в хеш конфига и в выгрузку не попадает).
    Переменные окружения BINANCE_API_KEY и BINANCE_API_SECRET, если заданы, главнее файла."""
    import os
    p = ROOT / "secrets.toml"
    sec = tomllib.loads(p.read_text(encoding="utf-8")) if p.exists() else {}
    bn = sec.setdefault("binance", {})
    if os.environ.get("BINANCE_API_KEY"):
        bn["api_key"] = os.environ["BINANCE_API_KEY"]
    if os.environ.get("BINANCE_API_SECRET"):
        bn["api_secret"] = os.environ["BINANCE_API_SECRET"]
    return sec


def path_of(cfg: dict, key: str) -> Path:
    p = Path(cfg["paths"][key])
    return p if p.is_absolute() else cfg["_root"] / p
