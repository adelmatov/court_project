"""Централизованное управление путями приложения.

Работает в двух режимах:
  • Запуск через python -m app.main  (разработка)
  • Запуск как собранный .exe        (PyInstaller)
"""
from __future__ import annotations

import sys
from pathlib import Path


def _project_root() -> Path:
    """Определяет корень court_project в обоих режимах запуска."""
    if getattr(sys, "frozen", False):
        # Режим .exe: sys.executable = путь к .exe
        # .exe будет лежать в court_parser/ui_app/dist/
        # dist -> ui_app -> court_parser -> parsers -> court_project
        exe_dir = Path(sys.executable).resolve().parent      # dist/
        return exe_dir.parents[4]                            # court_project
    # Режим разработки: от этого файла
    # core -> app -> ui_app -> court_parser -> parsers -> court_project
    return Path(__file__).resolve().parents[5]


PROJECT_ROOT = _project_root()
PARSER_DIR = PROJECT_ROOT / "parsers" / "court_parser"

PARSER_ENTRY = PARSER_DIR / "main.py"
CONFIG_PATH = PARSER_DIR / "config.json"

WORKING_DIR = PROJECT_ROOT
LOGS_DIR = PROJECT_ROOT / "logs"


def ensure_dirs() -> None:
    LOGS_DIR.mkdir(parents=True, exist_ok=True)


def latest_log_file() -> Path | None:
    if not LOGS_DIR.exists():
        return None
    logs = sorted(
        LOGS_DIR.glob("*.log"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    return logs[0] if logs else None


def parser_python() -> str:
    candidates = [
        PROJECT_ROOT / "venv" / "Scripts" / "python.exe",
        PROJECT_ROOT / "venv" / "bin" / "python",
        PROJECT_ROOT / ".venv" / "Scripts" / "python.exe",
        PROJECT_ROOT / ".venv" / "bin" / "python",
    ]
    for candidate in candidates:
        if candidate.exists():
            return str(candidate)
    return sys.executable