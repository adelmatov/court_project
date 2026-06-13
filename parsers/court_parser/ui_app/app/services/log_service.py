"""Сервис разбора и слежения за логами парсера."""
from __future__ import annotations

import re
from pathlib import Path

from PySide6.QtCore import QObject, QTimer, Signal

from app.models.domain import LogLevel

# Регулярные выражения под формат логов твоего парсера.
# Пример строки в консоли: "[23:31:55] INFO  🚀 Парсер инициализирован"
_LEVEL_RE = re.compile(r"\b(INFO|WARNING|ERROR|DEBUG|CRITICAL)\b")
_PIPELINE_COMPLETE_RE = re.compile(r"PIPELINE COMPLETE", re.IGNORECASE)


class LogService(QObject):
    """
    Разбирает строки логов и следит за лог-файлом.

    Сигналы:
      new_line(str, LogLevel)     — строка + распознанный уровень
      pipeline_complete()         — встречен маркер PIPELINE COMPLETE
      counters_changed(int, int)  — (events, warnings)
    """

    new_line = Signal(str, LogLevel)
    pipeline_complete = Signal()
    counters_changed = Signal(int, int)

    def __init__(self, parent: QObject | None = None) -> None:
        super().__init__(parent)
        self._path: Path | None = None
        self._offset = 0
        self._events = 0
        self._warnings = 0

        # Поллинг файла каждые 800 мс (надёжнее, чем file watcher).
        self._timer = QTimer(self)
        self._timer.setInterval(800)
        self._timer.timeout.connect(self._read_new)

    # ---------- слежение за файлом ----------

    def watch(self, path: Path) -> None:
        """Начать слежение за файлом с его текущего конца (только новые строки)."""
        self.stop()
        self._path = path
        # Начинаем с конца файла, чтобы не выводить старое
        self._offset = path.stat().st_size if path.exists() else 0
        self._timer.start()

    def stop(self) -> None:
        self._timer.stop()
        self._path = None

    def reset_counters(self) -> None:
        self._events = 0
        self._warnings = 0
        self.counters_changed.emit(self._events, self._warnings)

    def _read_new(self) -> None:
        if not self._path or not self._path.exists():
            return
        size = self._path.stat().st_size
        if size < self._offset:        # файл пересоздан (новая сессия)
            self._offset = 0
        if size == self._offset:
            return

        with self._path.open("r", encoding="utf-8", errors="replace") as f:
            f.seek(self._offset)
            chunk = f.read()
            self._offset = f.tell()

        for line in chunk.splitlines():
            self.process_line(line)

    # ---------- разбор строки ----------

    def process_line(self, line: str) -> None:
        """Разбирает строку (из файла или из stdout процесса) и шлёт сигналы."""
        level = self.detect_level(line)

        if _PIPELINE_COMPLETE_RE.search(line):
            self.pipeline_complete.emit()

        changed = False
        if level == LogLevel.WARNING:
            self._warnings += 1
            changed = True
        # Считаем «Сохранено»/"Saved" как событие прогресса
        if "Сохранено" in line or "Saved:" in line:
            self._events += 1
            changed = True

        self.new_line.emit(line, level)
        if changed:
            self.counters_changed.emit(self._events, self._warnings)

    @staticmethod
    def detect_level(line: str) -> LogLevel:
        m = _LEVEL_RE.search(line)
        if m:
            value = m.group(1)
            if value == "CRITICAL":
                return LogLevel.ERROR
            return LogLevel(value)
        return LogLevel.INFO