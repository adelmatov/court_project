"""Виджет лога с цветовой подсветкой уровней."""
from __future__ import annotations

from PySide6.QtGui import QTextCursor
from PySide6.QtWidgets import QPlainTextEdit

from app.models.domain import LogLevel

_COLORS = {
    LogLevel.INFO: "#cdd2dc",
    LogLevel.DEBUG: "#7b8190",
    LogLevel.WARNING: "#fbbf24",
    LogLevel.ERROR: "#f87171",
    LogLevel.EVENT: "#4ade80",
}
_MAX_LINES = 5000  # ограничение для производительности


class LogView(QPlainTextEdit):
    def __init__(self) -> None:
        super().__init__()
        self.setObjectName("LogView")
        self.setReadOnly(True)
        self.setMaximumBlockCount(_MAX_LINES)

    def append_line(self, text: str, level: LogLevel = LogLevel.INFO) -> None:
        color = _COLORS.get(level, "#cdd2dc")
        # Экранируем спецсимволы HTML
        safe = (
            text.replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
        )
        self.appendHtml(f'<span style="color:{color};">{safe}</span>')
        self.moveCursor(QTextCursor.End)

    def clear_log(self) -> None:
        self.clear()