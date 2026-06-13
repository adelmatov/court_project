"""Страница «Monitor»: tail лог-файла + счётчики событий/warnings."""
from __future__ import annotations

from PySide6.QtCore import Signal
from PySide6.QtWidgets import (
    QHBoxLayout,
    QLabel,
    QPushButton,
    QVBoxLayout,
    QWidget,
)

from app.ui.widgets.log_view import LogView


class MonitorPage(QWidget):
    reattach_requested = Signal()

    def __init__(self) -> None:
        super().__init__()
        root = QVBoxLayout(self)
        root.setContentsMargins(24, 24, 24, 24)
        root.setSpacing(12)

        title = QLabel("Мониторинг логов")
        title.setStyleSheet("font-size: 22px; font-weight: 700;")
        root.addWidget(title)

        bar = QHBoxLayout()
        self.events_label = QLabel("Saved: 0")
        self.events_label.setObjectName("StatusRunning")
        self.warnings_label = QLabel("Warnings: 0")
        self.warnings_label.setObjectName("StatusError")
        self.reattach_btn = QPushButton("Перечитать последний лог")
        bar.addWidget(self.events_label)
        bar.addWidget(self.warnings_label)
        bar.addStretch()
        bar.addWidget(self.reattach_btn)
        root.addLayout(bar)

        self.log_view = LogView()
        root.addWidget(self.log_view, 1)

        self.reattach_btn.clicked.connect(self.reattach_requested)

    def set_counters(self, events: int, warnings: int) -> None:
        self.events_label.setText(f"Saved: {events}")
        self.warnings_label.setText(f"Warnings: {warnings}")