"""Карточка статистики для дашборда."""
from __future__ import annotations

from PySide6.QtCore import Qt
from PySide6.QtWidgets import QFrame, QLabel, QVBoxLayout


class StatCard(QFrame):
    def __init__(self, title: str) -> None:
        super().__init__()
        self.setObjectName("StatCard")
        self.setMinimumHeight(110)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(18, 16, 18, 16)

        self._title = QLabel(title)
        self._title.setObjectName("StatCardTitle")

        self._value = QLabel("—")
        self._value.setObjectName("StatCardValue")

        layout.addWidget(self._title)
        layout.addWidget(self._value)
        layout.addStretch()

    def set_value(self, value: int | str) -> None:
        if isinstance(value, int):
            self._value.setText(f"{value:,}".replace(",", " "))
        else:
            self._value.setText(str(value))