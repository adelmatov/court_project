"""Страница «Dashboard»: карточки со статистикой из БД."""
from __future__ import annotations

from PySide6.QtCore import Signal
from PySide6.QtWidgets import (
    QGridLayout,
    QHBoxLayout,
    QLabel,
    QPushButton,
    QVBoxLayout,
    QWidget,
)

from app.models.domain import DashboardStats
from app.ui.widgets.stat_card import StatCard


class DashboardPage(QWidget):
    refresh_requested = Signal()

    def __init__(self) -> None:
        super().__init__()
        root = QVBoxLayout(self)
        root.setContentsMargins(24, 24, 24, 24)
        root.setSpacing(18)

        header = QHBoxLayout()
        title = QLabel("Дашборд")
        title.setStyleSheet("font-size: 22px; font-weight: 700;")
        header.addWidget(title)
        header.addStretch()
        self.refresh_btn = QPushButton("⟳  Обновить")
        self.refresh_btn.clicked.connect(self.refresh_requested)
        header.addWidget(self.refresh_btn)
        root.addLayout(header)

        # статус подключения к БД
        self.db_status = QLabel("База данных: проверка…")
        self.db_status.setObjectName("StatusStopped")
        root.addWidget(self.db_status)

        # карточки
        grid = QGridLayout()
        grid.setSpacing(16)
        self.card_cases = StatCard("Всего дел")
        self.card_docs = StatCard("Скачано документов")
        self.card_no_docs = StatCard("Дел без полных документов")
        self.card_no_judge = StatCard("Дел без судьи")
        grid.addWidget(self.card_cases, 0, 0)
        grid.addWidget(self.card_docs, 0, 1)
        grid.addWidget(self.card_no_docs, 1, 0)
        grid.addWidget(self.card_no_judge, 1, 1)
        root.addLayout(grid)
        root.addStretch()

    def set_stats(self, stats: DashboardStats) -> None:
        self.card_cases.set_value(stats.total_cases)
        self.card_docs.set_value(stats.total_documents)
        self.card_no_docs.set_value(stats.cases_without_docs)
        self.card_no_judge.set_value(stats.cases_without_judge)

    def set_db_status(self, ok: bool, message: str = "") -> None:
        if ok:
            self.db_status.setText("База данных: подключено ✓")
            self.db_status.setObjectName("StatusRunning")
        else:
            self.db_status.setText(f"База данных: ошибка — {message}")
            self.db_status.setObjectName("StatusError")
        self.db_status.style().unpolish(self.db_status)
        self.db_status.style().polish(self.db_status)