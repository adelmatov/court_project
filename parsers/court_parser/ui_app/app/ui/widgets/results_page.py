"""Страница «Results»: таблица дел с поиском."""
from __future__ import annotations

from PySide6.QtCore import Signal
from PySide6.QtWidgets import (
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QLineEdit,
    QPushButton,
    QTableView,
    QVBoxLayout,
    QWidget,
)

from app.models.domain import Case
from app.models.table_models import CasesTableModel


class ResultsPage(QWidget):
    search_requested = Signal(str)

    def __init__(self) -> None:
        super().__init__()
        root = QVBoxLayout(self)
        root.setContentsMargins(24, 24, 24, 24)
        root.setSpacing(12)

        title = QLabel("Дела")
        title.setStyleSheet("font-size: 22px; font-weight: 700;")
        root.addWidget(title)

        bar = QHBoxLayout()
        self.search_input = QLineEdit()
        self.search_input.setPlaceholderText("Поиск по номеру дела или судье…")
        self.search_btn = QPushButton("Найти")
        self.count_label = QLabel("")
        self.count_label.setStyleSheet("color: #9aa0ac;")
        bar.addWidget(self.search_input, 1)
        bar.addWidget(self.search_btn)
        bar.addStretch()
        bar.addWidget(self.count_label)
        root.addLayout(bar)

        self.table = QTableView()
        self.model = CasesTableModel()
        self.table.setModel(self.model)
        self.table.setAlternatingRowColors(True)
        self.table.setSelectionBehavior(QTableView.SelectRows)
        self.table.verticalHeader().setVisible(False)
        self.table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeToContents)
        self.table.horizontalHeader().setSectionResizeMode(2, QHeaderView.Stretch)
        root.addWidget(self.table, 1)

        self.search_btn.clicked.connect(self._emit_search)
        self.search_input.returnPressed.connect(self._emit_search)

    def _emit_search(self) -> None:
        self.search_requested.emit(self.search_input.text())

    def set_cases(self, cases: list[Case]) -> None:
        self.model.set_cases(cases)
        self.count_label.setText(f"Показано: {len(cases)}")