"""Модель таблицы дел для QTableView."""
from __future__ import annotations

from PySide6.QtCore import QAbstractTableModel, QModelIndex, Qt

from app.models.domain import Case


class CasesTableModel(QAbstractTableModel):
    HEADERS = ["№ дела", "Дата", "Судья", "Док-тов", "Док-ты собраны", "Обновлено"]

    def __init__(self) -> None:
        super().__init__()
        self._rows: list[Case] = []

    def set_cases(self, cases: list[Case]) -> None:
        self.beginResetModel()
        self._rows = cases
        self.endResetModel()

    def rowCount(self, parent: QModelIndex = QModelIndex()) -> int:
        return 0 if parent.isValid() else len(self._rows)

    def columnCount(self, parent: QModelIndex = QModelIndex()) -> int:
        return len(self.HEADERS)

    def data(self, index: QModelIndex, role: int = Qt.DisplayRole):
        if not index.isValid():
            return None
        case = self._rows[index.row()]
        col = index.column()

        if role == Qt.DisplayRole:
            if col == 0:
                return case.case_number
            if col == 1:
                return case.case_date or "—"
            if col == 2:
                return case.judge or "—"
            if col == 3:
                return str(case.documents_count)
            if col == 4:
                return "Да" if case.documents_complete else "Нет"
            if col == 5:
                return case.updated_at or "—"
        if role == Qt.TextAlignmentRole and col in (3, 4):
            return int(Qt.AlignCenter)
        return None

    def headerData(self, section: int, orientation: Qt.Orientation, role: int = Qt.DisplayRole):
        if role == Qt.DisplayRole and orientation == Qt.Horizontal:
            return self.HEADERS[section]
        return None