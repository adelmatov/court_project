"""Точка входа приложения Court Parser Control Center."""
from __future__ import annotations

import sys

from PySide6.QtWidgets import QApplication

from app.core import paths
from app.core.theme import DARK_QSS
from app.ui.main_window import MainWindow


def main() -> int:
    # Гарантируем, что папка логов существует
    paths.ensure_dirs()

    app = QApplication(sys.argv)
    app.setApplicationName("Court Parser Control Center")
    app.setStyleSheet(DARK_QSS)

    window = MainWindow()
    window.show()
    return app.exec()


if __name__ == "__main__":
    sys.exit(main())