"""Страница «Run»: выбор режима, запуск/остановка, живой лог."""
from __future__ import annotations

from PySide6.QtCore import Signal
from PySide6.QtWidgets import (
    QComboBox,
    QHBoxLayout,
    QLabel,
    QProgressBar,
    QVBoxLayout,
    QPushButton,
    QWidget,
)

from app.models.domain import PARSER_MODES, ProcessState
from app.ui.widgets.log_view import LogView


class RunPage(QWidget):
    start_requested = Signal(str)   # передаёт выбранный режим
    stop_requested = Signal()

    def __init__(self) -> None:
        super().__init__()
        root = QVBoxLayout(self)
        root.setContentsMargins(24, 24, 24, 24)
        root.setSpacing(16)

        title = QLabel("Запуск парсера")
        title.setStyleSheet("font-size: 22px; font-weight: 700;")
        root.addWidget(title)

        # --- панель управления ---
        controls = QHBoxLayout()

        self.mode_box = QComboBox()
        for cli_value, label in PARSER_MODES.items():
            self.mode_box.addItem(label, cli_value)

        self.start_btn = QPushButton("▶  Запустить")
        self.stop_btn = QPushButton("■  Остановить")
        self.stop_btn.setObjectName("DangerButton")
        self.stop_btn.setEnabled(False)

        self.status_label = QLabel("Idle")
        self.status_label.setObjectName("StatusStopped")

        controls.addWidget(QLabel("Режим:"))
        controls.addWidget(self.mode_box, 1)
        controls.addWidget(self.start_btn)
        controls.addWidget(self.stop_btn)
        controls.addStretch()
        controls.addWidget(QLabel("Статус:"))
        controls.addWidget(self.status_label)
        root.addLayout(controls)

        # --- индикатор работы ---
        self.progress = QProgressBar()
        self.progress.setRange(0, 0)        # «бегущая» полоса
        self.progress.setVisible(False)
        root.addWidget(self.progress)

        # --- лог ---
        self.log_view = LogView()
        root.addWidget(self.log_view, 1)

        # связи
        self.start_btn.clicked.connect(self._on_start)
        self.stop_btn.clicked.connect(self.stop_requested)

    def _on_start(self) -> None:
        self.start_requested.emit(self.mode_box.currentData())

    def on_state_changed(self, state: ProcessState) -> None:
        running = state in (
            ProcessState.STARTING,
            ProcessState.RUNNING,
            ProcessState.STOPPING,
        )
        self.start_btn.setEnabled(not running)
        self.stop_btn.setEnabled(running)
        self.progress.setVisible(running)
        self.mode_box.setEnabled(not running)

        labels = {
            ProcessState.IDLE: ("Idle", "StatusStopped"),
            ProcessState.STARTING: ("Starting…", "StatusRunning"),
            ProcessState.RUNNING: ("Running", "StatusRunning"),
            ProcessState.STOPPING: ("Stopping…", "StatusRunning"),
            ProcessState.FINISHED: ("Finished", "StatusStopped"),
            ProcessState.ERROR: ("Error", "StatusError"),
        }
        text, obj_name = labels[state]
        self.status_label.setText(text)
        self.status_label.setObjectName(obj_name)
        # обновляем стиль (чтобы цвет сменился)
        self.status_label.style().unpolish(self.status_label)
        self.status_label.style().polish(self.status_label)