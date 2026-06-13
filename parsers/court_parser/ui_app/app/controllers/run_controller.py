"""Контроллер запуска: связывает кнопки UI, ProcessService и LogService."""
from __future__ import annotations

from PySide6.QtCore import QObject

from app.core import paths
from app.models.domain import LogLevel
from app.services.log_service import LogService
from app.services.process_service import ProcessService
from app.ui.widgets.monitor_page import MonitorPage
from app.ui.widgets.run_page import RunPage


class RunController(QObject):
    def __init__(
        self,
        process: ProcessService,
        log: LogService,
        run_page: RunPage,
        monitor_page: MonitorPage,
    ) -> None:
        super().__init__()
        self._process = process
        self._log = log
        self._run_page = run_page
        self._monitor_page = monitor_page
        self._wire()

    def _wire(self) -> None:
        # UI -> процесс
        self._run_page.start_requested.connect(self._on_start)
        self._run_page.stop_requested.connect(self._process.stop)
        self._monitor_page.reattach_requested.connect(self._attach_log_file)

        # процесс -> UI
        self._process.stdout_line.connect(self._on_stdout)
        self._process.stderr_line.connect(self._on_stderr)
        self._process.state_changed.connect(self._run_page.on_state_changed)

        # лог-файл -> Monitor
        self._log.new_line.connect(self._monitor_page.log_view.append_line)
        self._log.counters_changed.connect(self._monitor_page.set_counters)

    def _on_start(self, mode: str) -> None:
        self._run_page.log_view.clear_log()
        self._log.reset_counters()
        self._process.start(mode, workdir=paths.WORKING_DIR)
        self._attach_log_file()

    def _attach_log_file(self) -> None:
        log_file = paths.latest_log_file()
        if log_file:
            self._log.watch(log_file)

    def _on_stdout(self, line: str) -> None:
        # Все строки из stdout (включая UI) выводим на Run page
        level = self._log.detect_level(line)
        self._run_page.log_view.append_line(line, level)
        
        # Если это строка из терминального UI, помечаем как EVENT
        if "│" in line or "✓" in line or "✗" in line:
            self._log.process_line(line)  # Обновляет счётчики

    def _on_stderr(self, line: str) -> None:
        # stderr — это обычно ошибки
        self._run_page.log_view.append_line(line, LogLevel.ERROR)
        self._monitor_page.log_view.append_line(line, LogLevel.ERROR)