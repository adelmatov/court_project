"""Сервис запуска парсера через QProcess (без блокировки UI)."""
from __future__ import annotations

import sys
from pathlib import Path

from PySide6.QtCore import QObject, QProcess, Signal, QProcessEnvironment

from app.models.domain import ProcessState


class ProcessService(QObject):
    """
    Управляет дочерним процессом парсера.

    Сигналы (на них подпишется контроллер):
      stdout_line(str)        — новая строка из стандартного вывода
      stderr_line(str)        — новая строка из потока ошибок
      state_changed(state)    — изменилось состояние процесса
      finished(int)           — процесс завершился (с кодом возврата)
    """

    stdout_line = Signal(str)
    stderr_line = Signal(str)
    state_changed = Signal(ProcessState)
    finished = Signal(int)

    def __init__(self, parser_entry: Path, parent: QObject | None = None) -> None:
        super().__init__(parent)
        self._entry = parser_entry
        self._state = ProcessState.IDLE
        self._proc: QProcess | None = None
        # Буферы — данные приходят кусками, собираем в целые строки
        self._stdout_buf = ""
        self._stderr_buf = ""

    @property
    def state(self) -> ProcessState:
        return self._state

    def is_running(self) -> bool:
        return self._proc is not None and self._proc.state() != QProcess.NotRunning

    def start(self, mode: str, workdir: Path) -> None:
        """
        Запускает: python main.py --mode <mode>

        mode может содержать пробел ("update judge") — тогда он
        разбивается на несколько аргументов автоматически.
        """
        if self.is_running():
            return

        self._set_state(ProcessState.STARTING)

        self._proc = QProcess(self)
        # sys.executable — это python из нашего venv.
        # ВАЖНО: парсер должен запускаться своим python'ом.
        # На Этапе 7 мы разберём вопрос отдельного python парсера.
        from app.core.paths import parser_python
        self._proc.setProgram(parser_python())

        # Собираем аргументы: [путь_к_main.py, "--mode", "update", "judge"]
        args = [str(self._entry), "--mode", *mode.split()]
        self._proc.setArguments(args)

        # Рабочая папка = корень парсера (чтобы он нашёл config.json и logs/)
        self._proc.setWorkingDirectory(str(workdir))

        # Подписываемся на события процесса
        self._proc.readyReadStandardOutput.connect(self._on_stdout)
        self._proc.readyReadStandardError.connect(self._on_stderr)
        self._proc.finished.connect(self._on_finished)
        self._proc.errorOccurred.connect(self._on_error)

        self._proc.start()
        self._set_state(ProcessState.RUNNING)

    def stop(self) -> None:
        """Останавливает процесс: сначала мягко, затем принудительно."""
        if not self.is_running() or self._proc is None:
            return
        self._set_state(ProcessState.STOPPING)
        self._proc.terminate()                      # мягкая остановка
        if not self._proc.waitForFinished(5000):    # ждём 5 секунд
            self._proc.kill()                       # принудительно

    # ---------- внутренние обработчики ----------

    def _on_stdout(self) -> None:
        if self._proc is None:
            return
        data = bytes(self._proc.readAllStandardOutput()).decode("utf-8", "replace")
        self._stdout_buf += data
        self._stdout_buf = self._flush_lines(self._stdout_buf, self.stdout_line)

    def _on_stderr(self) -> None:
        if self._proc is None:
            return
        data = bytes(self._proc.readAllStandardError()).decode("utf-8", "replace")
        self._stderr_buf += data
        self._stderr_buf = self._flush_lines(self._stderr_buf, self.stderr_line)

    @staticmethod
    def _flush_lines(buffer: str, signal: Signal) -> str:
        """Отправляет завершённые строки, возвращает «хвост» без перевода строки."""
        *lines, rest = buffer.split("\n")
        for line in lines:
            signal.emit(line.rstrip("\r"))
        return rest

    def _on_finished(self, exit_code: int, _status) -> None:
        # Выводим остатки буферов
        if self._stdout_buf:
            self.stdout_line.emit(self._stdout_buf)
            self._stdout_buf = ""
        if self._stderr_buf:
            self.stderr_line.emit(self._stderr_buf)
            self._stderr_buf = ""
        self._set_state(ProcessState.FINISHED)
        self.finished.emit(exit_code)
        self._proc = None

    def _on_error(self, _error) -> None:
        self._set_state(ProcessState.ERROR)

    def _set_state(self, state: ProcessState) -> None:
        self._state = state
        self.state_changed.emit(state)