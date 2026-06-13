"""Выполнение запросов к БД в фоновом потоке (чтобы не блокировать UI)."""
from __future__ import annotations

from typing import Any, Callable

from PySide6.QtCore import QObject, QRunnable, QThreadPool, Signal


class _Signals(QObject):
    success = Signal(object)   # результат запроса
    error = Signal(str)        # текст ошибки


class _Task(QRunnable):
    def __init__(self, fn: Callable[..., Any], *args: Any) -> None:
        super().__init__()
        self.signals = _Signals()
        self._fn = fn
        self._args = args

    def run(self) -> None:
        try:
            result = self._fn(*self._args)
            self.signals.success.emit(result)
        except Exception as exc:  # noqa: BLE001
            self.signals.error.emit(str(exc))


def run_async(
    fn: Callable[..., Any],
    *args: Any,
    on_success: Callable[[Any], None],
    on_error: Callable[[str], None],
) -> None:
    """Запускает fn(*args) в пуле потоков и вызывает колбэки в главном потоке."""
    task = _Task(fn, *args)
    task.signals.success.connect(on_success)
    task.signals.error.connect(on_error)
    QThreadPool.globalInstance().start(task)