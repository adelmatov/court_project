"""Контроллер дашборда и таблицы дел: связывает UI с DbService."""
from __future__ import annotations

from PySide6.QtCore import QObject

from app.models.domain import Case, DashboardStats
from app.services.db_service import DbService
from app.services.db_worker import run_async
from app.ui.widgets.dashboard_page import DashboardPage
from app.ui.widgets.results_page import ResultsPage


class DashboardController(QObject):
    def __init__(
        self,
        db: DbService,
        dashboard_page: DashboardPage,
        results_page: ResultsPage,
    ) -> None:
        super().__init__()
        self._db = db
        self._dashboard = dashboard_page
        self._results = results_page

        self._dashboard.refresh_requested.connect(self.refresh)
        self._results.search_requested.connect(self.search_cases)

    def refresh(self) -> None:
        """Обновляет дашборд: проверка БД + статистика."""
        run_async(
            self._db.test_connection,
            on_success=self._on_conn_checked,
            on_error=lambda msg: self._dashboard.set_db_status(False, msg),
        )

    def _on_conn_checked(self, ok: bool) -> None:
        if not ok:
            self._dashboard.set_db_status(False, "нет подключения")
            return
        self._dashboard.set_db_status(True)
        run_async(
            self._db.fetch_stats,
            on_success=self._on_stats,
            on_error=lambda msg: self._dashboard.set_db_status(False, msg),
        )

    def _on_stats(self, stats: DashboardStats) -> None:
        self._dashboard.set_stats(stats)

    def search_cases(self, term: str = "") -> None:
        run_async(
            self._db.fetch_cases,
            term,
            on_success=self._on_cases,
            on_error=lambda msg: print(f"[DB error] {msg}"),
        )

    def _on_cases(self, cases: list[Case]) -> None:
        self._results.set_cases(cases)