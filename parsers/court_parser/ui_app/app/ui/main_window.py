"""Главное окно: боковое меню + страницы + контроллеры."""
from __future__ import annotations

from PySide6.QtWidgets import (
    QButtonGroup,
    QFrame,
    QHBoxLayout,
    QLabel,
    QMainWindow,
    QPushButton,
    QStackedWidget,
    QVBoxLayout,
    QWidget,
)
from PySide6.QtWidgets import QMessageBox

from app.controllers.dashboard_controller import DashboardController
from app.services.db_service import DbService
from app.ui.widgets.dashboard_page import DashboardPage
from app.ui.widgets.results_page import ResultsPage
from app.services.config_service import ConfigError, ConfigService
from app.ui.widgets.config_page import ConfigPage
from app.core import paths
from app.controllers.run_controller import RunController
from app.services.log_service import LogService
from app.services.process_service import ProcessService
from app.ui.widgets.monitor_page import MonitorPage
from app.ui.widgets.run_page import RunPage


class MainWindow(QMainWindow):
    PAGES = ["Dashboard", "Run", "Config", "Monitor", "Results"]

    def __init__(self) -> None:
        super().__init__()
        self.setWindowTitle("Court Parser Control Center")
        self.resize(1180, 760)

        # --- сервисы ---
        self._process = ProcessService(paths.PARSER_ENTRY)
        self._log = LogService()

        # --- страницы ---
        self.run_page = RunPage()
        self.monitor_page = MonitorPage()
        # config
        self._config_service = ConfigService(paths.CONFIG_PATH)
        self.config_page = ConfigPage(paths.CONFIG_PATH)
        self.config_page.save_requested.connect(self._on_config_saved)
        self._load_config()
        # dashboard + results
        db_config = {}
        try:
            db_config = self._config_service.load().get("database", {})
        except ConfigError:
            pass
        self._db_service = DbService(db_config)
        self.dashboard_page = DashboardPage()
        self.results_page = ResultsPage()
        self._dashboard_ctrl = DashboardController(
            self._db_service, self.dashboard_page, self.results_page
        )

        # --- контроллер ---
        self._run_ctrl = RunController(
            self._process, self._log, self.run_page, self.monitor_page
        )

        self._build_ui()

    def _build_ui(self) -> None:
        central = QWidget()
        layout = QHBoxLayout(central)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)

        # боковое меню
        sidebar = QFrame()
        sidebar.setObjectName("Sidebar")
        sidebar.setFixedWidth(220)
        side_layout = QVBoxLayout(sidebar)
        side_layout.setContentsMargins(10, 10, 10, 10)
        side_layout.setSpacing(6)

        logo = QLabel("⚖  Court Parser")
        logo.setObjectName("SidebarLogo")
        side_layout.addWidget(logo)

        self._button_group = QButtonGroup(self)
        self._button_group.setExclusive(True)
        for index, name in enumerate(self.PAGES):
            btn = QPushButton(name)
            btn.setObjectName("SidebarButton")
            btn.setCheckable(True)
            btn.clicked.connect(lambda _=False, i=index: self.stack.setCurrentIndex(i))
            self._button_group.addButton(btn, index)
            side_layout.addWidget(btn)
        side_layout.addStretch()

        # страницы (Dashboard/Config/Results пока заглушки)
        self.stack = QStackedWidget()
        self.stack.addWidget(self.dashboard_page)              # 0
        self.stack.addWidget(self.run_page)                    # 1
        self.stack.addWidget(self.config_page)                 # 2
        self.stack.addWidget(self.monitor_page)                # 3
        self.stack.addWidget(self.results_page)                # 4

        layout.addWidget(sidebar)
        layout.addWidget(self.stack, 1)
        self.setCentralWidget(central)

        self._button_group.button(0).setChecked(True)
        # Загружаем данные дашборда и таблицы при старте
        self._dashboard_ctrl.refresh()
        self._dashboard_ctrl.search_cases("")

    @staticmethod
    def _placeholder(name: str) -> QLabel:
        label = QLabel(f"Страница «{name}» — скоро здесь будет контент")
        label.setStyleSheet("font-size: 18px; color: #9aa0ac;")
        label.setContentsMargins(40, 40, 40, 40)
        return label

    def _load_config(self) -> None:
        try:
            config = self._config_service.load()
            self.config_page.load_config(config)
        except ConfigError as exc:
            QMessageBox.warning(self, "Конфиг", str(exc))

    def _on_config_saved(self, config: dict) -> None:
        try:
            self._config_service.save(config)
        except ConfigError as exc:
            QMessageBox.critical(self, "Ошибка сохранения", str(exc))
            return
        QMessageBox.information(
            self, "Конфиг",
            "Сохранено.\nРезервная копия: config.json.bak",
        )
    
    def closeEvent(self, event) -> None:
        # При закрытии окна корректно гасим процесс и слежение за логом
        self._process.stop()
        self._log.stop()
        super().closeEvent(event)