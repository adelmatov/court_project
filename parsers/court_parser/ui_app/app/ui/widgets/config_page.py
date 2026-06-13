"""Страница «Config»: редактирование основных параметров config.json."""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from typing import Any

from PySide6.QtCore import Signal
from PySide6.QtWidgets import (
    QCheckBox,
    QFormLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMessageBox,
    QPushButton,
    QScrollArea,
    QSpinBox,
    QVBoxLayout,
    QWidget,
)


class ConfigPage(QWidget):
    # Передаёт обновлённый словарь конфига наверх для сохранения
    save_requested = Signal(dict)

    def __init__(self, config_path: Path) -> None:
        super().__init__()
        self._config_path = config_path
        self._config: dict[str, Any] = {}

        # Прокручиваемая область (полей много)
        outer = QVBoxLayout(self)
        outer.setContentsMargins(24, 24, 24, 24)
        outer.setSpacing(12)

        header = QHBoxLayout()
        title = QLabel("Конфигурация")
        title.setStyleSheet("font-size: 22px; font-weight: 700;")
        header.addWidget(title)
        header.addStretch()
        self.open_file_btn = QPushButton("Открыть config.json в редакторе")
        self.open_file_btn.clicked.connect(self._open_in_editor)
        header.addWidget(self.open_file_btn)
        outer.addLayout(header)

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setStyleSheet("QScrollArea { border: none; }")
        content = QWidget()
        self._form_layout = QVBoxLayout(content)
        self._form_layout.setSpacing(16)
        scroll.setWidget(content)
        outer.addWidget(scroll, 1)

        self._build_forms()

        # сообщение об ошибке валидации
        self.error_label = QLabel()
        self.error_label.setObjectName("StatusError")
        outer.addWidget(self.error_label)

        self.save_btn = QPushButton("💾  Сохранить")
        self.save_btn.clicked.connect(self._on_save)
        outer.addWidget(self.save_btn)

    def _build_forms(self) -> None:
        # === Авторизация ===
        auth_box = QGroupBox("Авторизация")
        auth_form = QFormLayout(auth_box)
        self.auth_login = QLineEdit()
        self.auth_password = QLineEdit()
        self.auth_password.setEchoMode(QLineEdit.Password)
        self.auth_user_name = QLineEdit()
        auth_form.addRow("Логин (ИИН)", self.auth_login)
        auth_form.addRow("Пароль", self.auth_password)
        auth_form.addRow("Имя пользователя", self.auth_user_name)
        self._form_layout.addWidget(auth_box)

        # === База данных ===
        db_box = QGroupBox("База данных (PostgreSQL)")
        db_form = QFormLayout(db_box)
        self.db_host = QLineEdit()
        self.db_port = QSpinBox()
        self.db_port.setRange(1, 65535)
        self.db_name = QLineEdit()
        self.db_user = QLineEdit()
        self.db_password = QLineEdit()
        self.db_password.setEchoMode(QLineEdit.Password)
        db_form.addRow("Host", self.db_host)
        db_form.addRow("Port", self.db_port)
        db_form.addRow("Database (dbname)", self.db_name)
        db_form.addRow("User", self.db_user)
        db_form.addRow("Password", self.db_password)
        self._form_layout.addWidget(db_box)

        # === Настройки парсинга ===
        ps_box = QGroupBox("Настройки парсинга")
        ps_form = QFormLayout(ps_box)
        self.ps_start_from = QSpinBox()
        self.ps_start_from.setRange(1, 999999)
        self.ps_max_number = QSpinBox()
        self.ps_max_number.setRange(1, 999999)
        self.ps_max_empty = QSpinBox()
        self.ps_max_empty.setRange(1, 1000)
        self.ps_max_parallel = QSpinBox()
        self.ps_max_parallel.setRange(1, 20)
        self.ps_delay = QSpinBox()
        self.ps_delay.setRange(0, 600)
        self.ps_max_gaps = QSpinBox()
        self.ps_max_gaps.setRange(1, 100000)
        ps_form.addRow("Начать с номера (start_from)", self.ps_start_from)
        ps_form.addRow("Макс. номер (max_number)", self.ps_max_number)
        ps_form.addRow("Макс. пустых подряд (max_consecutive_empty)", self.ps_max_empty)
        ps_form.addRow("Параллельных регионов (max_parallel_regions)", self.ps_max_parallel)
        ps_form.addRow("Задержка между запросами, сек (delay_between_requests)", self.ps_delay)
        ps_form.addRow("Макс. пропусков за сессию (max_gaps_per_session)", self.ps_max_gaps)
        self._form_layout.addWidget(ps_box)

        # === Документы (docs) ===
        docs_box = QGroupBox("Скачивание документов")
        docs_form = QFormLayout(docs_box)
        self.docs_enabled = QCheckBox("Включено")
        self.docs_storage = QLineEdit()
        self.docs_interval = QSpinBox()
        self.docs_interval.setRange(0, 365)
        self.docs_max_attempts = QSpinBox()
        self.docs_max_attempts.setRange(1, 100)
        docs_form.addRow("", self.docs_enabled)
        docs_form.addRow("Папка хранения (storage_dir)", self.docs_storage)
        docs_form.addRow("Интервал проверки, дней (check_interval_days)", self.docs_interval)
        docs_form.addRow("Макс. попыток (documents_max_attempts)", self.docs_max_attempts)
        self._form_layout.addWidget(docs_box)

        self._form_layout.addStretch()

    # ---------- загрузка значений в формы ----------

    def load_config(self, config: dict[str, Any]) -> None:
        """Заполняет формы значениями из словаря конфига."""
        self._config = config
        self.error_label.clear()

        auth = config.get("auth", {})
        self.auth_login.setText(str(auth.get("login", "")))
        self.auth_password.setText(str(auth.get("password", "")))
        self.auth_user_name.setText(str(auth.get("user_name", "")))

        db = config.get("database", {})
        self.db_host.setText(str(db.get("host", "localhost")))
        self.db_port.setValue(int(db.get("port", 5432)))
        self.db_name.setText(str(db.get("dbname", "")))
        self.db_user.setText(str(db.get("user", "")))
        self.db_password.setText(str(db.get("password", "")))

        ps = config.get("parsing_settings", {})
        self.ps_start_from.setValue(int(ps.get("start_from", 1)))
        self.ps_max_number.setValue(int(ps.get("max_number", 9999)))
        self.ps_max_empty.setValue(int(ps.get("max_consecutive_empty", 5)))
        self.ps_max_parallel.setValue(int(ps.get("max_parallel_regions", 3)))
        self.ps_delay.setValue(int(ps.get("delay_between_requests", 0)))
        self.ps_max_gaps.setValue(int(ps.get("max_gaps_per_session", 200)))

        docs = config.get("update_settings", {}).get("docs", {})
        self.docs_enabled.setChecked(bool(docs.get("enabled", True)))
        self.docs_storage.setText(str(docs.get("storage_dir", "./docs")))
        self.docs_interval.setValue(int(docs.get("check_interval_days", 5)))
        self.docs_max_attempts.setValue(int(docs.get("documents_max_attempts", 5)))

    # ---------- сбор значений из форм ----------

    def _on_save(self) -> None:
        self.error_label.clear()

        # Простая валидация
        if not self.auth_login.text().strip():
            self.error_label.setText("Логин не может быть пустым")
            return
        if not self.db_name.text().strip():
            self.error_label.setText("Имя базы данных не может быть пустым")
            return

        # Берём исходный конфиг и обновляем только нужные поля.
        # Так regions / retry_settings и прочее НЕ теряются.
        cfg = self._config

        cfg.setdefault("auth", {})
        cfg["auth"]["login"] = self.auth_login.text().strip()
        cfg["auth"]["password"] = self.auth_password.text()
        cfg["auth"]["user_name"] = self.auth_user_name.text().strip()

        cfg.setdefault("database", {})
        cfg["database"]["host"] = self.db_host.text().strip()
        cfg["database"]["port"] = self.db_port.value()
        cfg["database"]["dbname"] = self.db_name.text().strip()
        cfg["database"]["user"] = self.db_user.text().strip()
        cfg["database"]["password"] = self.db_password.text()

        cfg.setdefault("parsing_settings", {})
        cfg["parsing_settings"]["start_from"] = self.ps_start_from.value()
        cfg["parsing_settings"]["max_number"] = self.ps_max_number.value()
        cfg["parsing_settings"]["max_consecutive_empty"] = self.ps_max_empty.value()
        cfg["parsing_settings"]["max_parallel_regions"] = self.ps_max_parallel.value()
        cfg["parsing_settings"]["delay_between_requests"] = self.ps_delay.value()
        cfg["parsing_settings"]["max_gaps_per_session"] = self.ps_max_gaps.value()

        cfg.setdefault("update_settings", {}).setdefault("docs", {})
        cfg["update_settings"]["docs"]["enabled"] = self.docs_enabled.isChecked()
        cfg["update_settings"]["docs"]["storage_dir"] = self.docs_storage.text().strip()
        cfg["update_settings"]["docs"]["check_interval_days"] = self.docs_interval.value()
        cfg["update_settings"]["docs"]["documents_max_attempts"] = self.docs_max_attempts.value()

        self.save_requested.emit(cfg)

    def _open_in_editor(self) -> None:
        """Открывает config.json в системном редакторе по умолчанию."""
        try:
            if sys.platform == "win32":
                import os
                os.startfile(str(self._config_path))  # type: ignore[attr-defined]
            elif sys.platform == "darwin":
                subprocess.run(["open", str(self._config_path)], check=False)
            else:
                subprocess.run(["xdg-open", str(self._config_path)], check=False)
        except Exception as exc:
            QMessageBox.warning(self, "Ошибка", f"Не удалось открыть файл:\n{exc}")