"""Загрузка и сохранение config.json парсера.

Подход: читаем ВЕСЬ конфиг как словарь, редактируем только избранные
поля через формы, остальное (regions, retry_settings и т.д.) сохраняем
без изменений.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any


class ConfigError(Exception):
    """Ошибка работы с конфигом."""


class ConfigService:
    def __init__(self, path: Path) -> None:
        self._path = path

    def load(self) -> dict[str, Any]:
        """Загружает весь config.json как словарь."""
        if not self._path.exists():
            raise ConfigError(f"Файл не найден: {self._path}")
        try:
            return json.loads(self._path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise ConfigError(f"Ошибка чтения JSON: {exc}") from exc

    def save(self, config: dict[str, Any]) -> None:
        """
        Сохраняет конфиг обратно в файл.

        Перед записью делает резервную копию config.json.bak,
        чтобы можно было откатиться при ошибке.
        """
        try:
            # Резервная копия
            if self._path.exists():
                backup = self._path.with_suffix(".json.bak")
                backup.write_text(
                    self._path.read_text(encoding="utf-8"),
                    encoding="utf-8",
                )
            # Запись (отступ 2 пробела, кириллица как есть)
            self._path.write_text(
                json.dumps(config, indent=2, ensure_ascii=False),
                encoding="utf-8",
            )
        except OSError as exc:
            raise ConfigError(f"Ошибка сохранения: {exc}") from exc