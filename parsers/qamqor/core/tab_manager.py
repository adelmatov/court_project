"""Менеджер пула вкладок браузера."""

import asyncio
import logging
from contextlib import asynccontextmanager
from typing import List

from playwright.async_api import BrowserContext, Page

from .config import Config


class TabManager:
    """Менеджер пула вкладок браузера."""
    
    def __init__(self, context: BrowserContext, config: Config, logger: logging.Logger):
        self.context = context
        self.config = config
        self.logger = logger
        self.available_tabs: asyncio.Queue[Page] = asyncio.Queue()
        self.all_tabs: List[Page] = []
        self._lock = asyncio.Lock()
        self._closed = False
        
    async def initialize(self):
        """Инициализация пула вкладок."""
        self.logger.debug(f"🔧 Инициализация {self.config.MAX_CONCURRENT_TABS} вкладок...")
        
        for i in range(self.config.MAX_CONCURRENT_TABS):
            try:
                page = await self._create_configured_page()
                self.all_tabs.append(page)
                await self.available_tabs.put(page)
                self.logger.debug(f"✅ Вкладка {i+1}/{self.config.MAX_CONCURRENT_TABS} создана")
            except Exception as e:
                self.logger.error(f"❌ Не удалось создать вкладку {i+1}: {e}")
                # Закрываем уже созданные вкладки
                await self.close_all()
                raise
        
        self.logger.debug("✅ Пул вкладок инициализирован")
    
    async def _create_configured_page(self) -> Page:
        """
        Создание настроенной вкладки с блокировкой лишних ресурсов.
        
        Returns:
            Настроенная вкладка
        """
        page = await self.context.new_page()
        
        # ✅ Оптимизация: блокировка ненужных ресурсов
        blocked_types = {"image", "stylesheet", "font", "media"}
        
        async def route_handler(route):
            """Асинхронный обработчик маршрутов."""
            try:
                if route.request.resource_type in blocked_types:
                    await route.abort()
                else:
                    await route.continue_()
            except Exception as e:
                self.logger.debug(f"Ошибка обработки маршрута: {e}")
                try:
                    await route.abort()
                except Exception:
                    pass
        
        await page.route("**/*", route_handler)
        
        return page
    
    @asynccontextmanager
    async def get_tab(self):
        """Получение вкладки с безопасным восстановлением."""
        if self._closed:
            raise RuntimeError("TabManager уже закрыт")
        
        page = await self.available_tabs.get()
        
        # Проверка и восстановление под одним lock'ом
        if page.is_closed():
            async with self._lock:
                # ✅ Double-check под lock'ом
                if page.is_closed():
                    self.logger.warning("⚠️ Вкладка закрыта, создаем новую")
                    try:
                        new_page = await self._create_configured_page()
                        
                        # ✅ Безопасное обновление списка
                        try:
                            idx = self.all_tabs.index(page)
                            self.all_tabs[idx] = new_page
                        except ValueError:
                            # Старая вкладка не найдена, добавляем новую
                            self.all_tabs.append(new_page)
                        
                        page = new_page
                        
                    except Exception as e:
                        self.logger.error(f"❌ Не удалось создать вкладку: {e}")
                        # Возвращаем что-то в пул, чтобы не заблокировать
                        await self.available_tabs.put(page)
                        raise
        
        try:
            yield page
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка работы с вкладкой: {e}")
            
            # При ошибке восстанавливаем вкладку
            async with self._lock:
                try:
                    if not page.is_closed():
                        await page.close()
                except Exception:
                    pass
                
                # Создаем замену
                try:
                    page = await self._create_configured_page()
                    self.logger.info("✅ Вкладка восстановлена после ошибки")
                except Exception as create_error:
                    self.logger.critical(f"🚨 Не удалось восстановить вкладку: {create_error}")
                    try:
                        page = await self.context.new_page()
                    except Exception:
                        raise RuntimeError("Не удалось создать вкладку") from create_error
            
            raise
            
        finally:
            # Возвращаем вкладку в пул
            if not page.is_closed():
                await self.available_tabs.put(page)
            else:
                # ✅ Асинхронное восстановление пула
                asyncio.create_task(self._restore_pool_tab(page))

    async def _restore_pool_tab(self, closed_page: Page):
        """Асинхронное восстановление вкладки в пуле."""
        async with self._lock:
            try:
                new_page = await self._create_configured_page()
                await self.available_tabs.put(new_page)
                
                # Обновляем список
                try:
                    idx = self.all_tabs.index(closed_page)
                    self.all_tabs[idx] = new_page
                except ValueError:
                    self.all_tabs.append(new_page)
                    
            except Exception as e:
                self.logger.critical(f"🚨 Не удалось восстановить пул вкладок: {e}")
    
    async def close_all(self):
        """Закрытие всех вкладок."""
        self._closed = True
        
        self.logger.debug("🔧 Закрытие всех вкладок...")
        
        async with self._lock:
            for i, page in enumerate(self.all_tabs):
                try:
                    if not page.is_closed():
                        await page.close()
                        self.logger.debug(f"✅ Вкладка {i+1} закрыта")
                except Exception as e:
                    self.logger.debug(f"⚠️ Ошибка закрытия вкладки {i+1}: {e}")
        
        self.logger.debug("✅ Все вкладки закрыты")