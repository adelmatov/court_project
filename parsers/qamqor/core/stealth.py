"""
Stealth utilities для обхода детекции автоматизации браузера.
"""

import asyncio
import logging
from contextlib import asynccontextmanager
from typing import List, Set

from playwright.async_api import BrowserContext, Page, Route

from .config import Config


async def apply_stealth(page: Page) -> bool:
    """
    Маскировка браузера под реального пользователя.
    
    Удаляет признаки автоматизации (webdriver флаг, добавляет chrome объект,
    настраивает permissions, plugins и languages).
    
    Args:
        page: Страница Playwright для применения stealth
        
    Returns:
        True при успешном применении, False при ошибке
    """
    try:
        await page.add_init_script("""
            // Удаляем webdriver
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            });
            
            // Chrome объект
            window.navigator.chrome = {
                runtime: {},
                loadTimes: function() {},
                csi: function() {}
            };
            
            // Permissions
            const originalQuery = window.navigator.permissions.query;
            window.navigator.permissions.query = (parameters) => (
                parameters.name === 'notifications' ?
                    Promise.resolve({ state: Notification.permission }) :
                    originalQuery(parameters)
            );
            
            // Plugins
            Object.defineProperty(navigator, 'plugins', {
                get: () => [1, 2, 3, 4, 5]
            });
            
            // Languages
            Object.defineProperty(navigator, 'languages', {
                get: () => ['ru-RU', 'ru', 'en-US', 'en']
            });
        """)
        return True
    except Exception:
        return False


class StealthTabManager:
    """
    Менеджер пула вкладок браузера с автоматическим применением stealth.
    
    Управляет пулом вкладок с блокировкой ненужных ресурсов (изображения,
    стили, шрифты) и автоматическим восстановлением при сбоях.
    """
    
    # Типы ресурсов для блокировки
    BLOCKED_RESOURCE_TYPES: Set[str] = {"image", "stylesheet", "font", "media"}
    
    def __init__(
        self,
        context: BrowserContext,
        config: Config,
        logger: logging.Logger
    ):
        """
        Инициализация менеджера вкладок.
        
        Args:
            context: Контекст браузера Playwright
            config: Конфигурация парсера
            logger: Логгер для вывода сообщений
        """
        self.context = context
        self.config = config
        self.logger = logger
        self.available_tabs: asyncio.Queue[Page] = asyncio.Queue()
        self.all_tabs: List[Page] = []
        self._lock = asyncio.Lock()
        self._closed = False
    
    async def initialize(self) -> None:
        """
        Инициализация пула вкладок.
        
        Создает указанное в конфигурации количество вкладок,
        применяет stealth и блокировку ресурсов к каждой.
        
        Raises:
            Exception: При ошибке создания вкладок
        """
        self.logger.debug(
            "🔧 Инициализация %d вкладок...",
            self.config.MAX_CONCURRENT_TABS
        )
        
        for i in range(self.config.MAX_CONCURRENT_TABS):
            try:
                page = await self._create_configured_page()
                await apply_stealth(page)
                
                self.all_tabs.append(page)
                await self.available_tabs.put(page)
                
                self.logger.debug("✅ Вкладка %d (stealth) создана", i + 1)
            except Exception as e:
                self.logger.error("❌ Ошибка создания вкладки %d: %s", i + 1, e)
                await self.close_all()
                raise
        
        self.logger.debug("✅ Пул вкладок (stealth) инициализирован")
    
    async def _create_configured_page(self) -> Page:
        """
        Создание настроенной вкладки с блокировкой ресурсов.
        
        Returns:
            Настроенная вкладка Playwright
        """
        page = await self.context.new_page()
        
        async def route_handler(route: Route) -> None:
            """Обработчик маршрутов для блокировки ресурсов."""
            try:
                if route.request.resource_type in self.BLOCKED_RESOURCE_TYPES:
                    await route.abort()
                else:
                    await route.continue_()
            except Exception:
                try:
                    await route.abort()
                except Exception:
                    pass
        
        await page.route("**/*", route_handler)
        return page
    
    @asynccontextmanager
    async def get_tab(self):
        """
        Получение вкладки из пула с автоматическим восстановлением.
        
        Контекстный менеджер для безопасного получения и возврата
        вкладки в пул. При закрытой вкладке автоматически создает новую.
        
        Yields:
            Page: Вкладка браузера
            
        Raises:
            RuntimeError: Если TabManager уже закрыт
        """
        if self._closed:
            raise RuntimeError("TabManager закрыт")
        
        page = await self.available_tabs.get()
        
        # Проверка и восстановление закрытой вкладки
        if page.is_closed():
            async with self._lock:
                # Double-check под lock'ом
                if page.is_closed():
                    self.logger.warning("⚠️ Вкладка закрыта, создаем новую")
                    try:
                        new_page = await self._create_configured_page()
                        await apply_stealth(new_page)
                        
                        # Безопасное обновление списка
                        try:
                            idx = self.all_tabs.index(page)
                            self.all_tabs[idx] = new_page
                        except ValueError:
                            self.all_tabs.append(new_page)
                        
                        page = new_page
                    except Exception as e:
                        self.logger.error("❌ Не удалось создать вкладку: %s", e)
                        await self.available_tabs.put(page)
                        raise
        
        try:
            yield page
        except Exception as e:
            self.logger.error("❌ Ошибка работы с вкладкой: %s", e)
            
            # Восстановление при ошибке
            async with self._lock:
                try:
                    if not page.is_closed():
                        await page.close()
                except Exception:
                    pass
                
                try:
                    page = await self._create_configured_page()
                    await apply_stealth(page)
                    self.logger.info("✅ Вкладка восстановлена после ошибки")
                except Exception as create_error:
                    self.logger.critical(
                        "🚨 Не удалось восстановить вкладку: %s",
                        create_error
                    )
                    raise
            raise
        finally:
            # Возврат вкладки в пул
            if not page.is_closed():
                await self.available_tabs.put(page)
            else:
                # Асинхронное восстановление
                asyncio.create_task(self._restore_pool_tab(page))
    
    async def _restore_pool_tab(self, closed_page: Page) -> None:
        """
        Асинхронное восстановление вкладки в пуле.
        
        Args:
            closed_page: Закрытая вкладка для замены
        """
        async with self._lock:
            try:
                new_page = await self._create_configured_page()
                await apply_stealth(new_page)
                await self.available_tabs.put(new_page)
                
                # Обновление списка
                try:
                    idx = self.all_tabs.index(closed_page)
                    self.all_tabs[idx] = new_page
                except ValueError:
                    self.all_tabs.append(new_page)
            except Exception as e:
                self.logger.critical("🚨 Не удалось восстановить пул: %s", e)
    
    async def close_all(self) -> None:
        """Закрытие всех вкладок в пуле."""
        self._closed = True
        
        async with self._lock:
            for i, page in enumerate(self.all_tabs):
                try:
                    if not page.is_closed():
                        await page.close()
                except Exception:
                    pass