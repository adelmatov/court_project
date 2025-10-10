"""Клиент для проверки доступности сервиса."""

import asyncio
import logging
from typing import Optional

from playwright.async_api import Page, Response

from .config import Config
from .api_validator import APIValidator


class WebClient:
    """Клиент для проверки доступности сервиса."""
    
    def __init__(self, config: Config, logger: logging.Logger, validator: APIValidator):
        self.config = config
        self.logger = logger
        self.validator = validator
    
    async def check_api_health_with_page(self, page: Page) -> bool:
        """
        Проверка API с использованием существующей вкладки.
        
        Args:
            page: Вкладка браузера
            
        Returns:
            True если API работает корректно
        """
        try:
            self.logger.info("🔍 Проверка доступности API...")
            
            test_number = self.config.TEST_NUMBER
            response_future: asyncio.Future[Response] = asyncio.Future()
            
            def handle_response(response: Response):
                """Синхронный обработчик ответа."""
                if self.config.API_ENDPOINT in response.url:
                    if not response_future.done():
                        response_future.set_result(response)
            
            page.on("response", handle_response)
            
            try:
                # Заполняем поле
                await page.fill(
                    'input[placeholder="Тексеру/ тіркеу нөмірі"]', 
                    test_number
                )
                
                # Кликаем
                await page.click("button.btn.btn-primary:has-text('Іздеу')")
                
                # Ждем Response объект
                response = await asyncio.wait_for(
                    response_future,
                    timeout=self.config.RESPONSE_TIMEOUT
                )
                
                # Парсим JSON вне обработчика
                response_data = await response.json()
                
                if not response_data:
                    self.logger.error("❌ API не ответил")
                    return False
                
                # Валидация ответа
                is_valid, error_msg = self.validator.validate_response(
                    response_data, 
                    context="Проверка API"
                )
                
                if not is_valid:
                    self.logger.error(f"❌ API вернул некорректный ответ: {error_msg}")
                    self.logger.debug(f"   Ответ: {response_data}")
                    return False
                
                self.logger.info("✅ API работает корректно")
                return True
                
            finally:
                # Удаляем обработчик
                try:
                    page.remove_listener("response", handle_response)
                except Exception:
                    pass
                
        except asyncio.TimeoutError:
            self.logger.error("❌ Таймаут проверки API")
            return False
        except Exception as e:
            self.logger.error(f"❌ Ошибка проверки API: {e}", exc_info=True)
            return False