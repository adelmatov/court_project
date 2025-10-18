# parsers/court_parser/core/session.py
"""
Управление HTTP сессиями с retry
"""

import ssl
import aiohttp
from typing import Optional, Dict, Any

from utils.logger import get_logger
from utils.retry import RetryStrategy, RetryConfig, CircuitBreaker, NonRetriableError


class SessionManager:
    """Менеджер HTTP сессий с автоматическим retry"""
    
    def __init__(self, timeout: int = 30, retry_config: Optional[Dict] = None):
        self.timeout = aiohttp.ClientTimeout(total=timeout)
        self.session: Optional[aiohttp.ClientSession] = None
        self.logger = get_logger('session_manager')
        
        # Retry конфигурация
        self.retry_config = retry_config or {}
        self.circuit_breaker = None
        
        # Инициализация Circuit Breaker
        if 'circuit_breaker' in self.retry_config:
            self.circuit_breaker = CircuitBreaker(self.retry_config['circuit_breaker'])
    
    async def create_session(self) -> aiohttp.ClientSession:
        """Создание новой сессии"""
        if self.session and not self.session.closed:
            await self.session.close()
        
        # SSL контекст без проверки сертификата
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        
        connector = aiohttp.TCPConnector(ssl=ssl_context, limit=10)
        
        self.session = aiohttp.ClientSession(
            timeout=self.timeout,
            connector=connector
        )
        
        self.logger.debug("Создана новая HTTP сессия")
        return self.session
    
    async def get_session(self) -> aiohttp.ClientSession:
        """Получить текущую сессию"""
        if not self.session or self.session.closed:
            return await self.create_session()
        return self.session
    
    async def request(self, method: str, url: str, **kwargs) -> aiohttp.ClientResponse:
        """
        HTTP запрос с автоматическим retry
        
        Args:
            method: HTTP метод (GET, POST, etc)
            url: URL
            **kwargs: параметры для aiohttp
        
        Returns:
            aiohttp.ClientResponse
        
        Raises:
            NonRetriableError: если ошибка не подлежит retry (400, 401, 404, etc)
        """
        session = await self.get_session()
        
        # Получаем retry config
        http_retry_config = self.retry_config.get('http_request', {})
        
        if not http_retry_config:
            # Нет конфига - выполняем без retry
            return await session.request(method, url, **kwargs)
        
        # Retry стратегия
        retry_cfg = RetryConfig(http_retry_config)
        strategy = RetryStrategy(retry_cfg, self.circuit_breaker)
        
        async def _do_request():
            async with session.request(method, url, **kwargs) as response:
                # Проверка на non-retriable статусы
                if response.status in [400, 401, 403, 404]:
                    raise NonRetriableError(f"HTTP {response.status}")
                
                # Проверка на retriable статусы
                if strategy.is_retriable_status(response.status):
                    raise aiohttp.ClientError(f"HTTP {response.status}")
                
                # Успех
                return response
        
        error_context = f"{method} {url}"
        return await strategy.execute_with_retry(_do_request, error_context=error_context)
    
    async def get(self, url: str, **kwargs) -> aiohttp.ClientResponse:
        """GET запрос с retry"""
        return await self.request('GET', url, **kwargs)
    
    async def post(self, url: str, **kwargs) -> aiohttp.ClientResponse:
        """POST запрос с retry"""
        return await self.request('POST', url, **kwargs)
    
    async def close(self):
        """Закрытие сессии"""
        if self.session and not self.session.closed:
            await self.session.close()
            self.logger.debug("Сессия закрыта")
    
    async def __aenter__(self):
        await self.create_session()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()