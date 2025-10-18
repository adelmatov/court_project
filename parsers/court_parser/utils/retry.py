"""
Гибкая система retry с поддержкой различных стратегий
"""

import asyncio
import random
from typing import Callable, Any, Optional, List, Type
from functools import wraps
from datetime import datetime, timedelta

import aiohttp

from utils.logger import get_logger


class RetryableError(Exception):
    """Ошибка, которую можно повторить"""
    pass


class NonRetriableError(Exception):
    """Ошибка, которую нельзя повторить"""
    pass


class CircuitBreakerOpenError(Exception):
    """Circuit Breaker в состоянии OPEN"""
    pass


class RetryConfig:
    """Конфигурация retry"""
    
    def __init__(self, config: dict):
        self.max_attempts = config.get('max_attempts', 3)
        self.initial_delay = config.get('initial_delay', 1.0)
        self.backoff_multiplier = config.get('backoff_multiplier', 2.0)
        self.max_delay = config.get('max_delay', 30.0)
        self.jitter = config.get('jitter', True)
        self.backoff = config.get('backoff', 'exponential')  # exponential или linear
        
        # Для HTTP retry
        self.retriable_status_codes = config.get('retriable_status_codes', [500, 502, 503, 504])
        self.retriable_exceptions = config.get('retriable_exceptions', [])


class CircuitBreaker:
    """
    Circuit Breaker паттерн
    
    Состояния:
    - CLOSED: нормальная работа
    - OPEN: сервер недоступен, не пытаемся
    - HALF_OPEN: проверка восстановления
    """
    
    def __init__(self, config: dict):
        self.enabled = config.get('enabled', True)
        self.failure_threshold = config.get('failure_threshold', 20)
        self.recovery_timeout = config.get('recovery_timeout', 300)  # секунд
        self.half_open_max_attempts = config.get('half_open_max_attempts', 3)
        
        self.state = 'CLOSED'
        self.failure_count = 0
        self.last_failure_time: Optional[datetime] = None
        self.half_open_success_count = 0
        
        self.logger = get_logger('circuit_breaker')
    
    def record_success(self):
        """Запись успешного запроса"""
        if not self.enabled:
            return
        
        if self.state == 'HALF_OPEN':
            self.half_open_success_count += 1
            
            if self.half_open_success_count >= self.half_open_max_attempts:
                self.logger.info("🎉 Circuit Breaker: HALF_OPEN → CLOSED (сервер восстановлен)")
                self.state = 'CLOSED'
                self.failure_count = 0
                self.half_open_success_count = 0
        
        elif self.state == 'CLOSED':
            # Сбрасываем счетчик при успехе
            self.failure_count = max(0, self.failure_count - 1)
    
    def record_failure(self):
        """Запись неудачного запроса"""
        if not self.enabled:
            return
        
        self.failure_count += 1
        self.last_failure_time = datetime.now()
        
        if self.state == 'CLOSED' and self.failure_count >= self.failure_threshold:
            self.logger.critical(
                f"🚨 Circuit Breaker: CLOSED → OPEN "
                f"({self.failure_count} ошибок подряд)"
            )
            self.state = 'OPEN'
        
        elif self.state == 'HALF_OPEN':
            self.logger.warning("Circuit Breaker: HALF_OPEN → OPEN (проверка не пройдена)")
            self.state = 'OPEN'
            self.half_open_success_count = 0
    
    def can_execute(self) -> bool:
        """Можно ли выполнить запрос"""
        if not self.enabled:
            return True
        
        if self.state == 'CLOSED':
            return True
        
        if self.state == 'HALF_OPEN':
            return True
        
        # state == 'OPEN'
        if self.last_failure_time:
            elapsed = (datetime.now() - self.last_failure_time).total_seconds()
            
            if elapsed >= self.recovery_timeout:
                self.logger.info(
                    f"Circuit Breaker: OPEN → HALF_OPEN "
                    f"(пауза {self.recovery_timeout} сек прошла)"
                )
                self.state = 'HALF_OPEN'
                self.half_open_success_count = 0
                return True
        
        return False
    
    def get_wait_time(self) -> Optional[float]:
        """Сколько ждать до следующей попытки (если OPEN)"""
        if self.state != 'OPEN' or not self.last_failure_time:
            return None
        
        elapsed = (datetime.now() - self.last_failure_time).total_seconds()
        remaining = self.recovery_timeout - elapsed
        
        return max(0, remaining)


class RetryStrategy:
    """Стратегия retry с гибкими настройками"""
    
    def __init__(self, config: RetryConfig, circuit_breaker: Optional[CircuitBreaker] = None):
        self.config = config
        self.circuit_breaker = circuit_breaker
        self.logger = get_logger('retry_strategy')
    
    def calculate_delay(self, attempt: int) -> float:
        """Расчет задержки перед следующей попыткой"""
        if self.config.backoff == 'linear':
            delay = self.config.initial_delay
        else:  # exponential
            delay = self.config.initial_delay * (self.config.backoff_multiplier ** (attempt - 1))
        
        # Ограничение максимальной задержки
        delay = min(delay, self.config.max_delay)
        
        # Jitter (случайность ±20%)
        if self.config.jitter:
            jitter_range = delay * 0.2
            delay += random.uniform(-jitter_range, jitter_range)
        
        return max(0, delay)
    
    def is_retriable_exception(self, exc: Exception) -> bool:
        """Проверка что исключение можно повторить"""
        exc_name = type(exc).__name__
        
        # Проверка по списку из конфига
        if exc_name in self.config.retriable_exceptions:
            return True
        
        # Стандартные retriable исключения
        retriable_types = (
            asyncio.TimeoutError,
            aiohttp.ClientError,
            aiohttp.ServerDisconnectedError,
            aiohttp.ClientConnectionError,
            ConnectionError,
            RetryableError
        )
        
        return isinstance(exc, retriable_types)
    
    def is_retriable_status(self, status: int) -> bool:
        """Проверка что HTTP статус можно повторить"""
        return status in self.config.retriable_status_codes
    
    async def execute_with_retry(self, 
                                func: Callable,
                                *args,
                                error_context: str = "",
                                **kwargs) -> Any:
        """
        Выполнение функции с retry
        
        Args:
            func: асинхронная функция для выполнения
            error_context: контекст для логирования (например, "HTTP GET /api")
            *args, **kwargs: параметры для func
        
        Raises:
            NonRetriableError: если ошибка не подлежит retry
            CircuitBreakerOpenError: если Circuit Breaker в состоянии OPEN
        """
        last_exception = None
        
        for attempt in range(1, self.config.max_attempts + 1):
            # Проверка Circuit Breaker
            if self.circuit_breaker and not self.circuit_breaker.can_execute():
                wait_time = self.circuit_breaker.get_wait_time()
                
                if wait_time and wait_time > 0:
                    self.logger.warning(
                        f"⏸️ Circuit Breaker OPEN, ждем {wait_time:.0f} сек..."
                    )
                    await asyncio.sleep(wait_time)
                    
                    # Повторная проверка после ожидания
                    if not self.circuit_breaker.can_execute():
                        raise CircuitBreakerOpenError(
                            f"Circuit Breaker в состоянии OPEN, сервер недоступен"
                        )
                else:
                    raise CircuitBreakerOpenError(
                        f"Circuit Breaker в состоянии OPEN"
                    )
            
            try:
                # Попытка выполнения
                result = await func(*args, **kwargs)
                
                # Успех
                if self.circuit_breaker:
                    self.circuit_breaker.record_success()
                
                if attempt > 1:
                    self.logger.info(f"✅ Успех на попытке {attempt}")
                
                return result
            
            except NonRetriableError:
                # Не повторяемая ошибка - прокидываем наверх
                if self.circuit_breaker:
                    self.circuit_breaker.record_success()  # Не считаем как failure
                raise
            
            except Exception as exc:
                last_exception = exc
                
                # Проверяем можно ли повторить
                if not self.is_retriable_exception(exc):
                    self.logger.error(f"❌ Non-retriable error: {type(exc).__name__}: {exc}")
                    raise NonRetriableError(f"{type(exc).__name__}: {exc}") from exc
                
                # Записываем failure
                if self.circuit_breaker:
                    self.circuit_breaker.record_failure()
                
                # Если это последняя попытка - выбрасываем ошибку
                if attempt >= self.config.max_attempts:
                    self.logger.error(
                        f"❌ Все {self.config.max_attempts} попытки исчерпаны"
                    )
                    raise RetryableError(
                        f"Не удалось выполнить после {self.config.max_attempts} попыток: {exc}"
                    ) from exc
                
                # Логирование и задержка перед следующей попыткой
                delay = self.calculate_delay(attempt)
                
                self.logger.warning(
                    f"⚠️ [{error_context}] {type(exc).__name__} "
                    f"(попытка {attempt}/{self.config.max_attempts}), "
                    f"повтор через {delay:.1f} сек"
                )
                
                await asyncio.sleep(delay)
        
        # Не должно сюда дойти, но на всякий случай
        raise RetryableError(f"Unexpected retry exhaustion") from last_exception