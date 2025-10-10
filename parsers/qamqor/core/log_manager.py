"""Менеджер логирования."""

import logging
from datetime import datetime
from typing import Dict

from .config import Config


class LogManager:
    """Менеджер логирования с метриками."""
    
    def __init__(self, config: Config, name: str = "qamqor"):
        self.config = config
        self.logger = self._setup_logger(name)
        self.metrics = {
            'start_time': datetime.now(),
            'records_processed': 0,
            'api_requests': 0,
            'api_errors': 0,
            'db_inserts': 0,
            'db_updates': 0,
            'regions_completed': 0
        }
        
    def _setup_logger(self, name: str) -> logging.Logger:
        """Настройка логгера."""
        self.config.LOG_DIR.mkdir(exist_ok=True)
        
        logger = logging.getLogger(name)
        logger.setLevel(getattr(logging, self.config.LOG_LEVEL))
        logger.handlers.clear()
        logger.propagate = False  # ✅ Предотвращаем дублирование
        
        formatter = logging.Formatter(
            '%(asctime)s | %(levelname)-8s | %(message)s',
            datefmt='%H:%M:%S'
        )
        
        # Файловый хендлер
        log_file = self.config.LOG_DIR / f"{name}_{datetime.now():%Y%m%d_%H%M%S}.log"
        fh = logging.FileHandler(log_file, encoding='utf-8')
        fh.setFormatter(formatter)
        fh.setLevel(logging.DEBUG)
        
        # Консольный хендлер
        ch = logging.StreamHandler()
        ch.setFormatter(formatter)
        ch.setLevel(getattr(logging, self.config.LOG_LEVEL))
        
        logger.addHandler(fh)
        logger.addHandler(ch)
        
        return logger
    
    def increment_metric(self, metric_name: str, delta: int = 1):
        """Инкремент метрики."""
        if metric_name in self.metrics:
            self.metrics[metric_name] += delta
        else:
            self.logger.warning(f"⚠️ Неизвестная метрика: {metric_name}")
    
    def get_metrics_summary(self) -> Dict:
        """Получение сводки метрик."""
        elapsed = (datetime.now() - self.metrics['start_time']).total_seconds()
        records_per_sec = (
            self.metrics['records_processed'] / elapsed 
            if elapsed > 0 
            else 0
        )
        
        return {
            **self.metrics,
            'elapsed_seconds': round(elapsed, 2),
            'records_per_second': round(records_per_sec, 2)
        }