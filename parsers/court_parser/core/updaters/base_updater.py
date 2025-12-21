"""
Базовый класс для updater'ов
"""
import asyncio
from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional
from collections import defaultdict
from datetime import datetime

from config.settings import Settings
from database.db_manager import DatabaseManager
from core.region_worker import RegionWorker
from utils.text_processor import TextProcessor
from utils.logger import get_logger
from utils.terminal_ui import init_ui, get_ui, Mode, RegionStatus


class BaseUpdater(ABC):
    """Базовый класс для всех updater'ов"""
    
    MODE: Mode = Mode.PARSE  # Переопределяется в наследниках
    
    def __init__(self, settings: Settings, db_manager: DatabaseManager):
        self.settings = settings
        self.db_manager = db_manager
        self.text_processor = TextProcessor()
        self.logger = get_logger(self.__class__.__name__.lower())
        
        self.common_config = settings.config.get('update_settings', {}).get('common', {})
        self.max_parallel = self.common_config.get('max_parallel_workers', 3)
        self.delay = self.common_config.get('delay_between_requests', 2.0)
        
        # Статистика
        self.stats = {
            'processed': 0,
            'errors': 0,
            'judges_found': 0,
            'events_added': 0,
            'docs_downloaded': 0,
        }
    
    @abstractmethod
    async def get_cases_to_process(self) -> List[str]:
        raise NotImplementedError
    
    @abstractmethod
    async def process_case(self, worker: RegionWorker, case_number: str) -> Dict[str, Any]:
        raise NotImplementedError
    
    @abstractmethod
    def get_config(self) -> Dict[str, Any]:
        raise NotImplementedError
    
    def _group_cases_by_region(self, case_numbers: List[str]) -> Dict[str, List[str]]:
        """Группировка дел по регионам"""
        grouped = defaultdict(list)
        
        for case_number in case_numbers:
            info = self.text_processor.find_region_and_court_by_case_number(
                case_number, self.settings.regions
            )
            if info:
                grouped[info['region_key']].append(case_number)
            else:
                self.logger.warning(f"Не удалось определить регион для {case_number}")
        
        return dict(grouped)
    
    async def run(self) -> Dict[str, Any]:
        """Запуск обработки"""
        # Получаем дела
        case_numbers = await self.get_cases_to_process()
        
        if not case_numbers:
            self.logger.info("Нет дел для обработки")
            return {'total': 0, 'processed': 0, 'skipped': True}
        
        # Группируем по регионам
        grouped = self._group_cases_by_region(case_numbers)
        
        if not grouped:
            self.logger.info("Нет дел после группировки по регионам")
            return {'total': len(case_numbers), 'processed': 0, 'skipped': True}
        
        self.logger.info(f"Дел к обработке: {len(case_numbers)}, регионов: {len(grouped)}")
        
        # Инициализация UI
        regions_display = {
            key: self.settings.get_region(key)['name']
            for key in grouped.keys()
        }
        
        ui = init_ui(self.MODE, regions_display, court_types=[])
        
        # Устанавливаем total для каждого региона
        for region_key, region_cases in grouped.items():
            region = ui.regions.get(region_key)
            if region:
                region.total_cases = len(region_cases)
        
        await ui.start()
        
        # Обработка
        semaphore = asyncio.Semaphore(self.max_parallel)
        
        try:
            tasks = [
                self._process_region_group(region_key, cases, semaphore, ui)
                for region_key, cases in grouped.items()
            ]
            
            await asyncio.gather(*tasks, return_exceptions=True)
            
        finally:
            await ui.finish()
        
        # Финальный отчёт
        self._print_summary()
        
        return {
            'total': len(case_numbers),
            'processed': self.stats['processed'],
            'errors': self.stats['errors'],
        }
    
    async def _process_region_group(
        self, 
        region_key: str, 
        case_numbers: List[str],
        semaphore: asyncio.Semaphore,
        ui
    ) -> Dict[str, Any]:
        """Обработка группы дел одного региона"""
        async with semaphore:
            worker = RegionWorker(self.settings, region_key)
            
            try:
                if not await worker.initialize():
                    self.logger.error(f"Не удалось инициализировать воркер {region_key}")
                    ui.region_error(region_key, "Init failed")
                    return {}
                
                ui.region_start(region_key)
                
                processed = 0
                
                for case_number in case_numbers:
                    try:
                        result = await self.process_case(worker, case_number)
                        processed += 1
                        self.stats['processed'] += 1
                        
                        if result.get('error'):
                            self.stats['errors'] += 1
                        
                        # Mode-specific stats
                        if result.get('judge_found'):
                            self.stats['judges_found'] += 1
                        if result.get('events_added'):
                            self.stats['events_added'] += result['events_added']
                        if result.get('documents_downloaded'):
                            self.stats['docs_downloaded'] += result['documents_downloaded']
                        
                        # Обновляем UI
                        ui.update_progress(
                            region_key,
                            processed=processed,
                            found=self.stats.get('judges_found', 0),
                            events=self.stats.get('events_added', 0),
                            docs=self.stats.get('docs_downloaded', 0)
                        )
                        
                        await asyncio.sleep(self.delay)
                        
                    except Exception as e:
                        self.logger.error(f"Ошибка обработки {case_number}: {e}")
                        self.stats['errors'] += 1
                        self.stats['processed'] += 1
                
                ui.region_done(region_key)
                
            except Exception as e:
                self.logger.error(f"Ошибка в регионе {region_key}: {e}")
                ui.region_error(region_key, str(e))
            
            finally:
                await worker.cleanup()
        
        return {}
    
    def _print_summary(self):
        """Вывод краткой статистики"""
        self.logger.info("-" * 40)
        self.logger.info(f"Обработано: {self.stats['processed']}")
        self.logger.info(f"Ошибок: {self.stats['errors']}")
        
        if self.stats['judges_found'] > 0:
            self.logger.info(f"Судей найдено: {self.stats['judges_found']}")
        if self.stats['events_added'] > 0:
            self.logger.info(f"Событий добавлено: {self.stats['events_added']}")
        if self.stats['docs_downloaded'] > 0:
            self.logger.info(f"Документов скачано: {self.stats['docs_downloaded']}")