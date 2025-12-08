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
from utils.update_progress import UpdateProgressDisplay, RegionStatus
from utils.update_reporter import UpdateReporter


class BaseUpdater(ABC):
    """Базовый класс для всех updater'ов"""
    
    MODE = 'unknown'  # Переопределяется в наследниках
    
    def __init__(self, settings: Settings, db_manager: DatabaseManager):
        self.settings = settings
        self.db_manager = db_manager
        self.text_processor = TextProcessor()
        self.logger = get_logger(self.__class__.__name__.lower())
        
        self.common_config = settings.config.get('update_settings', {}).get('common', {})
        self.max_parallel = self.common_config.get('max_parallel_workers', 3)
        self.delay = self.common_config.get('delay_between_requests', 2.0)
        
        self.progress: Optional[UpdateProgressDisplay] = None
        self.reporter: Optional[UpdateReporter] = None
    
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
        """Группировка дел по регионам с учётом parsing_settings"""
        grouped = defaultdict(list)
        
        # Получить ограничения из parsing_settings
        target_regions = self.settings.parsing_settings.get('target_regions') or []
        target_courts = self.settings.parsing_settings.get('court_types') or []
        
        for case_number in case_numbers:
            info = self.text_processor.find_region_and_court_by_case_number(
                case_number, self.settings.regions
            )
            if not info:
                self.logger.warning(f"Не удалось определить регион для {case_number}")
                continue
            
            region_key = info['region_key']
            court_key = info.get('court_key', '')
            
            # Фильтр по регионам
            if target_regions and region_key not in target_regions:
                continue
            
            # Фильтр по типам судов
            if target_courts and court_key not in target_courts:
                continue
            
            grouped[region_key].append(case_number)
        
        return dict(grouped)
    
    async def _process_region_group(
        self, 
        region_key: str, 
        case_numbers: List[str],
        semaphore: asyncio.Semaphore
    ) -> Dict[str, Any]:
        """Обработка группы дел одного региона"""
        region_config = self.settings.get_region(region_key)
        region_name = region_config['name']
        start_time = datetime.now()
        
        region_stats = {
            'processed': 0,
            'errors': 0,
            'judges_found': 0,
            'judges_not_found': 0,
            'cases_updated': 0,
            'events_added': 0,
            'docs_downloaded': 0,
            'docs_size_mb': 0.0,
        }
        
        async with semaphore:
            await self.progress.set_region_active(region_key, len(case_numbers))
            
            worker = RegionWorker(self.settings, region_key)
            
            try:
                if not await worker.initialize():
                    self.logger.error(f"Не удалось инициализировать воркер {region_key}")
                    region_stats['errors'] = len(case_numbers)
                    await self.progress.set_region_error(region_key)
                    return region_stats
                
                for case_number in case_numbers:
                    try:
                        result = await self.process_case(worker, case_number)
                        region_stats['processed'] += 1
                        
                        if result.get('error'):
                            region_stats['errors'] += 1
                        
                        # Mode-specific stats
                        if self.MODE == 'judge':
                            if result.get('judge_found'):
                                region_stats['judges_found'] += 1
                            else:
                                region_stats['judges_not_found'] += 1
                        elif self.MODE == 'events':
                            if result.get('events_added', 0) > 0:
                                region_stats['cases_updated'] += 1
                                region_stats['events_added'] += result['events_added']
                        elif self.MODE == 'docs':
                            region_stats['docs_downloaded'] += result.get('documents_downloaded', 0)
                            region_stats['docs_size_mb'] += result.get('size_mb', 0.0)
                        
                        # Update progress
                        await self.progress.update(
                            region_key,
                            processed=region_stats['processed'],
                            errors=region_stats['errors'],
                            judges_found=region_stats['judges_found'],
                            judges_not_found=region_stats['judges_not_found'],
                            cases_updated=region_stats['cases_updated'],
                            events_added=region_stats['events_added'],
                            docs_downloaded=region_stats['docs_downloaded'],
                            docs_size_mb=region_stats['docs_size_mb'],
                        )
                        
                        await asyncio.sleep(self.delay)
                        
                    except Exception as e:
                        self.logger.error(f"Ошибка обработки {case_number}: {e}")
                        region_stats['errors'] += 1
                        region_stats['processed'] += 1
                
                await self.progress.set_region_done(region_key)
                
            finally:
                await worker.cleanup()
        
        # Время выполнения
        end_time = datetime.now()
        elapsed = end_time - start_time
        minutes, seconds = divmod(int(elapsed.total_seconds()), 60)
        region_stats['time'] = f"{minutes}:{seconds:02d}"
        
        # Добавляем в репортер
        self.reporter.add_region_stats(region_key, region_name, region_stats)
        
        return region_stats
    
    async def run(self) -> Dict[str, Any]:
        """Запуск обработки"""
        case_numbers = await self.get_cases_to_process()
        
        if not case_numbers:
            self.logger.info("Нет дел для обработки")
            return {'total': 0, 'processed': 0}
        
        grouped = self._group_cases_by_region(case_numbers)
        
        if not grouped:
            self.logger.info("Нет дел после группировки")
            return {'total': len(case_numbers), 'processed': 0}
        
        # Инициализация прогресса и репортера
        regions_display = {
            key: self.settings.get_region(key)['name']
            for key in grouped.keys()
        }
        
        self.progress = UpdateProgressDisplay(self.MODE, regions_display)
        self.reporter = UpdateReporter(self.MODE)
        self.reporter.set_total_cases(len(case_numbers))
        
        # Запуск
        await self.progress.start()
        
        semaphore = asyncio.Semaphore(self.max_parallel)
        
        tasks = [
            self._process_region_group(region_key, cases, semaphore)
            for region_key, cases in grouped.items()
        ]
        
        await asyncio.gather(*tasks, return_exceptions=True)
        
        await self.progress.finish()
        
        # Финальный отчёт
        print()
        self.reporter.print_report()
        
        return {
            'total': len(case_numbers),
            'processed': self.reporter.stats.processed,
            'errors': self.reporter.stats.errors,
        }