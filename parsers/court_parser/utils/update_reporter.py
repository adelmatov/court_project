"""
Отчёты для update режимов
"""
from dataclasses import dataclass, field
from typing import Dict, List
from datetime import datetime

from utils.logger import get_logger


@dataclass
class UpdateStats:
    processed: int = 0
    errors: int = 0
    judges_found: int = 0
    judges_not_found: int = 0
    cases_updated: int = 0
    events_added: int = 0
    docs_downloaded: int = 0
    docs_size_mb: float = 0.0


@dataclass
class RegionReport:
    key: str
    name: str
    stats: Dict
    time: str = ""


class UpdateReporter:
    """Генератор отчётов для update режимов"""
    
    def __init__(self, mode: str):
        self.mode = mode
        self.stats = UpdateStats()
        self.regions: List[RegionReport] = []
        self.total_cases: int = 0
        self.start_time = datetime.now()
        self.logger = get_logger('update_reporter')
    
    def set_total_cases(self, count: int):
        self.total_cases = count
    
    def add_region_stats(self, region_key: str, region_name: str, stats: Dict):
        self.regions.append(RegionReport(
            key=region_key,
            name=region_name,
            stats=stats,
            time=stats.get('time', '')
        ))
        
        # Aggregate stats
        self.stats.processed += stats.get('processed', 0)
        self.stats.errors += stats.get('errors', 0)
        self.stats.judges_found += stats.get('judges_found', 0)
        self.stats.judges_not_found += stats.get('judges_not_found', 0)
        self.stats.cases_updated += stats.get('cases_updated', 0)
        self.stats.events_added += stats.get('events_added', 0)
        self.stats.docs_downloaded += stats.get('docs_downloaded', 0)
        self.stats.docs_size_mb += stats.get('docs_size_mb', 0.0)
    
    def print_report(self):
        elapsed = datetime.now() - self.start_time
        minutes, seconds = divmod(int(elapsed.total_seconds()), 60)
        
        self.logger.info("=" * 50)
        self.logger.info(f"{self.mode.upper()} UPDATE REPORT")
        self.logger.info("=" * 50)
        self.logger.info(f"Total time: {minutes}:{seconds:02d}")
        self.logger.info(f"Processed: {self.stats.processed}/{self.total_cases}")
        self.logger.info(f"Errors: {self.stats.errors}")
        
        if self.mode == 'judge':
            self.logger.info(f"Judges found: {self.stats.judges_found}")
            self.logger.info(f"Judges not found: {self.stats.judges_not_found}")
        elif self.mode == 'events':
            self.logger.info(f"Cases updated: {self.stats.cases_updated}")
            self.logger.info(f"Events added: {self.stats.events_added}")
        elif self.mode == 'docs':
            self.logger.info(f"Documents downloaded: {self.stats.docs_downloaded}")
            self.logger.info(f"Total size: {self.stats.docs_size_mb:.1f} MB")