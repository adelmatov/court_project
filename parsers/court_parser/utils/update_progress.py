"""
Прогресс для update режимов
"""
import asyncio
from typing import Dict, Optional
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum

from utils.logger import get_logger


class RegionStatus(Enum):
    WAIT = "wait"
    ACTIVE = "active"
    DONE = "done"
    ERROR = "error"


@dataclass
class RegionProgress:
    key: str
    name: str
    status: RegionStatus = RegionStatus.WAIT
    total: int = 0
    processed: int = 0
    errors: int = 0
    
    # Mode-specific
    judges_found: int = 0
    judges_not_found: int = 0
    cases_updated: int = 0
    events_added: int = 0
    docs_downloaded: int = 0
    docs_size_mb: float = 0.0
    
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None


class UpdateProgressDisplay:
    """Отображение прогресса update режимов"""
    
    def __init__(self, mode: str, regions: Dict[str, str]):
        self.mode = mode
        self.regions: Dict[str, RegionProgress] = {}
        self.logger = get_logger('update_progress')
        
        for key, name in regions.items():
            self.regions[key] = RegionProgress(key=key, name=name)
    
    async def start(self):
        self.logger.info(f"Starting {self.mode.upper()} update")
        self.logger.info(f"Regions to process: {len(self.regions)}")
    
    async def finish(self):
        total_processed = sum(r.processed for r in self.regions.values())
        total_errors = sum(r.errors for r in self.regions.values())
        self.logger.info(f"Update complete: {total_processed} processed, {total_errors} errors")
    
    async def set_region_active(self, region_key: str, total: int = 0):
        region = self.regions.get(region_key)
        if region:
            region.status = RegionStatus.ACTIVE
            region.total = total
            region.start_time = datetime.now()
            self.logger.info(f"[{region.name}] Started, {total} cases to process")
    
    async def set_region_done(self, region_key: str):
        region = self.regions.get(region_key)
        if region:
            region.status = RegionStatus.DONE
            region.end_time = datetime.now()
            elapsed = (region.end_time - region.start_time).total_seconds() if region.start_time else 0
            self.logger.info(f"[{region.name}] Done: {region.processed}/{region.total} in {elapsed:.0f}s")
    
    async def set_region_error(self, region_key: str, error: str = ""):
        region = self.regions.get(region_key)
        if region:
            region.status = RegionStatus.ERROR
            region.end_time = datetime.now()
            self.logger.error(f"[{region.name}] Error: {error}")
    
    async def update(self, region_key: str, **kwargs):
        region = self.regions.get(region_key)
        if not region:
            return
        
        for key, value in kwargs.items():
            if hasattr(region, key):
                setattr(region, key, value)