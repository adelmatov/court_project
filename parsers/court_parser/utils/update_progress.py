"""
Прогресс-бар для Update режимов
"""
import asyncio
import sys
from typing import Dict, Optional, List
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum

from utils.logger import set_progress_mode


class RegionStatus(Enum):
    WAIT = "WAIT"
    ACTIVE = "ACTIVE"
    DONE = "DONE"
    ERROR = "ERROR"


@dataclass
class UpdateRegionStats:
    """Статистика региона для update режимов"""
    key: str
    name: str
    status: RegionStatus = RegionStatus.WAIT
    start_time: Optional[datetime] = None
    
    # Общие
    total_cases: int = 0
    processed: int = 0
    errors: int = 0
    
    # Judge mode
    judges_found: int = 0
    judges_not_found: int = 0
    
    # Events mode
    cases_updated: int = 0
    events_added: int = 0
    
    # Docs mode
    docs_downloaded: int = 0
    docs_size_mb: float = 0.0
    
    def elapsed(self) -> str:
        if not self.start_time:
            return "-"
        seconds = int((datetime.now() - self.start_time).total_seconds())
        minutes = seconds // 60
        secs = seconds % 60
        return f"{minutes}:{secs:02d}"


class Colors:
    """ANSI цвета"""
    GREEN = '\033[32m'
    BRIGHT_GREEN = '\033[92m'
    RED = '\033[31m'
    GRAY = '\033[90m'
    WHITE = '\033[97m'
    CYAN = '\033[36m'
    RESET = '\033[0m'
    
    _enabled = True
    
    @classmethod
    def disable(cls):
        cls.GREEN = ''
        cls.BRIGHT_GREEN = ''
        cls.RED = ''
        cls.GRAY = ''
        cls.WHITE = ''
        cls.CYAN = ''
        cls.RESET = ''
        cls._enabled = False

class UpdateProgressDisplay:
    """
    Прогресс-бар для Update режимов
    """
    
    WIDTH = 110
    
    def __init__(self, mode: str, regions: Dict[str, str]):
        """
        Args:
            mode: 'judge', 'events', 'docs'
            regions: {'astana': 'город Астана', ...}
        """
        self.mode = mode
        self.regions: Dict[str, UpdateRegionStats] = {}
        
        for key, name in regions.items():
            self.regions[key] = UpdateRegionStats(
                key=key,
                name=self._short_name(name)
            )
        
        self.lock = asyncio.Lock()
        self.started = False
        self.finished = False
        self._lines_printed = 0
        self._supports_ansi = self._check_ansi_support()
        self._last_render = ""
        
        if not self._supports_ansi:
            Colors.disable()
    
    def _short_name(self, name: str) -> str:
        """Сокращение названий регионов"""
        replacements = {
            'Республиканские суды': 'Republic',
            'город ': '',
            'область': '',
            'Область ': '',
            'Акмолинская': 'Akmola',
            'Актюбинская': 'Aktobe',
            'Алматинская': 'Almaty rgn',
            'Атырауская': 'Atyrau',
            'Восточно-Казахстанская': 'VKO',
            'Жамбылская': 'Zhambyl',
            'Западно-Казахстанская': 'ZKO',
            'Карагандинская': 'Karaganda',
            'Костанайская': 'Kostanay',
            'Кызылординская': 'Kyzylorda',
            'Мангистауская': 'Mangystau',
            'Павлодарская': 'Pavlodar',
            'Северо-Казахстанская': 'SKO',
            'Туркестанская': 'Turkestan',
            'Ұлытау': 'Ulytau',
            'Абай': 'Abay',
            'Жетісу': 'Zhetysu',
            'Астана': 'Astana',
            'Алматы': 'Almaty',
            'Шымкент': 'Shymkent',
        }
        for old, new in replacements.items():
            name = name.replace(old, new)
        return name.strip()[:14]
    
    def _check_ansi_support(self) -> bool:
        if not sys.stdout.isatty():
            return False
        if sys.platform == 'win32':
            try:
                import os
                os.system('')
                return True
            except:
                return False
        return True
    
    def _c(self, color: str, text: str) -> str:
        return f"{color}{text}{Colors.RESET}"
    
    def _get_title(self) -> str:
        titles = {
            'judge': 'JUDGE UPDATER',
            'events': 'EVENTS UPDATER',
            'docs': 'DOCS UPDATER'
        }
        return titles.get(self.mode, 'UPDATER')
    
    def _get_totals(self) -> Dict:
        totals = {
            'processed': sum(r.processed for r in self.regions.values()),
            'total_cases': sum(r.total_cases for r in self.regions.values()),
            'errors': sum(r.errors for r in self.regions.values()),
            'done': len([r for r in self.regions.values() if r.status == RegionStatus.DONE]),
            'active': len([r for r in self.regions.values() if r.status == RegionStatus.ACTIVE]),
        }
        
        if self.mode == 'judge':
            totals['found'] = sum(r.judges_found for r in self.regions.values())
            totals['not_found'] = sum(r.judges_not_found for r in self.regions.values())
        elif self.mode == 'events':
            totals['updated'] = sum(r.cases_updated for r in self.regions.values())
            totals['events'] = sum(r.events_added for r in self.regions.values())
        elif self.mode == 'docs':
            totals['docs'] = sum(r.docs_downloaded for r in self.regions.values())
            totals['size'] = sum(r.docs_size_mb for r in self.regions.values())
        
        return totals
    
    def _render(self) -> List[str]:
        """Генерация строк прогресса"""
        G = Colors.GREEN
        W = self.WIDTH
        lines = []
        
        totals = self._get_totals()
        progress_str = f"[{totals['processed']}/{totals['total_cases']} cases]" if totals['total_cases'] > 0 else ""
        
        # Заголовок
        lines.append(self._c(G, "┌" + "─" * (W - 2) + "┐"))
        title = self._get_title()
        header = f"  {title}{' ' * (W - len(title) - len(progress_str) - 6)}{progress_str}"
        lines.append(self._c(G, "│") + f"{header:<{W-2}}" + self._c(G, "│"))
        lines.append(self._c(G, "├" + "─" * (W - 2) + "┤"))
        
        # Заголовок таблицы
        if self.mode == 'judge':
            header_line = f"  {'STATUS':<10}  {'REGION':<14}  {'PROCESSED':>10}  {'FOUND':>10}  {'NOT FOUND':>10}  {'ERRORS':>10}  {'TIME':>10}"
        elif self.mode == 'events':
            header_line = f"  {'STATUS':<10}  {'REGION':<14}  {'PROCESSED':>10}  {'UPDATED':>10}  {'NEW EVENTS':>10}  {'ERRORS':>10}  {'TIME':>10}"
        elif self.mode == 'docs':
            header_line = f"  {'STATUS':<10}  {'REGION':<14}  {'CASES':>10}  {'DOCS':>10}  {'SIZE MB':>10}  {'ERRORS':>10}  {'TIME':>10}"
        else:
            header_line = f"  {'STATUS':<10}  {'REGION':<14}  {'PROCESSED':>10}  {'SUCCESS':>10}  {'ERRORS':>10}  {'TIME':>10}"
        
        lines.append(self._c(G, "│") + f"{header_line:<{W-2}}" + self._c(G, "│"))
        divider = f"  {'─'*10}  {'─'*14}  {'─'*10}  {'─'*10}  {'─'*10}  {'─'*10}  {'─'*10}"
        lines.append(self._c(G, "│") + f"{divider:<{W-2}}" + self._c(G, "│"))
        
        # Регионы
        for key, region in self.regions.items():
            if region.status == RegionStatus.ACTIVE:
                status_str = "* ACTIVE"
                color = Colors.BRIGHT_GREEN
            elif region.status == RegionStatus.DONE:
                status_str = "  DONE"
                color = G
            elif region.status == RegionStatus.ERROR:
                status_str = "  ERROR"
                color = Colors.RED
            else:
                status_str = "  WAIT"
                color = Colors.GRAY
            
            processed_str = str(region.processed) if region.processed > 0 else "-"
            errors_str = str(region.errors) if region.errors > 0 else "-"
            
            if self.mode == 'judge':
                found_str = str(region.judges_found) if region.judges_found > 0 else "-"
                not_found_str = str(region.judges_not_found) if region.judges_not_found > 0 else "-"
                line = f"{status_str:<10}  {region.name:<14}  {processed_str:>10}  {found_str:>10}  {not_found_str:>10}  {errors_str:>10}  {region.elapsed():>10}"
            elif self.mode == 'events':
                updated_str = str(region.cases_updated) if region.cases_updated > 0 else "-"
                events_str = str(region.events_added) if region.events_added > 0 else "-"
                line = f"{status_str:<10}  {region.name:<14}  {processed_str:>10}  {updated_str:>10}  {events_str:>10}  {errors_str:>10}  {region.elapsed():>10}"
            elif self.mode == 'docs':
                docs_str = str(region.docs_downloaded) if region.docs_downloaded > 0 else "-"
                size_str = f"{region.docs_size_mb:.1f}" if region.docs_size_mb > 0 else "-"
                line = f"{status_str:<10}  {region.name:<14}  {processed_str:>10}  {docs_str:>10}  {size_str:>10}  {errors_str:>10}  {region.elapsed():>10}"
            else:
                line = f"{status_str:<10}  {region.name:<14}  {processed_str:>10}  {errors_str:>10}  {region.elapsed():>10}"
            
            lines.append(self._c(G, "│") + self._c(color, f"  {line:<{W-4}}") + self._c(G, "│"))
        
        # Итоги
        lines.append(self._c(G, "├" + "─" * (W - 2) + "┤"))
        
        if self.mode == 'judge':
            summary = f"  Total: {totals['processed']} processed │ {totals['found']} found │ {totals['not_found']} not found │ {totals['errors']} errors"
        elif self.mode == 'events':
            summary = f"  Total: {totals['processed']} processed │ {totals['updated']} updated │ {totals['events']} new events │ {totals['errors']} errors"
        elif self.mode == 'docs':
            summary = f"  Total: {totals['processed']} cases │ {totals['docs']} docs │ {totals['size']:.1f} MB │ {totals['errors']} errors"
        else:
            summary = f"  Total: {totals['processed']} processed │ {totals['errors']} errors"
        
        lines.append(self._c(G, "│") + f"{summary:<{W-2}}" + self._c(G, "│"))
        lines.append(self._c(G, "└" + "─" * (W - 2) + "┘"))
        
        return lines
    
    def _redraw(self):
        """Перерисовать прогресс на месте"""
        if not self._supports_ansi:
            return
        
        try:
            lines = self._render()
            output = '\n'.join(lines)
            
            # Перемещаем курсор вверх и очищаем
            if self._lines_printed > 0:
                sys.stdout.write(f"\033[{self._lines_printed}A")  # Вверх
                sys.stdout.write("\033[J")  # Очистить до конца экрана
            
            # Выводим новое содержимое
            sys.stdout.write(output)
            sys.stdout.flush()
            
            self._lines_printed = len(lines)
            
        except Exception:
            pass
    
    async def update(self, region_key: str, **kwargs):
        """Обновить прогресс региона"""
        if self.finished:
            return
        
        async with self.lock:
            region = self.regions.get(region_key)
            if not region:
                return
            
            if 'status' in kwargs:
                region.status = kwargs['status']
                if kwargs['status'] == RegionStatus.ACTIVE and not region.start_time:
                    region.start_time = datetime.now()
            
            if 'total_cases' in kwargs:
                region.total_cases = kwargs['total_cases']
            if 'processed' in kwargs:
                region.processed = kwargs['processed']
            if 'errors' in kwargs:
                region.errors = kwargs['errors']
            
            # Judge mode
            if 'judges_found' in kwargs:
                region.judges_found = kwargs['judges_found']
            if 'judges_not_found' in kwargs:
                region.judges_not_found = kwargs['judges_not_found']
            
            # Events mode
            if 'cases_updated' in kwargs:
                region.cases_updated = kwargs['cases_updated']
            if 'events_added' in kwargs:
                region.events_added = kwargs['events_added']
            
            # Docs mode
            if 'docs_downloaded' in kwargs:
                region.docs_downloaded = kwargs['docs_downloaded']
            if 'docs_size_mb' in kwargs:
                region.docs_size_mb = kwargs['docs_size_mb']
            
            if self.started:
                self._redraw()
    
    async def start(self):
        """Начать отображение"""
        async with self.lock:
            self.started = True
            set_progress_mode(True)
            
            if self._supports_ansi:
                # Первая отрисовка
                lines = self._render()
                output = '\n'.join(lines)
                sys.stdout.write(output)
                sys.stdout.flush()
                self._lines_printed = len(lines)
    
    async def finish(self):
        """Завершить отображение"""
        async with self.lock:
            self.finished = True
            set_progress_mode(False)
            
            # Финальная перерисовка
            self._redraw()
            sys.stdout.write("\n")
            sys.stdout.flush()
    
    async def set_region_done(self, region_key: str):
        await self.update(region_key, status=RegionStatus.DONE)
    
    async def set_region_error(self, region_key: str):
        await self.update(region_key, status=RegionStatus.ERROR)
    
    async def set_region_active(self, region_key: str, total_cases: int = 0):
        await self.update(region_key, status=RegionStatus.ACTIVE, total_cases=total_cases)