"""
Отображение прогресса парсинга
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
class CourtProgress:
    """Прогресс одного суда"""
    saved: int = 0
    queries: int = 0
    consecutive_empty: int = 0
    gaps: int = 0
    no_judge: int = 0
    no_party: int = 0
    done: bool = False


@dataclass
class RegionProgress:
    """Прогресс региона"""
    key: str
    name: str
    status: RegionStatus = RegionStatus.WAIT
    courts: Dict[str, CourtProgress] = field(default_factory=dict)
    current_court: str = ""
    start_time: Optional[datetime] = None
    
    def elapsed(self) -> str:
        if not self.start_time:
            return "-"
        seconds = int((datetime.now() - self.start_time).total_seconds())
        minutes = seconds // 60
        secs = seconds % 60
        return f"{minutes}:{secs:02d}"
    
    def total_saved(self) -> int:
        return sum(c.saved for c in self.courts.values())
    
    def total_queries(self) -> int:
        return sum(c.queries for c in self.courts.values())


class Colors:
    """ANSI цвета для прогресса"""
    GREEN = '\033[32m'
    BRIGHT_GREEN = '\033[92m'
    RED = '\033[31m'
    GRAY = '\033[90m'
    WHITE = '\033[97m'
    RESET = '\033[0m'
    
    _enabled = True
    
    @classmethod
    def disable(cls):
        cls.GREEN = ''
        cls.BRIGHT_GREEN = ''
        cls.RED = ''
        cls.GRAY = ''
        cls.WHITE = ''
        cls.RESET = ''
        cls._enabled = False
    
    @classmethod
    def is_enabled(cls) -> bool:
        return cls._enabled


class ProgressDisplay:
    """
    Отображение прогресса парсинга
    
    Стабильная таблица без сдвигов
    """
    
    WIDTH = 110  # Ширина на треть больше (было 80)
    
    def __init__(self, regions: Dict[str, str], court_types: List[str]):
        self.regions: Dict[str, RegionProgress] = {}
        self.court_types = court_types
        self.is_republic_mode = 'cassation' in court_types or 'supreme' in court_types
        
        for key, name in regions.items():
            self.regions[key] = RegionProgress(
                key=key,
                name=self._short_name(name),
                courts={ct: CourtProgress() for ct in court_types}
            )
        
        self.lock = asyncio.Lock()
        self.started = False
        self.finished = False
        self._lines_printed = 0
        self._supports_ansi = self._check_ansi_support()
        
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
    
    def _move_cursor_up(self, n: int):
        if n > 0 and self._supports_ansi:
            sys.stdout.write(f"\033[{n}A")
    
    def _clear_line(self):
        if self._supports_ansi:
            sys.stdout.write("\033[2K\r")
    
    def _write_line(self, text: str):
        self._clear_line()
        sys.stdout.write(text + "\n")
    
    def _c(self, color: str, text: str) -> str:
        """Обернуть текст в цвет"""
        return f"{color}{text}{Colors.RESET}"
    
    def _render(self) -> List[str]:
        """Генерация строк прогресса"""
        G = Colors.GREEN
        W = self.WIDTH
        lines = []
        
        total_saved = sum(r.total_saved() for r in self.regions.values())
        total_queries = sum(r.total_queries() for r in self.regions.values())
        done_count = len([r for r in self.regions.values() if r.status == RegionStatus.DONE])
        error_count = len([r for r in self.regions.values() if r.status == RegionStatus.ERROR])
        total_regions = len(self.regions)
        
        # Заголовок
        lines.append(self._c(G, "┌" + "─" * (W - 2) + "┐"))
        header = f"  COURT PARSER v2.2{' ' * 50}[{done_count}/{total_regions} regions]"
        lines.append(self._c(G, "│") + f"{header:<{W-2}}" + self._c(G, "│"))
        lines.append(self._c(G, "├" + "─" * (W - 2) + "┤"))
        
        # Республиканские суды
        republic = self.regions.get('republic')
        if republic and self.is_republic_mode:
            kass = republic.courts.get('cassation', CourtProgress())
            supr = republic.courts.get('supreme', CourtProgress())
            
            kass_str = f"Kass: {kass.saved}"
            if kass.consecutive_empty > 0:
                kass_str += f"({kass.consecutive_empty})"
            
            supr_str = f"Supr: {supr.saved}"
            if supr.consecutive_empty > 0:
                supr_str += f"({supr.consecutive_empty})"
            
            if republic.status == RegionStatus.ACTIVE:
                status_color = Colors.BRIGHT_GREEN
            elif republic.status == RegionStatus.ERROR:
                status_color = Colors.RED
            else:
                status_color = G
            
            status_str = republic.status.value
            rep_line = f"  REPUBLIC        {kass_str:<20} {supr_str:<20} {status_str:<10} {republic.elapsed():>8}"
            lines.append(self._c(G, "│") + self._c(status_color, f"{rep_line:<{W-2}}") + self._c(G, "│"))
            lines.append(self._c(G, "├" + "─" * (W - 2) + "┤"))
        
        # Заголовок таблицы
        header_line = f"  {'STATUS':<10}  {'REGION':<14}  {'SMAS':>10}  {'APPEL':>10}  {'SAVED':>10}  {'QUERIES':>10}  {'TIME':>10}"
        lines.append(self._c(G, "│") + f"{header_line:<{W-2}}" + self._c(G, "│"))
        divider =      f"  {'─'*10}  {'─'*14}  {'─'*10}  {'─'*10}  {'─'*10}  {'─'*10}  {'─'*10}"
        lines.append(self._c(G, "│") + f"{divider:<{W-2}}" + self._c(G, "│"))
        
        # Регионы
        for key, region in self.regions.items():
            if key == 'republic':
                continue
            
            smas = region.courts.get('smas', CourtProgress())
            appel = region.courts.get('appellate', CourtProgress())
            
            # Формат SMAS
            if smas.done:
                smas_str = str(smas.saved)
            elif region.current_court == 'smas' and smas.consecutive_empty > 0:
                smas_str = f"{smas.saved}({smas.consecutive_empty})"
            elif smas.saved > 0:
                smas_str = str(smas.saved)
            else:
                smas_str = "-"
            
            # Формат APPEL
            if appel.done:
                appel_str = str(appel.saved)
            elif region.current_court == 'appellate' and appel.consecutive_empty > 0:
                appel_str = f"{appel.saved}({appel.consecutive_empty})"
            elif appel.saved > 0:
                appel_str = str(appel.saved)
            else:
                appel_str = "-"
            
            saved_total = region.total_saved()
            saved_str = str(saved_total) if saved_total > 0 else "-"
            
            queries_total = region.total_queries()
            queries_str = str(queries_total) if queries_total > 0 else "-"
            
            # Статус и цвет
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
            
            line = f"{status_str:<10}  {region.name:<14}  {smas_str:>10}  {appel_str:>10}  {saved_str:>10}  {queries_str:>10}  {region.elapsed():>10}"
            lines.append(self._c(G, "│") + self._c(color, f"  {line:<{W-4}}") + self._c(G, "│"))
        
        # Итоги
        lines.append(self._c(G, "├" + "─" * (W - 2) + "┤"))
        
        error_str = f" │ Errors: {error_count}" if error_count > 0 else ""
        pending = len([r for r in self.regions.values() if r.status == RegionStatus.WAIT])
        pending_str = f" │ Pending: {pending}" if pending > 0 else ""
        active = len([r for r in self.regions.values() if r.status == RegionStatus.ACTIVE])
        active_str = f" │ Active: {active}" if active > 0 else ""
        
        summary = f"  Total: {total_saved:,} saved │ {total_queries:,} queries{active_str}{error_str}{pending_str}"
        lines.append(self._c(G, "│") + f"{summary:<{W-2}}" + self._c(G, "│"))
        lines.append(self._c(G, "└" + "─" * (W - 2) + "┘"))
        
        return lines
    
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
            
            if 'court' in kwargs:
                region.current_court = kwargs['court']
                if region.status == RegionStatus.WAIT:
                    region.status = RegionStatus.ACTIVE
                    region.start_time = datetime.now()
            
            court_key = kwargs.get('court') or region.current_court
            if court_key and court_key in region.courts:
                court = region.courts[court_key]
                if 'saved' in kwargs:
                    court.saved = kwargs['saved']
                if 'queries' in kwargs:
                    court.queries = kwargs['queries']
                if 'consecutive_empty' in kwargs:
                    court.consecutive_empty = kwargs['consecutive_empty']
                if 'court_done' in kwargs:
                    court.done = kwargs['court_done']
                if 'gaps' in kwargs:
                    court.gaps = kwargs['gaps']
                if 'no_judge' in kwargs:
                    court.no_judge = kwargs['no_judge']
                if 'no_party' in kwargs:
                    court.no_party = kwargs['no_party']
            
            if self.started and self._supports_ansi:
                self._redraw()
    
    def _redraw(self):
        """Перерисовать прогресс"""
        try:
            if self._lines_printed > 0:
                self._move_cursor_up(self._lines_printed)
            
            lines = self._render()
            for line in lines:
                self._write_line(line)
            
            self._lines_printed = len(lines)
            sys.stdout.flush()
        except Exception:
            pass
    
    async def start(self):
        """Начать отображение"""
        async with self.lock:
            self.started = True
            set_progress_mode(True)  # Подавить консольные логи
            
            if self._supports_ansi:
                sys.stdout.write("\n")
                self._redraw()
    
    async def finish(self):
        """Завершить отображение"""
        async with self.lock:
            self.finished = True
            set_progress_mode(False)  # Включить консольные логи
            
            if self._supports_ansi:
                self._redraw()
            sys.stdout.write("\n")
            sys.stdout.flush()
    
    async def set_region_done(self, region_key: str):
        await self.update(region_key, status=RegionStatus.DONE)
    
    async def set_region_error(self, region_key: str):
        await self.update(region_key, status=RegionStatus.ERROR)
    
    async def set_court_done(self, region_key: str, court_key: str):
        await self.update(region_key, court=court_key, court_done=True)