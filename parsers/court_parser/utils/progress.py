"""
Отображение прогресса параллельного парсинга
"""
import asyncio
import sys
from typing import Dict, Optional, List
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum


class RegionStatus(Enum):
    WAITING = "⏸️"
    ACTIVE = "⏳"
    DONE = "✅"
    ERROR = "❌"


@dataclass
class CourtProgress:
    """Прогресс одного суда"""
    saved: int = 0
    queries: int = 0
    consecutive_empty: int = 0
    done: bool = False


@dataclass
class RegionProgress:
    """Прогресс региона с несколькими судами"""
    key: str
    name: str
    status: RegionStatus = RegionStatus.WAITING
    courts: Dict[str, CourtProgress] = field(default_factory=dict)
    current_court: str = ""
    start_time: Optional[datetime] = None
    
    def elapsed(self) -> str:
        if not self.start_time:
            return "--:--"
        seconds = int((datetime.now() - self.start_time).total_seconds())
        return f"{seconds // 60}:{seconds % 60:02d}"
    
    def total_saved(self) -> int:
        return sum(c.saved for c in self.courts.values())
    
    def total_queries(self) -> int:
        return sum(c.queries for c in self.courts.values())


class ProgressDisplay:
    """Улучшенное отображение прогресса с поддержкой нескольких судов"""
    
    def __init__(self, regions: Dict[str, str], court_types: List[str]):
        """
        Args:
            regions: {region_key: display_name}
            court_types: ['smas', 'appellate']
        """
        self.regions: Dict[str, RegionProgress] = {}
        self.court_types = court_types
        
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
    
    def _short_name(self, name: str) -> str:
        """Сокращение длинных названий"""
        replacements = {
            'город ': '',
            'область': 'обл.',
            'Область ': '',
            '-Казахстанская': '-Каз.',
            'Восточно-Каз.': 'ВКО',
            'Западно-Каз.': 'ЗКО',
            'Северо-Каз.': 'СКО',
        }
        for old, new in replacements.items():
            name = name.replace(old, new)
        
        if len(name) > 18:
            name = name[:17] + "…"
        
        return name
    
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
        """Переместить курсор вверх на n строк"""
        if n > 0 and self._supports_ansi:
            sys.stdout.write(f"\033[{n}A")
    
    def _clear_line(self):
        """Очистить текущую строку"""
        if self._supports_ansi:
            sys.stdout.write("\033[2K\r")
    
    def _write_line(self, text: str):
        """Записать строку"""
        self._clear_line()
        sys.stdout.write(text + "\n")
    
    def _get_active_regions(self) -> List[RegionProgress]:
        return [r for r in self.regions.values() if r.status == RegionStatus.ACTIVE]
    
    def _get_done_regions(self) -> List[RegionProgress]:
        return [r for r in self.regions.values() if r.status == RegionStatus.DONE]
    
    def _get_waiting_regions(self) -> List[RegionProgress]:
        return [r for r in self.regions.values() if r.status == RegionStatus.WAITING]
    
    def _get_error_regions(self) -> List[RegionProgress]:
        return [r for r in self.regions.values() if r.status == RegionStatus.ERROR]
    
    def _render(self) -> List[str]:
        """Генерация строк для отображения"""
        lines = []
        
        total_saved = sum(r.total_saved() for r in self.regions.values())
        total_queries = sum(r.total_queries() for r in self.regions.values())
        done_count = len(self._get_done_regions())
        active_count = len(self._get_active_regions())
        error_count = len(self._get_error_regions())
        
        lines.append("═" * 72)
        
        status_parts = []
        if done_count > 0:
            status_parts.append(f"{done_count}✅")
        if active_count > 0:
            status_parts.append(f"{active_count}⏳")
        if error_count > 0:
            status_parts.append(f"{error_count}❌")
        status_str = " ".join(status_parts) if status_parts else "0"
        
        header = f" ПАРСИНГ │ {status_str} / {len(self.regions)} │ {total_saved} сохр │ {total_queries} запр"
        lines.append(header)
        lines.append("═" * 72)
        
        active = self._get_active_regions()
        if active:
            lines.append("")
            for region in active:
                court_parts = []
                for ct in self.court_types:
                    court = region.courts[ct]
                    ct_label = ct.upper()[:4]
                    
                    if court.done:
                        status = f"✓{court.saved}"
                    elif ct == region.current_court:
                        status = f"▶{court.saved}"
                        if court.consecutive_empty > 0:
                            status += f"⌀{court.consecutive_empty}"
                    elif court.queries > 0:
                        status = f"{court.saved}"
                    else:
                        status = "—"
                    
                    court_parts.append(f"{ct_label}:{status}")
                
                courts_str = "  ".join(court_parts)
                line = f" {region.status.value} {region.name:<18} {courts_str:<32} {region.elapsed()}"
                lines.append(line)
            lines.append("")
        
        lines.append("─" * 72)
        
        done = self._get_done_regions()
        if done:
            done_items = [f"{r.name}({r.total_saved()})" for r in done[:4]]
            done_str = ", ".join(done_items)
            if len(done) > 4:
                done_str += f" (+{len(done) - 4})"
            lines.append(f" ✅ Готово: {done_str}")
        
        errors = self._get_error_regions()
        if errors:
            error_names = ", ".join([r.name for r in errors[:3]])
            if len(errors) > 3:
                error_names += f" (+{len(errors) - 3})"
            lines.append(f" ❌ Ошибки: {error_names}")
        
        waiting = self._get_waiting_regions()
        if waiting:
            waiting_names = ", ".join([r.name for r in waiting[:3]])
            if len(waiting) > 3:
                waiting_names += f"… (+{len(waiting) - 3})"
            lines.append(f" ⏸️  Ожидают: {waiting_names}")
        
        lines.append("═" * 72)
        
        return lines
    
    async def update(self, region_key: str, **kwargs):
        """
        Обновить прогресс региона
        
        Args:
            region_key: ключ региона
            court: текущий суд
            saved: сохранено дел
            queries: выполнено запросов
            consecutive_empty: пустых подряд
            court_done: суд завершён
            status: статус региона (RegionStatus)
        """
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
                if region.status == RegionStatus.WAITING:
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
            
            if self.started and self._supports_ansi:
                self._redraw()
    
    def _redraw(self):
        """Перерисовать экран"""
        try:
            if self._lines_printed > 0:
                self._move_cursor_up(self._lines_printed)
            
            lines = self._render()
            
            for line in lines:
                self._write_line(line)
            
            self._lines_printed = len(lines)
            sys.stdout.flush()
            
        except Exception:
            # Молча игнорируем ошибки отрисовки, чтобы не прерывать основной процесс
            pass
    
    async def start(self):
        """Начать отображение"""
        async with self.lock:
            self.started = True
            if self._supports_ansi:
                sys.stdout.write("\n")
                self._redraw()
    
    async def finish(self):
        """Завершить отображение"""
        async with self.lock:
            self.finished = True
            if self._supports_ansi:
                self._redraw()
            sys.stdout.write("\n")
            sys.stdout.flush()
    
    async def set_region_done(self, region_key: str):
        """Пометить регион как завершённый"""
        await self.update(region_key, status=RegionStatus.DONE)
    
    async def set_region_error(self, region_key: str):
        """Пометить регион как ошибочный"""
        await self.update(region_key, status=RegionStatus.ERROR)
    
    async def set_court_done(self, region_key: str, court_key: str):
        """Пометить суд региона как завершённый"""
        await self.update(region_key, court=court_key, court_done=True)