"""
Единый модуль терминального UI

Формат: одна строка = один регион
"""
import sys
import threading
import asyncio
import shutil
from typing import Dict, Optional, List
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum


class Mode(Enum):
    """Режимы работы"""
    PARSE = "PARSING"
    JUDGE = "JUDGE UPDATE"
    EVENTS = "EVENTS UPDATE"
    DOCS = "DOCS UPDATE"


class RegionStatus(Enum):
    """Статусы регионов"""
    WAIT = "wait"
    ACTIVE = "active"
    DONE = "done"
    ERROR = "error"


class CourtStatus(Enum):
    """Статусы судов"""
    WAIT = "wait"
    ACTIVE = "active"
    DONE = "done"
    ERROR = "error"


class Colors:
    """ANSI цвета"""
    RESET = '\033[0m'
    BOLD = '\033[1m'
    DIM = '\033[2m'
    
    GREEN = '\033[32m'
    BRIGHT_GREEN = '\033[92m'
    YELLOW = '\033[33m'
    RED = '\033[31m'
    CYAN = '\033[36m'
    GRAY = '\033[90m'
    WHITE = '\033[97m'
    
    # Символы
    CHECK = '✓'
    CROSS = '✗'
    WARN = '⚠'
    DOT = '●'
    
    _enabled = True
    
    @classmethod
    def disable(cls):
        for attr in ['RESET', 'BOLD', 'DIM', 'GREEN', 'BRIGHT_GREEN', 
                     'YELLOW', 'RED', 'CYAN', 'GRAY', 'WHITE']:
            setattr(cls, attr, '')
        cls.CHECK = '[v]'
        cls.CROSS = '[x]'
        cls.WARN = '[!]'
        cls.DOT = '[*]'
        cls._enabled = False
    
    @classmethod
    def strip(cls, text: str) -> str:
        import re
        return re.compile(r'\033\[[0-9;]*m').sub('', text)


@dataclass
class CourtStats:
    """Статистика суда"""
    key: str
    status: CourtStatus = CourtStatus.WAIT
    saved: int = 0
    queries: int = 0


@dataclass
class RegionStats:
    """Статистика региона"""
    key: str
    name: str
    status: RegionStatus = RegionStatus.WAIT
    courts: Dict[str, CourtStats] = field(default_factory=dict)
    current_court: str = ""
    
    # Счётчики
    total_saved: int = 0
    total_queries: int = 0
    real_errors: int = 0  # Только реальные ошибки (сеть, авторизация)
    
    # Для update режимов
    processed: int = 0
    total_cases: int = 0
    judges_found: int = 0
    events_added: int = 0
    docs_downloaded: int = 0
    
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    line_number: int = -1  # Номер строки в консоли
    
    def elapsed_str(self) -> str:
        if not self.start_time:
            return ""
        end = self.end_time or datetime.now()
        seconds = int((end - self.start_time).total_seconds())
        minutes, secs = divmod(seconds, 60)
        return f"{minutes}:{secs:02d}"


@dataclass 
class GlobalStats:
    """Глобальная статистика"""
    mode: Mode = Mode.PARSE
    start_time: datetime = field(default_factory=datetime.now)
    
    total_saved: int = 0
    total_queries: int = 0
    total_real_errors: int = 0  # Только реальные ошибки
    
    # Для update
    total_processed: int = 0
    total_found: int = 0
    
    regions_active: int = 0
    regions_done: int = 0
    regions_total: int = 0
    
    def elapsed_str(self) -> str:
        seconds = int((datetime.now() - self.start_time).total_seconds())
        hours, remainder = divmod(seconds, 3600)
        minutes, secs = divmod(remainder, 60)
        if hours > 0:
            return f"{hours}:{minutes:02d}:{secs:02d}"
        return f"{minutes:02d}:{secs:02d}"


class TerminalUI:
    """
    Терминальный UI с форматом: одна строка = один регион
    """
    
    # Ширины колонок
    COL_TIME = 10
    COL_REGION = 14
    COL_COURT = 22
    COL_DURATION = 6
    
    def __init__(self, mode: Mode = Mode.PARSE, court_types: List[str] = None):
        self.mode = mode
        self.court_types = court_types or ['smas', 'appellate']
        self.stats = GlobalStats(mode=mode)
        self.regions: Dict[str, RegionStats] = {}
        
        self._lock = threading.Lock()
        self._is_tty = sys.stdout.isatty()
        self._terminal_width = self._get_terminal_width()
        self._running = False
        self._total_lines = 0  # Количество строк регионов
        self._header_lines = 4  # Количество строк заголовка
        
        if not self._is_tty:
            Colors.disable()
        elif sys.platform == 'win32':
            self._enable_windows_ansi()
    
    def _get_terminal_width(self) -> int:
        try:
            return shutil.get_terminal_size().columns
        except:
            return 100
    
    def _enable_windows_ansi(self):
        try:
            import os
            os.system('')
        except:
            Colors.disable()
    
    def add_region(self, key: str, name: str, court_keys: List[str] = None):
        """
        Добавить регион
        
        Args:
            key: ключ региона
            name: название региона
            court_keys: список ключей судов для этого региона (если None — используются дефолтные)
        """
        with self._lock:
            # Используем переданные court_keys или дефолтные
            region_court_types = court_keys if court_keys else self.court_types
            courts = {ct: CourtStats(key=ct) for ct in region_court_types}
            
            self.regions[key] = RegionStats(
                key=key, 
                name=self._short_name(name),
                courts=courts
            )
            self.stats.regions_total += 1
    
    def _short_name(self, name: str) -> str:
        """Сокращение названий регионов"""
        replacements = {
            'Республиканские суды': 'Republic',
            'город ': '', 'область': '', 'Область ': '',
            'Акмолинская': 'Akmola', 'Актюбинская': 'Aktobe',
            'Алматинская': 'Almaty rgn', 'Атырауская': 'Atyrau',
            'Восточно-Казахстанская': 'VKO', 'Жамбылская': 'Zhambyl',
            'Западно-Казахстанская': 'ZKO', 'Карагандинская': 'Karaganda',
            'Костанайская': 'Kostanay', 'Кызылординская': 'Kyzylorda',
            'Мангистауская': 'Mangystau', 'Павлодарская': 'Pavlodar',
            'Северо-Казахстанская': 'SKO', 'Туркестанская': 'Turkestan',
            'Ұлытау': 'Ulytau', 'Абай': 'Abay', 'Жетісу': 'Zhetysu',
            'Астана': 'Astana', 'Алматы': 'Almaty', 'Шымкент': 'Shymkent',
        }
        for old, new in replacements.items():
            name = name.replace(old, new)
        return name.strip()[:12]
    
    # =========================================================================
    # РЕНДЕРИНГ СТРОКИ РЕГИОНА
    # =========================================================================
    
    def _render_region_line(self, region: RegionStats) -> str:
        """Рендеринг одной строки региона"""
        C = Colors
        
        # Время
        time_str = datetime.now().strftime('%H:%M:%S')
        time_col = f"{C.DIM}[{time_str}]{C.RESET}"
        
        # Название региона
        region_col = f"{C.CYAN}{region.name:<{self.COL_REGION}}{C.RESET}"
        
        # Суды — только те, которые есть у региона
        court_parts = []
        
        for court_key, court in region.courts.items():
            if court.status == CourtStatus.WAIT:
                court_str = f"{court_key} ..."
                color = C.GRAY
            elif court.status == CourtStatus.ACTIVE:
                court_str = f"{court_key} {C.DOT}"
                color = C.YELLOW
            elif court.status == CourtStatus.DONE:
                court_str = f"{court_key} {C.CHECK} {court.saved}"
                color = C.GREEN
            elif court.status == CourtStatus.ERROR:
                court_str = f"{court_key} {C.CROSS}"
                color = C.RED
            else:
                court_str = f"{court_key} ..."
                color = C.GRAY
            
            # Фиксированная ширина колонки
            plain_str = Colors.strip(court_str)
            padding = self.COL_COURT - len(plain_str)
            if padding < 0:
                padding = 0
            court_parts.append(f"{color}{court_str}{C.RESET}{' ' * padding}")
        
        courts_col = f"{C.DIM}│{C.RESET} ".join(court_parts)
        
        # Время выполнения региона
        duration_col = ""
        if region.status in [RegionStatus.DONE, RegionStatus.ERROR]:
            duration_col = f" {C.DIM}│{C.RESET} {region.elapsed_str()}"
        
        return f"{time_col} {region_col}{C.DIM}│{C.RESET} {courts_col}{duration_col}"
    
    def _render_update_region_line(self, region: RegionStats) -> str:
        """Рендеринг строки для update режимов"""
        C = Colors
        
        time_str = datetime.now().strftime('%H:%M:%S')
        time_col = f"{C.DIM}[{time_str}]{C.RESET}"
        
        region_col = f"{C.CYAN}{region.name:<{self.COL_REGION}}{C.RESET}"
        
        # Прогресс-бар
        if region.total_cases > 0:
            pct = region.processed / region.total_cases
            filled = int(pct * 10)
            bar = '█' * filled + '░' * (10 - filled)
            progress = f"{bar} {region.processed}/{region.total_cases}"
        elif region.status == RegionStatus.ACTIVE:
            progress = f"{C.DOT}"
        elif region.status == RegionStatus.WAIT:
            progress = "waiting..."
        else:
            progress = ""
        
        progress_col = f"{progress:<24}"
        
        # Результат по режиму
        if self.mode == Mode.JUDGE:
            result_col = f"found: {region.judges_found}"
        elif self.mode == Mode.EVENTS:
            result_col = f"events: {region.events_added}"
        elif self.mode == Mode.DOCS:
            result_col = f"docs: {region.docs_downloaded}"
        else:
            result_col = ""
        
        result_col = f"{result_col:<16}"
        
        # Время
        duration = region.elapsed_str() if region.status != RegionStatus.WAIT else ""
        
        return f"{time_col} {region_col}{C.DIM}│{C.RESET} {progress_col}{C.DIM}│{C.RESET} {result_col}{C.DIM}│{C.RESET} {duration}"
    
    # =========================================================================
    # УПРАВЛЕНИЕ ВЫВОДОМ
    # =========================================================================
    
    def _move_to_region_line(self, line_number: int):
        """Переместить курсор к строке региона"""
        if not self._is_tty:
            return
        
        # Вычисляем сколько строк вверх от текущей позиции
        current_line = self._header_lines + self._total_lines
        lines_up = current_line - line_number
        
        if lines_up > 0:
            sys.stdout.write(f'\033[{lines_up}A')  # Вверх
            sys.stdout.write('\033[2K')  # Очистить строку
            sys.stdout.write('\r')  # В начало
    
    def _restore_cursor(self):
        """Вернуть курсор в конец"""
        if not self._is_tty:
            return
        sys.stdout.write(f'\033[{self._header_lines + self._total_lines}H')
    
    def _update_region_display(self, region_key: str):
        """Обновить отображение региона"""
        if not self._running or not self._is_tty:
            return
        
        region = self.regions.get(region_key)
        if not region or region.line_number < 0:
            return
        
        with self._lock:
            # Сохраняем позицию курсора
            sys.stdout.write('\033[s')
            
            # Перемещаемся к строке региона
            self._move_to_region_line(region.line_number)
            
            # Рендерим строку
            if self.mode == Mode.PARSE:
                line = self._render_region_line(region)
            else:
                line = self._render_update_region_line(region)
            
            sys.stdout.write(line)
            
            # Восстанавливаем позицию курсора
            sys.stdout.write('\033[u')
            sys.stdout.flush()
    
    def _add_region_line(self, region_key: str):
        """Добавить новую строку региона"""
        region = self.regions.get(region_key)
        if not region:
            return
        
        with self._lock:
            region.line_number = self._header_lines + self._total_lines
            self._total_lines += 1
            
            if self.mode == Mode.PARSE:
                line = self._render_region_line(region)
            else:
                line = self._render_update_region_line(region)
            
            print(line)
    
    # =========================================================================
    # ПУБЛИЧНЫЙ API
    # =========================================================================
    
    def region_start(self, region_key: str):
        """Регион начал обработку"""
        region = self.regions.get(region_key)
        if not region:
            return
        
        region.status = RegionStatus.ACTIVE
        region.start_time = datetime.now()
        self.stats.regions_active += 1
        
        if region.line_number < 0:
            self._add_region_line(region_key)
        else:
            self._update_region_display(region_key)
    
    def court_start(self, region_key: str, court_key: str):
        """Суд начал обработку"""
        region = self.regions.get(region_key)
        if not region:
            return
        
        court = region.courts.get(court_key)
        if court:
            court.status = CourtStatus.ACTIVE
        
        region.current_court = court_key
        
        self._update_region_display(region_key)
    
    def court_done(self, region_key: str, court_key: str, saved: int = 0):
        """Суд завершил обработку"""
        region = self.regions.get(region_key)
        if not region:
            return
        
        court = region.courts.get(court_key)
        if court:
            court.status = CourtStatus.DONE
            court.saved = saved
        
        region.total_saved += saved
        self.stats.total_saved += saved
        
        self._update_region_display(region_key)
    
    def court_error(self, region_key: str, court_key: str, error: str = ""):
        """Ошибка в суде"""
        region = self.regions.get(region_key)
        if not region:
            return
        
        court = region.courts.get(court_key)
        if court:
            court.status = CourtStatus.ERROR
        
        region.real_errors += 1
        self.stats.total_real_errors += 1
        
        self._update_region_display(region_key)
    
    def region_done(self, region_key: str):
        """Регион завершил обработку"""
        region = self.regions.get(region_key)
        if not region:
            return
        
        region.status = RegionStatus.DONE
        region.end_time = datetime.now()
        self.stats.regions_active -= 1
        self.stats.regions_done += 1
        
        self._update_region_display(region_key)
    
    def region_error(self, region_key: str, error: str = ""):
        """Ошибка в регионе"""
        region = self.regions.get(region_key)
        if not region:
            return
        
        region.status = RegionStatus.ERROR
        region.end_time = datetime.now()
        region.real_errors += 1
        self.stats.regions_active -= 1
        self.stats.total_real_errors += 1
        
        self._update_region_display(region_key)
    
    def increment_saved(self, region_key: str, court_key: str, count: int = 1):
        """Увеличить счётчик сохранённых"""
        region = self.regions.get(region_key)
        if not region:
            return
        
        court = region.courts.get(court_key)
        if court:
            court.saved += count
        
        # Не обновляем дисплей на каждое сохранение - будет обновлено в court_done
    
    def increment_queries(self, region_key: str, count: int = 1):
        """Увеличить счётчик запросов"""
        region = self.regions.get(region_key)
        if region:
            region.total_queries += count
        self.stats.total_queries += count
    
    # Для update режимов
    def update_progress(self, region_key: str, processed: int = None, 
                       found: int = None, events: int = None, docs: int = None):
        """Обновить прогресс update режима"""
        region = self.regions.get(region_key)
        if not region:
            return
        
        if processed is not None:
            region.processed = processed
            self.stats.total_processed = sum(r.processed for r in self.regions.values())
        
        if found is not None:
            region.judges_found = found
            self.stats.total_found = sum(r.judges_found for r in self.regions.values())
        
        if events is not None:
            region.events_added = events
            self.stats.total_found = sum(r.events_added for r in self.regions.values())
        
        if docs is not None:
            region.docs_downloaded = docs
            self.stats.total_found = sum(r.docs_downloaded for r in self.regions.values())
        
        self._update_region_display(region_key)
    
    # =========================================================================
    # ЖИЗНЕННЫЙ ЦИКЛ
    # =========================================================================
    
    async def start(self):
        """Запустить UI"""
        self._running = True
        
        C = Colors
        width = min(self._terminal_width, 100)
        
        print()
        print(f"{C.DIM}{'═' * width}{C.RESET}")
        print(f" {C.BRIGHT_GREEN}COURT PARSER v2.2{C.RESET} │ Mode: {self.mode.value}")
        print(f"{C.DIM}{'═' * width}{C.RESET}")
        print()
    
    async def finish(self):
        """Завершить UI"""
        self._running = False
        
        C = Colors
        width = min(self._terminal_width, 100)
        
        # Пересчитываем статистику
        self.stats.total_saved = sum(
            sum(c.saved for c in r.courts.values()) 
            for r in self.regions.values()
        )
        self.stats.total_real_errors = sum(r.real_errors for r in self.regions.values())
        
        errors = self.stats.total_real_errors
        
        print()
        print(f"{C.DIM}{'─' * width}{C.RESET}")
        
        if errors == 0:
            status = f"{C.GREEN}{Colors.CHECK} COMPLETE{C.RESET}"
        else:
            status = f"{C.YELLOW}{Colors.WARN} COMPLETE WITH ERRORS{C.RESET}"
        
        done = self.stats.regions_done
        total = self.stats.regions_total
        saved = self.stats.total_saved
        queries = self.stats.total_queries
        elapsed = self.stats.elapsed_str()
        
        print(f" {status} │ {done}/{total} regions │ {saved} saved │ {queries} queries │ {elapsed}")
        print(f"{C.DIM}{'─' * width}{C.RESET}")
        print()
    
    def print_final_report(self, extra_data: Dict = None):
        """Вывести финальный отчёт"""
        C = Colors
        width = min(self._terminal_width, 80)
        
        print(f"\n{C.DIM}╔{'═' * (width - 2)}╗{C.RESET}")
        
        title = f"{self.mode.value} COMPLETE"
        print(f"{C.DIM}║{C.RESET}{title:^{width - 2}}{C.DIM}║{C.RESET}")
        
        start = self.stats.start_time.strftime('%Y-%m-%d %H:%M')
        end = datetime.now().strftime('%H:%M')
        time_line = f"{start} → {end}"
        print(f"{C.DIM}║{C.RESET}{time_line:^{width - 2}}{C.DIM}║{C.RESET}")
        print(f"{C.DIM}╠{'═' * (width - 2)}╣{C.RESET}")
        
        # Статистика
        duration = self.stats.elapsed_str()
        total_saved = self.stats.total_saved
        total_queries = self.stats.total_queries
        regions_done = self.stats.regions_done
        regions_total = self.stats.regions_total
        real_errors = self.stats.total_real_errors
        
        elapsed_sec = (datetime.now() - self.stats.start_time).total_seconds()
        speed = (total_saved / elapsed_sec * 60) if elapsed_sec > 0 and total_saved > 0 else 0
        
        stats_lines = [
            ("Duration", duration),
            ("Total saved", f"{total_saved:,} cases"),
            ("Total queries", f"{total_queries:,}"),
            ("Regions", f"{regions_done}/{regions_total} completed"),
        ]
        
        if real_errors > 0:
            stats_lines.append(("Errors", f"{C.RED}{real_errors}{C.RESET}"))
        
        stats_lines.append(("Avg speed", f"{speed:.1f} cases/min"))
        
        print(f"{C.DIM}║{C.RESET}{' ' * (width - 2)}{C.DIM}║{C.RESET}")
        for label, value in stats_lines:
            line = f"   {label:<20} {value}"
            clean_len = len(Colors.strip(line))
            padding = width - 2 - clean_len
            print(f"{C.DIM}║{C.RESET}{line}{' ' * padding}{C.DIM}║{C.RESET}")
        print(f"{C.DIM}║{C.RESET}{' ' * (width - 2)}{C.DIM}║{C.RESET}")
        
        # Проблемы
        issues = []
        if extra_data:
            no_judge = extra_data.get('no_judge', 0)
            no_parties = extra_data.get('no_parties', 0)
            if no_judge > 0 and total_saved > 0:
                pct = no_judge / total_saved * 100
                issues.append(f"{C.YELLOW}•{C.RESET} {no_judge} cases without judge ({pct:.1f}%)")
            if no_parties > 0 and total_saved > 0:
                pct = no_parties / total_saved * 100
                issues.append(f"{C.YELLOW}•{C.RESET} {no_parties} cases without parties ({pct:.1f}%)")
        
        if issues:
            print(f"{C.DIM}╠{'═' * (width - 2)}╣{C.RESET}")
            print(f"{C.DIM}║{C.RESET}   {C.YELLOW}ATTENTION{C.RESET}{' ' * (width - 14)}{C.DIM}║{C.RESET}")
            for issue in issues:
                clean_len = len(Colors.strip(issue)) + 3
                padding = width - 2 - clean_len
                print(f"{C.DIM}║{C.RESET}   {issue}{' ' * padding}{C.DIM}║{C.RESET}")
        
        print(f"{C.DIM}╚{'═' * (width - 2)}╝{C.RESET}\n")


# =============================================================================
# ГЛОБАЛЬНЫЙ ИНСТАНС
# =============================================================================

_ui_instance: Optional[TerminalUI] = None


def get_ui() -> Optional[TerminalUI]:
    return _ui_instance


def init_ui(mode: Mode = Mode.PARSE, regions: Dict[str, str] = None,
            court_types: List[str] = None,
            region_courts: Dict[str, List[str]] = None) -> TerminalUI:
    """
    Инициализировать UI
    
    Args:
        mode: режим работы
        regions: {region_key: region_name}
        court_types: дефолтные типы судов ['smas', 'appellate']
        region_courts: {region_key: [court_keys]} — суды для каждого региона
    """
    global _ui_instance
    _ui_instance = TerminalUI(mode, court_types)
    
    if regions:
        for key, name in regions.items():
            # Получаем суды для конкретного региона или дефолтные
            courts = region_courts.get(key) if region_courts else None
            _ui_instance.add_region(key, name, courts)
    
    return _ui_instance