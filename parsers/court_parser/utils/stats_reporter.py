"""
Сбор и отображение статистики парсера
"""
import re
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
from dataclasses import dataclass, field

from utils.logger import get_logger


@dataclass
class CourtStats:
    """Статистика одного суда"""
    total_cases: int = 0
    max_sequence: int = 0
    gaps_count: int = 0
    without_judge: int = 0  # только для СМАС
    without_parties: int = 0
    last_case_date: Optional[datetime] = None
    
    # Статистика сессии (заполняется во время парсинга)
    session_queries: int = 0
    session_saved: int = 0
    session_time: str = ""
    stop_reason: str = ""  # 'empty_limit', 'query_limit', 'error', 'manual', 'completed'
    consecutive_empty_at_stop: int = 0


@dataclass
class RegionStats:
    """Статистика региона"""
    name: str
    courts: Dict[str, CourtStats] = field(default_factory=dict)


@dataclass 
class DatabaseStats:
    """Общая статистика БД"""
    total_cases: int = 0
    total_judges: int = 0
    total_parties: int = 0
    total_events: int = 0
    smas_with_judge: int = 0
    smas_without_judge: int = 0
    cases_without_parties: int = 0
    total_gaps: int = 0
    first_case_date: Optional[datetime] = None
    last_case_date: Optional[datetime] = None


class StatsCollector:
    """Сбор статистики из БД"""
    
    def __init__(self, db_manager, settings):
        self.db = db_manager
        self.settings = settings
        self.logger = get_logger('stats_collector')
    
    async def collect_database_stats(self) -> DatabaseStats:
        """Собрать общую статистику БД"""
        stats = DatabaseStats()
        
        async with self.db.pool.acquire() as conn:
            # Общие счётчики
            stats.total_cases = await conn.fetchval("SELECT COUNT(*) FROM cases")
            stats.total_judges = await conn.fetchval("SELECT COUNT(*) FROM judges")
            stats.total_parties = await conn.fetchval("SELECT COUNT(*) FROM parties")
            stats.total_events = await conn.fetchval("SELECT COUNT(*) FROM case_events")
            
            # Дела без сторон
            stats.cases_without_parties = await conn.fetchval("""
                SELECT COUNT(*) FROM cases c
                WHERE NOT EXISTS (
                    SELECT 1 FROM case_parties cp WHERE cp.case_id = c.id
                )
            """)
            
            # Даты
            row = await conn.fetchrow("""
                SELECT MIN(case_date) as first_date, MAX(case_date) as last_date
                FROM cases
            """)
            if row:
                stats.first_case_date = row['first_date']
                stats.last_case_date = row['last_date']
            
            # СМАС с/без судьи
            smas_codes = self._get_smas_instance_codes()
            if smas_codes:
                codes_condition = self._build_codes_condition(smas_codes)
                
                row = await conn.fetchrow(f"""
                    SELECT 
                        COUNT(*) FILTER (WHERE judge_id IS NOT NULL) as with_judge,
                        COUNT(*) FILTER (WHERE judge_id IS NULL) as without_judge
                    FROM cases
                    WHERE {codes_condition}
                """)
                if row:
                    stats.smas_with_judge = row['with_judge']
                    stats.smas_without_judge = row['without_judge']
        
        return stats
    
    async def collect_region_stats(self, year: str) -> Dict[str, RegionStats]:
        """Собрать статистику по всем регионам"""
        regions_stats = {}
        
        for region_key, region_config in self.settings.regions.items():
            region_stats = RegionStats(name=region_config['name'])
            
            for court_key, court_config in region_config['courts'].items():
                court_stats = await self._collect_court_stats(
                    region_config, court_config, court_key, year
                )
                region_stats.courts[court_key] = court_stats
            
            regions_stats[region_key] = region_stats
        
        return regions_stats
    
    async def _collect_court_stats(
        self, 
        region_config: Dict, 
        court_config: Dict,
        court_key: str,
        year: str
    ) -> CourtStats:
        """Собрать статистику одного суда"""
        stats = CourtStats()
        
        # Формируем префикс номера дела
        kato = region_config['kato_code']
        instance = court_config['instance_code']
        year_short = year[-2:]
        case_type = court_config['case_type_code']
        prefix = f"{kato}{instance}-{year_short}-00-{case_type}/"
        
        async with self.db.pool.acquire() as conn:
            # Основные метрики
            row = await conn.fetchrow("""
                SELECT 
                    COUNT(*) as total,
                    MAX(created_at) as last_created
                FROM cases
                WHERE case_number LIKE $1
            """, f"{prefix}%")
            
            if row:
                stats.total_cases = row['total']
                stats.last_case_date = row['last_created']
            
            # Получаем все порядковые номера для анализа пропусков
            if stats.total_cases > 0:
                rows = await conn.fetch("""
                    SELECT case_number FROM cases
                    WHERE case_number LIKE $1
                """, f"{prefix}%")
                
                sequences = set()
                for r in rows:
                    try:
                        seq = int(r['case_number'].split('/')[-1])
                        sequences.add(seq)
                    except (ValueError, IndexError):
                        continue
                
                if sequences:
                    stats.max_sequence = max(sequences)
                    expected = set(range(1, stats.max_sequence + 1))
                    gaps = expected - sequences
                    stats.gaps_count = len(gaps)
            
            # Без судьи (только для СМАС)
            if court_key == 'smas':
                stats.without_judge = await conn.fetchval("""
                    SELECT COUNT(*) FROM cases
                    WHERE case_number LIKE $1 AND judge_id IS NULL
                """, f"{prefix}%")
            
            # Без сторон
            stats.without_parties = await conn.fetchval("""
                SELECT COUNT(*) FROM cases c
                WHERE c.case_number LIKE $1
                AND NOT EXISTS (
                    SELECT 1 FROM case_parties cp WHERE cp.case_id = c.id
                )
            """, f"{prefix}%")
        
        return stats
    
    def _get_smas_instance_codes(self) -> set:
        """Получить коды инстанций СМАС"""
        codes = set()
        for region_config in self.settings.regions.values():
            smas = region_config['courts'].get('smas')
            if smas:
                codes.add(smas['instance_code'])
        return codes
    
    def _build_codes_condition(self, codes: set) -> str:
        """Построить SQL условие для кодов инстанций"""
        conditions = [f"SUBSTRING(case_number FROM 3 FOR 2) = '{code}'" for code in codes]
        return f"({' OR '.join(conditions)})"


class ReportFormatter:
    """Форматирование отчётов"""
    
    WIDTH = 110
    
    def __init__(self, settings):
        self.settings = settings
        self.empty_limit = settings.parsing_settings.get('max_consecutive_empty', 5)
    
    def _line(self, content: str, left: str = "┃", right: str = "┃") -> str:
        """
        Создать строку таблицы с правильным выравниванием
        
        Учитывает Unicode-символы (эмодзи занимают 2 позиции)
        """
        # Вычисляем визуальную длину строки
        visual_len = self._visual_length(content)
        
        # Внутренняя ширина (без рамок)
        inner_width = self.WIDTH - 2
        
        # Дополняем пробелами
        padding = inner_width - visual_len
        if padding > 0:
            content = content + ' ' * padding
        elif padding < 0:
            # Обрезаем если слишком длинная
            content = self._truncate(content, inner_width)
        
        return f"{left}{content}{right}"
    
    def _visual_length(self, text: str) -> int:
        """
        Вычислить визуальную длину строки
        
        Эмодзи и некоторые Unicode-символы занимают 2 позиции
        """
        length = 0
        for char in text:
            # Эмодзи и широкие символы
            if ord(char) > 0x1F000 or char in '✅❌⚠️⏸️⏹️🔴👨‍⚖️📋📊📥📌🎉🚨':
                length += 2
            else:
                length += 1
        return length
    
    def _truncate(self, text: str, max_visual_len: int) -> str:
        """Обрезать строку до максимальной визуальной длины"""
        result = []
        current_len = 0
        
        for char in text:
            char_len = 2 if ord(char) > 0x1F000 or char in '✅❌⚠️⏸️⏹️🔴👨‍⚖️📋📊📥📌🎉🚨' else 1
            
            if current_len + char_len > max_visual_len - 1:
                result.append('…')
                break
            
            result.append(char)
            current_len += char_len
        
        return ''.join(result)
    
    def _center(self, text: str) -> str:
        """Центрирование текста с учётом Unicode"""
        visual_len = self._visual_length(text)
        total_padding = self.WIDTH - visual_len
        
        if total_padding <= 0:
            return text
        
        left_pad = total_padding // 2
        right_pad = total_padding - left_pad
        
        return ' ' * left_pad + text + ' ' * right_pad
    
    def format_start_report(
        self, 
        db_stats: DatabaseStats, 
        regions_stats: Dict[str, RegionStats],
        plan: Dict[str, Any]
    ) -> str:
        """Форматирование начального отчёта"""
        lines = []
        
        # Заголовок
        lines.append("═" * self.WIDTH)
        lines.append(self._center("ПАРСЕР СУДЕБНЫХ ДЕЛ КАЗАХСТАНА v2.1"))
        lines.append(self._center(f"Запуск: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"))
        lines.append("═" * self.WIDTH)
        lines.append("")
        
        # Общая статистика БД
        lines.extend(self._format_db_stats(db_stats))
        lines.append("")
        
        # Таблица регионов
        lines.extend(self._format_regions_table(regions_stats, plan.get('year', '2025')))
        lines.append("")
        
        # Проблемы
        problems = self._find_problems(regions_stats)
        if problems:
            lines.extend(self._format_problems(problems))
            lines.append("")
        
        # План запуска
        lines.extend(self._format_plan(plan))
        lines.append("")
        lines.append("═" * self.WIDTH)
        
        return "\n".join(lines)
    
    def format_end_report(
        self,
        db_stats_before: DatabaseStats,
        db_stats_after: DatabaseStats,
        regions_stats: Dict[str, RegionStats],
        session_stats: Dict[str, Any]
    ) -> str:
        """Форматирование финального отчёта"""
        lines = []
        
        # Заголовок
        lines.append("═" * self.WIDTH)
        lines.append(self._center("ПАРСИНГ ЗАВЕРШЁН"))
        
        start_time = session_stats.get('start_time', datetime.now())
        end_time = session_stats.get('end_time', datetime.now())
        duration = end_time - start_time
        hours, remainder = divmod(int(duration.total_seconds()), 3600)
        minutes, seconds = divmod(remainder, 60)
        
        lines.append(self._center(
            f"{start_time.strftime('%Y-%m-%d %H:%M')} — {end_time.strftime('%H:%M')} "
            f"({hours}ч {minutes}м {seconds}с)"
        ))
        lines.append("═" * self.WIDTH)
        lines.append("")
        
        # Результаты сессии
        lines.extend(self._format_session_results(session_stats))
        lines.append("")
        
        # Детализация по регионам (сессия)
        lines.extend(self._format_session_details(regions_stats))
        lines.append("")
        
        # Сравнение ДО и ПОСЛЕ
        lines.extend(self._format_comparison(db_stats_before, db_stats_after))
        lines.append("")
        
        # Финальная таблица регионов
        lines.extend(self._format_regions_table(regions_stats, session_stats.get('year', '2025'), show_status=True))
        lines.append("")
        
        # Рекомендации
        recommendations = self._generate_recommendations(regions_stats, db_stats_after)
        if recommendations:
            lines.extend(self._format_recommendations(recommendations))
            lines.append("")
        
        lines.append("═" * self.WIDTH)
        
        # Итоговый статус
        has_errors = any(
            court.stop_reason == 'error'
            for region in regions_stats.values()
            for court in region.courts.values()
        )
        
        if has_errors:
            lines.append(self._center("⚠️ ПАРСИНГ ЗАВЕРШЁН С ОШИБКАМИ"))
        else:
            lines.append(self._center("✅ ПАРСИНГ ЗАВЕРШЁН УСПЕШНО"))
        
        lines.append("═" * self.WIDTH)
        
        return "\n".join(lines)
    
    def _format_db_stats(self, stats: DatabaseStats) -> List[str]:
        """Форматирование общей статистики БД"""
        lines = []
        
        lines.append("┏" + "━" * (self.WIDTH - 2) + "┓")
        lines.append(self._line(self._center("СОСТОЯНИЕ БАЗЫ ДАННЫХ")[1 :-1]))
        lines.append("┣" + "━" * (self.WIDTH - 2) + "┫")
        
        # Левая и правая колонки
        first_date = stats.first_case_date.strftime('%Y-%m-%d') if stats.first_case_date else '—'
        last_date = stats.last_case_date.strftime('%Y-%m-%d') if stats.last_case_date else '—'
        
        rows = [
            (f"  Всего дел:          {stats.total_cases:,}", f"Первое дело:       {first_date}"),
            (f"  Уникальных судей:   {stats.total_judges:,}", f"Последнее дело:    {last_date}"),
            (f"  Уникальных сторон:  {stats.total_parties:,}", ""),
            (f"  Всего событий:      {stats.total_events:,}", ""),
        ]
        
        mid = self.WIDTH // 2
        for left, right in rows:
            content = f"{left:<{mid-1}}{right}"
            lines.append(self._line(content))
        
        lines.append(self._line(""))
        
        # Проблемные данные
        smas_total = stats.smas_with_judge + stats.smas_without_judge
        smas_pct = (stats.smas_without_judge / smas_total * 100) if smas_total > 0 else 0
        
        lines.append(self._line("  ⚠️ ПРОБЛЕМЫ"))
        lines.append(self._line("  " + "─" * 25))
        lines.append(self._line(f"  СМАС без судьи:      {stats.smas_without_judge:,} дел ({smas_pct:.1f}% от СМАС)"))
        lines.append(self._line(f"  Дел без сторон:      {stats.cases_without_parties:,} дел"))
        
        lines.append(self._line(""))
        lines.append("┗" + "━" * (self.WIDTH - 2) + "┛")
        
        return lines
    
    def _format_regions_table(
        self, 
        regions_stats: Dict[str, RegionStats],
        year: str, 
        show_status: bool = True
    ) -> List[str]:
        """Форматирование таблицы регионов"""
        lines = []
        
        # Заголовок таблицы
        lines.append("┏" + "━" * (self.WIDTH - 2) + "┓")
        lines.append(self._line(self._center(f"ДЕТАЛИЗАЦИЯ ПО РЕГИОНАМ ({year})")[1:-1]))
        lines.append("┣" + "━" * 19 + "┳" + "━" * 44 + "┳" + "━" * 43 + "┫")
        
        # Подзаголовки колонок
        lines.append("┃                   ┃              С М А С                       ┃           А П Е Л Л Я Ц И Я              ┃")
        
        if show_status:
            lines.append("┃ Регион            ┃ Всего │Пропус.│Без суд│Без ст.│ Статус   ┃ Всего │Пропус.│Без ст.│ Статус          ┃")
        else:
            lines.append("┃ Регион            ┃ Всего │Пропус.│Без суд│Без ст.│          ┃ Всего │Пропус.│Без ст.│                 ┃")
        
        lines.append("┣" + "━" * 19 + "╋" + "━" * 7 + "┿" + "━" * 7 + "┿" + "━" * 7 + "┿" + "━" * 7 + "┿" + "━" * 10 + "╋" + "━" * 7 + "┿" + "━" * 7 + "┿" + "━" * 7 + "┿" + "━" * 17 + "┫")
        
        # Данные
        totals = {'smas': CourtStats(), 'appellate': CourtStats()}
        
        for region_key, region_stats in regions_stats.items():
            smas = region_stats.courts.get('smas', CourtStats())
            appel = region_stats.courts.get('appellate', CourtStats())
            
            # Накопление итогов
            totals['smas'].total_cases += smas.total_cases
            totals['smas'].gaps_count += smas.gaps_count
            totals['smas'].without_judge += smas.without_judge
            totals['smas'].without_parties += smas.without_parties
            totals['appellate'].total_cases += appel.total_cases
            totals['appellate'].gaps_count += appel.gaps_count
            totals['appellate'].without_parties += appel.without_parties
            
            # Статусы
            smas_status = self._determine_status(smas) if show_status else ""
            appel_status = self._determine_status(appel) if show_status else ""
            
            # Короткое имя региона
            name = self._short_name(region_stats.name)
            
            # Форматируем строку с фиксированными позициями
            line = (
                f"┃ {name:<17} "
                f"┃{smas.total_cases:>6} │{smas.gaps_count:>6} │{smas.without_judge:>6} │{smas.without_parties:>6} │ {smas_status:<8} "
                f"┃{appel.total_cases:>6} │{appel.gaps_count:>6} │{appel.without_parties:>6} │ {appel_status:<15} ┃"
            )
            lines.append(line)
        
        # Итоги
        lines.append("┣" + "━" * 19 + "╋" + "━" * 7 + "┿" + "━" * 7 + "┿" + "━" * 7 + "┿" + "━" * 7 + "┿" + "━" * 10 + "╋" + "━" * 7 + "┿" + "━" * 7 + "┿" + "━" * 7 + "┿" + "━" * 17 + "┫")
        
        line = (
            f"┃ {'ИТОГО':<17} "
            f"┃{totals['smas'].total_cases:>6} │{totals['smas'].gaps_count:>6} │{totals['smas'].without_judge:>6} │{totals['smas'].without_parties:>6} │          "
            f"┃{totals['appellate'].total_cases:>6} │{totals['appellate'].gaps_count:>6} │{totals['appellate'].without_parties:>6} │                 ┃"
        )
        lines.append(line)
        
        lines.append("┗" + "━" * 19 + "┻" + "━" * 7 + "┷" + "━" * 7 + "┷" + "━" * 7 + "┷" + "━" * 7 + "┷" + "━" * 10 + "┻" + "━" * 7 + "┷" + "━" * 7 + "┷" + "━" * 7 + "┷" + "━" * 17 + "┛")
        
        # Легенда
        lines.append("")
        lines.append("Статусы: ✅ НОРМ — лимит пустых │ ✅ ГОТОВО — весь диапазон │ ⚠️ ЧАСТИЧ — неполные │ 🔴 ПУСТО — нет данных │ ❌ ОШИБКА")
        
        return lines
    
    def _determine_status(self, court: CourtStats) -> str:
        """Определить статус суда на основе данных"""
        # Если есть информация о сессии — используем её
        if court.stop_reason:
            return self._format_stop_reason(court.stop_reason)
        
        # Иначе — вычисляем по данным
        if court.total_cases == 0:
            return "🔴 ПУСТО"
        
        # Проверяем качество данных
        if court.max_sequence > 0:
            gap_ratio = court.gaps_count / court.max_sequence if court.max_sequence > 0 else 0
            if gap_ratio < 0.1:
                return "✅ НОРМ"
            else:
                return "⚠️ ЧАСТИЧ"
        
        return "✅ НОРМ"
    
    def _format_stop_reason(self, reason: str) -> str:
        """Форматирование причины остановки"""
        mapping = {
            'empty_limit': '✅ НОРМ',
            'query_limit': '⏸️ ЛИМИТ',
            'error': '❌ ОШИБКА',
            'manual': '⏹️ СТОП',
            'completed': '✅ ГОТОВО',
            '': '',
        }
        return mapping.get(reason, reason)
    
    def _short_name(self, name: str) -> str:
        """Сокращение имени региона"""
        replacements = {
            'город ': '',
            'область': 'обл.',
            'Область ': '',
            '-Казахстанская': '',
            'Восточно': 'ВК',
            'Западно': 'ЗК', 
            'Северо': 'СК',
        }
        for old, new in replacements.items():
            name = name.replace(old, new)
        return name[:17]
    
    def _find_problems(self, regions_stats: Dict[str, RegionStats]) -> Dict[str, List]:
        """Найти проблемы в данных"""
        problems = {
            'empty_regions': [],
            'high_gaps': [],
            'many_without_judge': [],
        }
        
        for region_key, region in regions_stats.items():
            smas = region.courts.get('smas', CourtStats())
            appel = region.courts.get('appellate', CourtStats())
            
            # Пустые регионы
            if smas.total_cases == 0 and appel.total_cases == 0:
                problems['empty_regions'].append(region.name)
            
            # Много пропусков
            if smas.gaps_count > 20:
                problems['high_gaps'].append((region.name, 'СМАС', smas.gaps_count))
            if appel.gaps_count > 20:
                problems['high_gaps'].append((region.name, 'Апелляция', appel.gaps_count))
            
            # Много без судьи (только СМАС)
            if smas.without_judge > 50:
                pct = (smas.without_judge / smas.total_cases * 100) if smas.total_cases > 0 else 0
                problems['many_without_judge'].append((region.name, smas.without_judge, pct))
        
        return problems
    
    def _format_problems(self, problems: Dict[str, List]) -> List[str]:
        """Форматирование блока проблем"""
        lines = []
        lines.append("┏" + "━" * (self.WIDTH - 2) + "┓")
        lines.append(self._line(self._center("⚠️ ТРЕБУЮТ ВНИМАНИЯ")[1:-1]))
        lines.append("┣" + "━" * (self.WIDTH - 2) + "┫")
        
        if problems['empty_regions']:
            lines.append(self._line(""))
            names = ", ".join(problems['empty_regions'][:5])
            if len(problems['empty_regions']) > 5:
                names += f" (+{len(problems['empty_regions']) - 5})"
            lines.append(self._line(f"  🔴 РЕГИОНЫ БЕЗ ДАННЫХ: {names}"))
        
        if problems['many_without_judge']:
            lines.append(self._line(""))
            lines.append(self._line("  👨‍⚖️ СМАС БЕЗ СУДЬИ:"))
            for name, count, pct in sorted(problems['many_without_judge'], key=lambda x: -x[1])[:5]:
                lines.append(self._line(f"     • {name}: {count} дел ({pct:.1f}%)"))
        
        if problems['high_gaps']:
            lines.append(self._line(""))
            lines.append(self._line("  📋 МНОГО ПРОПУСКОВ:"))
            for name, court, count in sorted(problems['high_gaps'], key=lambda x: -x[2])[:5]:
                lines.append(self._line(f"     • {name} {court}: {count} пропусков"))
        
        lines.append(self._line(""))
        lines.append("┗" + "━" * (self.WIDTH - 2) + "┛")
        
        return lines
    
    def _format_plan(self, plan: Dict[str, Any]) -> List[str]:
        """Форматирование плана запуска"""
        lines = []
        lines.append("┏" + "━" * (self.WIDTH - 2) + "┓")
        lines.append(self._line(self._center("📋 ПЛАН ЗАПУСКА")[1:-1]))
        lines.append("┣" + "━" * (self.WIDTH - 2) + "┫")
        lines.append(self._line(""))
        
        mode = plan.get('mode', 'parse')
        year = plan.get('year', '2025')
        courts = ", ".join(plan.get('court_types', ['smas']))
        regions = plan.get('target_regions', [])
        regions_str = ", ".join(regions[:3])
        if len(regions) > 3:
            regions_str += f" (+{len(regions) - 3})"
        
        lines.append(self._line(f"  Режим:                  {mode}"))
        lines.append(self._line(f"  Год:                    {year}"))
        lines.append(self._line(f"  Суды:                   {courts}"))
        lines.append(self._line(f"  Регионы:                {regions_str} ({len(regions)} шт)"))
        lines.append(self._line(f"  Лимит пустых подряд:    {plan.get('max_consecutive_empty', 5)}"))
        
        lines.append(self._line(""))
        lines.append("┗" + "━" * (self.WIDTH - 2) + "┛")
        
        return lines
    
    def _format_session_results(self, stats: Dict[str, Any]) -> List[str]:
        """Форматирование результатов сессии"""
        lines = []
        lines.append("┏" + "━" * (self.WIDTH - 2) + "┓")
        lines.append(self._line(self._center("РЕЗУЛЬТАТЫ СЕССИИ")[1:-1]))
        lines.append("┣" + "━" * (self.WIDTH - 2) + "┫")
        lines.append(self._line(""))
        
        regions_total = stats.get('regions_total', 0)
        regions_done = stats.get('regions_processed', 0)
        regions_failed = stats.get('regions_failed', 0)
        
        status_icon = "✅" if regions_failed == 0 else "⚠️"
        
        mid = self.WIDTH // 2
        
        # Форматируем числа отдельно, затем выравниваем строки
        total_queries = f"{stats.get('total_queries', 0):,}"
        total_saved = f"{stats.get('total_cases_saved', 0):,}"
        gaps_filled = f"{stats.get('gaps_filled', 0):,}"
        
        lines.append(self._line(f"  {'📊 ВЫПОЛНЕНО':<{mid-3}}{'📥 СОХРАНЕНО'}"))
        lines.append(self._line(f"  {'─' * 25:<{mid-3}}{'─' * 25}"))
        
        # Левая колонка
        left1 = f"Регионов: {regions_done}/{regions_total} {status_icon}"
        right1 = f"Новых дел: {total_saved}"
        lines.append(self._line(f"  {left1:<{mid-3}}{right1}"))
        
        left2 = f"Запросов: {total_queries}"
        right2 = f"Заполнено пропусков: {gaps_filled}"
        lines.append(self._line(f"  {left2:<{mid-3}}{right2}"))
        
        lines.append(self._line(""))
        lines.append("┗" + "━" * (self.WIDTH - 2) + "┛")
        
        return lines
    
    def _format_session_details(self, regions_stats: Dict[str, RegionStats]) -> List[str]:
        """Форматирование деталей сессии по регионам"""
        lines = []
        
        # Показываем только регионы с активностью в сессии
        active_regions = {
            k: v for k, v in regions_stats.items()
            if any(c.session_queries > 0 or c.stop_reason for c in v.courts.values())
        }
        
        if not active_regions:
            return lines
        
        lines.append("┏" + "━" * (self.WIDTH - 2) + "┓")
        lines.append(self._line(self._center("ДЕТАЛИЗАЦИЯ СЕССИИ")[1:-1]))
        lines.append("┣" + "━" * 19 + "┳" + "━" * 44 + "┳" + "━" * 43 + "┫")
        lines.append("┃                   ┃              С М А С                       ┃           А П Е Л Л Я Ц И Я              ┃")
        lines.append("┃ Регион            ┃Запрос.│Сохран.│Пустых │ Время  │ Остановка┃Запрос.│Сохран.│ Время  │ Остановка       ┃")
        lines.append("┣" + "━" * 19 + "╋" + "━" * 7 + "┿" + "━" * 7 + "┿" + "━" * 7 + "┿" + "━" * 8 + "┿" + "━" * 10 + "╋" + "━" * 7 + "┿" + "━" * 7 + "┿" + "━" * 8 + "┿" + "━" * 17 + "┫")
        
        for region_key, region in active_regions.items():
            smas = region.courts.get('smas', CourtStats())
            appel = region.courts.get('appellate', CourtStats())
            
            name = self._short_name(region.name)
            
            smas_stop = self._format_stop_reason(smas.stop_reason)
            appel_stop = self._format_stop_reason(appel.stop_reason)
            
            line = (
                f"┃ {name:<17} "
                f"┃{smas.session_queries:>6} │{smas.session_saved:>6} │{smas.consecutive_empty_at_stop:>6} │{smas.session_time:>7} │ {smas_stop:<8} "
                f"┃{appel.session_queries:>6} │{appel.session_saved:>6} │{appel.session_time:>7} │ {appel_stop:<15} ┃"
            )
            lines.append(line)
        
        lines.append("┗" + "━" * 19 + "┻" + "━" * 7 + "┷" + "━" * 7 + "┷" + "━" * 7 + "┷" + "━" * 8 + "┷" + "━" * 10 + "┻" + "━" * 7 + "┷" + "━" * 7 + "┷" + "━" * 8 + "┷" + "━" * 17 + "┛")
        
        return lines
    
    def _format_comparison(
        self, 
        before: DatabaseStats, 
        after: DatabaseStats
    ) -> List[str]:
        """Форматирование сравнения ДО и ПОСЛЕ"""
        lines = []
        lines.append("┏" + "━" * (self.WIDTH - 2) + "┓")
        lines.append(self._line(self._center("СРАВНЕНИЕ: ДО И ПОСЛЕ")[1:-1]))
        lines.append("┣" + "━" * (self.WIDTH - 2) + "┫")
        lines.append(self._line(""))
        
        # Заголовок таблицы сравнения
        header = f"  {'Показатель':<25}{'БЫЛО':>15}{'СТАЛО':>15}{'ИЗМЕНЕНИЕ':>15}"
        lines.append(self._line(header))
        lines.append(self._line("  " + "─" * 70))
        
        def diff_str(before_val: int, after_val: int) -> str:
            diff = after_val - before_val
            if diff > 0:
                return f"+{diff:,}"
            elif diff < 0:
                return f"{diff:,}"
            return "—"
        
        def format_row(label: str, bef: int, aft: int) -> str:
            bef_str = f"{bef:,}"
            aft_str = f"{aft:,}"
            diff = diff_str(bef, aft)
            return f"  {label:<25}{bef_str:>15}{aft_str:>15}{diff:>15}"
        
        rows = [
            ("Всего дел", before.total_cases, after.total_cases),
            ("Уникальных судей", before.total_judges, after.total_judges),
            ("Уникальных сторон", before.total_parties, after.total_parties),
            ("Всего событий", before.total_events, after.total_events),
        ]
        
        for label, bef, aft in rows:
            lines.append(self._line(format_row(label, bef, aft)))
        
        lines.append(self._line(""))
        lines.append("┗" + "━" * (self.WIDTH - 2) + "┛")
        
        return lines
    
    def _generate_recommendations(
        self, 
        regions_stats: Dict[str, RegionStats],
        db_stats: DatabaseStats
    ) -> List[str]:
        """Генерация рекомендаций"""
        recs = []
        
        # Регионы с ошибками
        error_regions = [
            region.name for region in regions_stats.values()
            if any(c.stop_reason == 'error' for c in region.courts.values())
        ]
        if error_regions:
            names = ", ".join(error_regions[:3])
            if len(error_regions) > 3:
                names += f" (+{len(error_regions) - 3})"
            recs.append(f"Перезапустить регионы с ошибками: {names}")
        
        # Много без судьи
        if db_stats.smas_without_judge > 100:
            recs.append(f"Обновить судей ({db_stats.smas_without_judge} дел СМАС без судьи): python main.py --mode update")
        
        # Много дел без сторон
        if db_stats.cases_without_parties > 50:
            recs.append(f"Проверить дела без сторон: {db_stats.cases_without_parties} шт")
        
        return recs
    
    def _format_recommendations(self, recs: List[str]) -> List[str]:
        """Форматирование рекомендаций"""
        lines = []
        lines.append("┏" + "━" * (self.WIDTH - 2) + "┓")
        lines.append(self._line(self._center("📌 РЕКОМЕНДАЦИИ")[1:-1]))
        lines.append("┣" + "━" * (self.WIDTH - 2) + "┫")
        lines.append(self._line(""))
        
        for i, rec in enumerate(recs, 1):
            lines.append(self._line(f"  {i}. {rec}"))
        
        lines.append(self._line(""))
        lines.append("┗" + "━" * (self.WIDTH - 2) + "┛")
        
        return lines


class StatsReporter:
    """Главный класс для вывода статистики"""
    
    def __init__(self, db_manager, settings):
        self.collector = StatsCollector(db_manager, settings)
        self.formatter = ReportFormatter(settings)
        self.settings = settings
        self.logger = get_logger('stats_reporter')
        
        # Сохраняем начальную статистику для сравнения
        self.db_stats_before: Optional[DatabaseStats] = None
        self.regions_stats: Dict[str, RegionStats] = {}
    
    async def print_start_report(self, plan: Dict[str, Any]):
        """Вывод начального отчёта"""
        year = plan.get('year', '2025')
        
        # Собираем статистику
        self.db_stats_before = await self.collector.collect_database_stats()
        self.regions_stats = await self.collector.collect_region_stats(year)
        
        # Форматируем
        report = self.formatter.format_start_report(
            self.db_stats_before,
            self.regions_stats,
            plan
        )
        
        # Выводим в консоль и лог
        print(report)
        self.logger.info("\n" + report)
    
    async def print_end_report(self, session_stats: Dict[str, Any]):
        """Вывод финального отчёта"""
        year = session_stats.get('year', '2025')
        
        # Собираем финальную статистику
        db_stats_after = await self.collector.collect_database_stats()
        regions_stats_after = await self.collector.collect_region_stats(year)
        
        # Объединяем с данными сессии
        for region_key, region_data in session_stats.get('regions', {}).items():
            if region_key in regions_stats_after:
                for court_key, court_data in region_data.items():
                    if court_key in regions_stats_after[region_key].courts:
                        court = regions_stats_after[region_key].courts[court_key]
                        court.session_queries = court_data.get('queries', 0)
                        court.session_saved = court_data.get('saved', 0)
                        court.session_time = court_data.get('time', '')
                        court.stop_reason = court_data.get('stop_reason', '')
                        court.consecutive_empty_at_stop = court_data.get('consecutive_empty', 0)
        
        # Форматируем
        report = self.formatter.format_end_report(
            self.db_stats_before,
            db_stats_after,
            regions_stats_after,
            session_stats
        )
        
        # Выводим
        print(report)
        self.logger.info("\n" + report)
    
    def update_court_session_stats(
        self, 
        region_key: str, 
        court_key: str,
        queries: int = 0,
        saved: int = 0,
        time_str: str = "",
        stop_reason: str = "",
        consecutive_empty: int = 0
    ):
        """Обновить статистику сессии для суда"""
        if region_key in self.regions_stats:
            court = self.regions_stats[region_key].courts.get(court_key)
            if court:
                court.session_queries = queries
                court.session_saved = saved
                court.session_time = time_str
                court.stop_reason = stop_reason
                court.consecutive_empty_at_stop = consecutive_empty