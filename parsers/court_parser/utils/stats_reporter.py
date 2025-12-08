"""
Статистика и финальный отчёт
"""
from typing import Dict, List, Optional, Any
from datetime import datetime
from dataclasses import dataclass, field
import logging

from utils.logger import get_logger, Colors


@dataclass
class CourtStats:
    """Статистика суда"""
    total_cases: int = 0
    max_sequence: int = 0
    gaps_count: int = 0
    without_judge: int = 0
    without_parties: int = 0
    last_case_date: Optional[datetime] = None
    session_queries: int = 0
    session_saved: int = 0
    session_time: str = ""
    stop_reason: str = ""
    consecutive_empty_at_stop: int = 0


@dataclass
class RegionStats:
    """Статистика региона"""
    name: str
    courts: Dict[str, CourtStats] = field(default_factory=dict)


@dataclass
class DatabaseStats:
    """Статистика БД"""
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
        """Собрать статистику БД"""
        stats = DatabaseStats()
        
        async with self.db.pool.acquire() as conn:
            stats.total_cases = await conn.fetchval("SELECT COUNT(*) FROM cases")
            stats.total_judges = await conn.fetchval("SELECT COUNT(*) FROM judges")
            stats.total_parties = await conn.fetchval("SELECT COUNT(*) FROM parties")
            stats.total_events = await conn.fetchval("SELECT COUNT(*) FROM case_events")
            
            stats.cases_without_parties = await conn.fetchval("""
                SELECT COUNT(*) FROM cases c
                WHERE NOT EXISTS (
                    SELECT 1 FROM case_parties cp WHERE cp.case_id = c.id
                )
            """)
            
            row = await conn.fetchrow("""
                SELECT MIN(case_date) as first_date, MAX(case_date) as last_date
                FROM cases
            """)
            if row:
                stats.first_case_date = row['first_date']
                stats.last_case_date = row['last_date']
            
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
        """Собрать статистику по регионам"""
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
        self, region_config: Dict, court_config: Dict, court_key: str, year: str
    ) -> CourtStats:
        """Статистика одного суда"""
        stats = CourtStats()
        
        kato = region_config['kato_code']
        instance = court_config['instance_code']
        year_short = year[-2:]
        case_type = court_config['case_type_code']
        prefix = f"{kato}{instance}-{year_short}-00-{case_type}/"
        
        async with self.db.pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT COUNT(*) as total, MAX(created_at) as last_created
                FROM cases WHERE case_number LIKE $1
            """, f"{prefix}%")
            
            if row:
                stats.total_cases = row['total']
                stats.last_case_date = row['last_created']
            
            if stats.total_cases > 0:
                rows = await conn.fetch(
                    "SELECT case_number FROM cases WHERE case_number LIKE $1",
                    f"{prefix}%"
                )
                
                sequences = set()
                for r in rows:
                    try:
                        seq = int(r['case_number'].split('/')[-1].split('(')[0])
                        sequences.add(seq)
                    except (ValueError, IndexError):
                        continue
                
                if sequences:
                    stats.max_sequence = max(sequences)
                    expected = set(range(1, stats.max_sequence + 1))
                    stats.gaps_count = len(expected - sequences)
            
            # Дела без судьи (для любого типа суда)
            stats.without_judge = await conn.fetchval("""
                SELECT COUNT(*) FROM cases
                WHERE case_number LIKE $1 AND judge_id IS NULL
            """, f"{prefix}%")
            
            stats.without_parties = await conn.fetchval("""
                SELECT COUNT(*) FROM cases c
                WHERE c.case_number LIKE $1
                AND NOT EXISTS (SELECT 1 FROM case_parties cp WHERE cp.case_id = c.id)
            """, f"{prefix}%")
        
        return stats
    
    def _get_smas_instance_codes(self) -> set:
        codes = set()
        for region_config in self.settings.regions.values():
            smas = region_config['courts'].get('smas')
            if smas:
                codes.add(smas['instance_code'])
        return codes
    
    def _build_codes_condition(self, codes: set) -> str:
        conditions = [f"SUBSTRING(case_number FROM 3 FOR 2) = '{code}'" for code in codes]
        return f"({' OR '.join(conditions)})"


class ReportFormatter:
    """Форматирование отчётов"""
    
    WIDTH = 110
    
    def __init__(self, settings):
        self.settings = settings
    
    def _c(self, color: str, text: str) -> str:
        return f"{color}{text}{Colors.RESET}"
    
    def _short_name(self, name: str) -> str:
        """Сокращение названий"""
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
    
    def format_start_report(self, plan: Dict[str, Any]) -> str:
        """Начальный отчёт"""
        G = Colors.GREEN
        year = plan.get('year', '2025')
        courts = ', '.join(plan.get('court_types', []))
        regions_count = len(plan.get('target_regions', []))
        
        lines = [
            self._c(G, "=" * self.WIDTH),
            self._c(G, f"  COURT PARSER v2.2"),
            self._c(G, f"  Mode: {plan.get('mode', 'parse')} | Year: {year} | Courts: {courts}"),
            self._c(G, f"  Regions: {regions_count}"),
            self._c(G, "=" * self.WIDTH),
        ]
        return "\n".join(lines)
    
    def format_end_report(
        self,
        db_stats_before: DatabaseStats,
        db_stats_after: DatabaseStats,
        regions_stats: Dict[str, RegionStats],
        session_stats: Dict[str, Any]
    ) -> str:
        """Финальный отчёт"""
        G = Colors.GREEN
        BG = Colors.BRIGHT_GREEN
        R = Colors.RED
        lines = []
        
        start_time = session_stats.get('start_time', datetime.now())
        end_time = session_stats.get('end_time', datetime.now())
        
        # Заголовок
        lines.append(self._c(G, "┌" + "─" * (self.WIDTH - 2) + "┐"))
        lines.append(self._c(G, "│") + f"{'PARSING COMPLETE':^{self.WIDTH-2}}" + self._c(G, "│"))
        time_str = f"{start_time.strftime('%Y-%m-%d  %H:%M')} - {end_time.strftime('%H:%M')}"
        lines.append(self._c(G, "│") + f"{time_str:^{self.WIDTH-2}}" + self._c(G, "│"))
        lines.append(self._c(G, "└" + "─" * (self.WIDTH - 2) + "┘"))
        lines.append("")
        
        # Республиканские суды
        republic = regions_stats.get('republic')
        if republic:
            lines.append(self._c(G, "┌" + "─" * (self.WIDTH - 2) + "┐"))
            lines.append(self._c(G, "│") + f"  {'REPUBLIC COURTS':<{self.WIDTH-4}}" + self._c(G, "│"))
            lines.append(self._c(G, "├" + "─" * 13 + "┬" + "─" * 9 + "┬" + "─" * 9 + "┬" + "─" * 10 + "┬" + "─" * 10 + "┬" + "─" * 10 + "┬" + "─" * 11 + "┤"))
            
            header = f"  {'COURT':<11} │ {'SAVED':>7} │ {'GAPS':>7} │ {'NO JUDGE':>8} │ {'NO PARTY':>8} │ {'STATUS':>8} │ {'TIME':>9}"
            lines.append(self._c(G, "│") + header + self._c(G, "│"))
            lines.append(self._c(G, "├" + "─" * 13 + "┼" + "─" * 9 + "┼" + "─" * 9 + "┼" + "─" * 10 + "┼" + "─" * 10 + "┼" + "─" * 10 + "┼" + "─" * 11 + "┤"))
            
            rep_total_saved = 0
            rep_total_gaps = 0
            
            for court_key, court_name in [('cassation', 'Cassation'), ('supreme', 'Supreme')]:
                court = republic.courts.get(court_key, CourtStats())
                
                if court.stop_reason == 'error':
                    status = "ERROR"
                    status_c = R
                elif court.total_cases > 0 or court.stop_reason in ('empty_limit', 'completed'):
                    status = "DONE"
                    status_c = G
                else:
                    status = "WAIT"
                    status_c = Colors.GRAY
                
                time_str = court.session_time if court.session_time else "-"
                
                row = f"  {court_name:<11} │ {court.total_cases:>7} │ {court.gaps_count:>7} │ {court.without_judge:>8} │ {court.without_parties:>8} │ {status:>8} │ {time_str:>9}"
                lines.append(self._c(G, "│") + self._c(status_c, row) + self._c(G, "│"))
                
                rep_total_saved += court.total_cases
                rep_total_gaps += court.gaps_count
            
            lines.append(self._c(G, "├" + "─" * 13 + "┼" + "─" * 9 + "┼" + "─" * 9 + "┼" + "─" * 10 + "┼" + "─" * 10 + "┼" + "─" * 10 + "┼" + "─" * 11 + "┤"))
            total_row = f"  {'TOTAL':<11} │ {rep_total_saved:>7} │ {rep_total_gaps:>7} │ {'-':>8} │ {'-':>8} │ {' ':>8} │ {' ':>9}"
            lines.append(self._c(G, "│") + self._c(BG, total_row) + self._c(G, "│"))
            lines.append(self._c(G, "└" + "─" * 13 + "┴" + "─" * 9 + "┴" + "─" * 9 + "┴" + "─" * 10 + "┴" + "─" * 10 + "┴" + "─" * 10 + "┴" + "─" * 11 + "┘"))
            lines.append("")
        
        # Региональные суды
        lines.append(self._c(G, "┌" + "─" * (self.WIDTH - 2) + "┐"))
        lines.append(self._c(G, "│") + f"  {'REGIONAL COURTS':<{self.WIDTH-4}}" + self._c(G, "│"))
        lines.append(self._c(G, "├" + "─" * 14 + "┬" + "─" * 8 + "┬" + "─" * 8 + "┬" + "─" * 8 + "┬" + "─" * 8 + "┬" + "─" * 10 + "┬" + "─" * 10 + "┬" + "─" * 7 + "┤"))
        
        header = f"  {'REGION':<12} │ {'SMAS':>6} │ {'APPEL':>6} │ {'SAVED':>6} │ {'GAPS':>6} │ {'NO JUDGE':>8} │ {'NO PARTY':>8} │ {'ST':>5}"
        lines.append(self._c(G, "│") + header + self._c(G, "│"))
        lines.append(self._c(G, "├" + "─" * 14 + "┼" + "─" * 8 + "┼" + "─" * 8 + "┼" + "─" * 8 + "┼" + "─" * 8 + "┼" + "─" * 10 + "┼" + "─" * 10 + "┼" + "─" * 7 + "┤"))
        
        total_smas = 0
        total_appel = 0
        total_saved = 0
        total_gaps = 0
        total_no_judge = 0
        total_no_party = 0
        
        for region_key, region in regions_stats.items():
            if region_key == 'republic':
                continue
            
            smas = region.courts.get('smas', CourtStats())
            appel = region.courts.get('appellate', CourtStats())
            
            # Используем total_cases из БД
            region_saved = smas.total_cases + appel.total_cases
            region_gaps = smas.gaps_count + appel.gaps_count
            region_no_judge = smas.without_judge + appel.without_judge
            region_no_party = smas.without_parties + appel.without_parties
            
            total_smas += smas.total_cases
            total_appel += appel.total_cases
            total_saved += region_saved
            total_gaps += region_gaps
            total_no_judge += region_no_judge
            total_no_party += region_no_party
            
            has_error = smas.stop_reason == 'error' or appel.stop_reason == 'error'
            if has_error:
                status = "ERR"
                status_c = R
            else:
                status = "OK"
                status_c = G
            
            smas_str = str(smas.total_cases) if smas.total_cases > 0 else "-"
            appel_str = str(appel.total_cases) if appel.total_cases > 0 else "-"
            saved_str = str(region_saved) if region_saved > 0 else "-"
            gaps_str = str(region_gaps) if region_gaps > 0 else "-"
            no_judge_str = str(region_no_judge) if region_no_judge > 0 else "-"
            no_party_str = str(region_no_party) if region_no_party > 0 else "-"
            
            name = self._short_name(region.name)
            row = f"  {name:<12} │ {smas_str:>6} │ {appel_str:>6} │ {saved_str:>6} │ {gaps_str:>6} │ {no_judge_str:>8} │ {no_party_str:>8} │ {status:>5}"
            lines.append(self._c(G, "│") + self._c(status_c, row) + self._c(G, "│"))
        
        lines.append(self._c(G, "├" + "─" * 14 + "┼" + "─" * 8 + "┼" + "─" * 8 + "┼" + "─" * 8 + "┼" + "─" * 8 + "┼" + "─" * 10 + "┼" + "─" * 10 + "┼" + "─" * 7 + "┤"))
        total_row = f"  {'TOTAL':<12} │ {total_smas:>6} │ {total_appel:>6} │ {total_saved:>6} │ {total_gaps:>6} │ {total_no_judge:>8} │ {total_no_party:>8} │ {' ':>5}"
        lines.append(self._c(G, "│") + self._c(BG, total_row) + self._c(G, "│"))
        lines.append(self._c(G, "└" + "─" * 14 + "┴" + "─" * 8 + "┴" + "─" * 8 + "┴" + "─" * 8 + "┴" + "─" * 8 + "┴" + "─" * 10 + "┴" + "─" * 10 + "┴" + "─" * 7 + "┘"))
        lines.append("")
        
        # Сводка сессии
        duration = end_time - start_time
        hours, remainder = divmod(int(duration.total_seconds()), 3600)
        minutes, seconds = divmod(remainder, 60)
        duration_str = f"{hours}h {minutes:02d}m {seconds:02d}s"
        
        regions_total = session_stats.get('regions_total', 0)
        regions_done = session_stats.get('regions_processed', 0)
        regions_failed = session_stats.get('regions_failed', 0)
        all_saved = session_stats.get('total_cases_saved', 0)
        all_queries = session_stats.get('total_queries', 0)
        
        error_str = f" ({regions_failed} error)" if regions_failed > 0 else ""
        no_judge_pct = (total_no_judge / all_saved * 100) if all_saved > 0 else 0
        no_party_pct = (total_no_party / all_saved * 100) if all_saved > 0 else 0
        speed = all_saved / (duration.total_seconds() / 60) if duration.total_seconds() > 0 else 0
        
        lines.append(self._c(G, "┌" + "─" * (self.WIDTH - 2) + "┐"))
        lines.append(self._c(G, "│") + f"  {'SESSION SUMMARY':<{self.WIDTH-4}}" + self._c(G, "│"))
        lines.append(self._c(G, "├" + "─" * (self.WIDTH - 2) + "┤"))
        lines.append(self._c(G, "│") + f"{'':^{self.WIDTH-2}}" + self._c(G, "│"))
        
        summary_lines = [
            f"    Duration         {duration_str}",
            f"    Regions          {regions_done}/{regions_total} completed{error_str}",
            f"    Total saved      {all_saved:,} cases",
            f"    Total queries    {all_queries:,}",
            f"    Gaps found       {total_gaps}",
            f"    No judge         {total_no_judge} ({no_judge_pct:.1f}%)",
            f"    No parties       {total_no_party} ({no_party_pct:.1f}%)",
            f"    Avg speed        {speed:.1f} cases/min",
        ]
        
        for line in summary_lines:
            lines.append(self._c(G, "│") + f"{line:<{self.WIDTH-2}}" + self._c(G, "│"))
        
        lines.append(self._c(G, "│") + f"{'':^{self.WIDTH-2}}" + self._c(G, "│"))
        lines.append(self._c(G, "└" + "─" * (self.WIDTH - 2) + "┘"))
        
        return "\n".join(lines)


class StatsReporter:
    """Главный класс статистики"""
    
    def __init__(self, db_manager, settings):
        self.collector = StatsCollector(db_manager, settings)
        self.formatter = ReportFormatter(settings)
        self.settings = settings
        self.logger = get_logger('stats_reporter')
        self.report_logger = logging.getLogger('report')
        
        self.db_stats_before: Optional[DatabaseStats] = None
        self.regions_stats: Dict[str, RegionStats] = {}
    
    async def print_start_report(self, plan: Dict[str, Any]):
        """Начальный отчёт """
        year = plan.get('year', '2025')
        self.db_stats_before = await self.collector.collect_database_stats()
        self.regions_stats = await self.collector.collect_region_stats(year)
        
        report = self.formatter.format_start_report(plan)
        self.report_logger.info(report)
    
    async def print_end_report(self, session_stats: Dict[str, Any]):
        """Финальный отчёт"""
        year = session_stats.get('year', '2025')
        
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
        
        report = self.formatter.format_end_report(
            self.db_stats_before,
            db_stats_after,
            regions_stats_after,
            session_stats
        )
        
        self.report_logger.info(report)