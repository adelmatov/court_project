"""
Финальные отчёты для Update режимов
"""
import logging
from typing import Dict, Any, Optional
from datetime import datetime
from dataclasses import dataclass

from utils.logger import Colors


@dataclass
class UpdateSessionStats:
    """Статистика сессии обновления"""
    mode: str
    start_time: datetime
    end_time: Optional[datetime] = None
    
    total_cases: int = 0
    processed: int = 0
    errors: int = 0
    
    # Judge
    judges_found: int = 0
    judges_not_found: int = 0
    
    # Events
    cases_updated: int = 0
    events_added: int = 0
    
    # Docs
    docs_downloaded: int = 0
    docs_total_size_mb: float = 0.0
    
    regions: Dict[str, Dict] = None
    
    def __post_init__(self):
        if self.regions is None:
            self.regions = {}


class UpdateReportFormatter:
    """Форматирование отчётов для Update режимов"""
    
    WIDTH = 110
    
    def _c(self, color: str, text: str) -> str:
        return f"{color}{text}{Colors.RESET}"
    
    def _short_name(self, name: str) -> str:
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
    
    def _get_title(self, mode: str) -> str:
        titles = {
            'judge': 'JUDGE UPDATE COMPLETE',
            'events': 'EVENTS UPDATE COMPLETE',
            'docs': 'DOCS UPDATE COMPLETE'
        }
        return titles.get(mode, 'UPDATE COMPLETE')
    
    def format_report(self, stats: UpdateSessionStats) -> str:
        """Форматирование финального отчёта"""
        G = Colors.GREEN
        BG = Colors.BRIGHT_GREEN
        R = Colors.RED
        W = self.WIDTH
        lines = []
        
        end_time = stats.end_time or datetime.now()
        duration = end_time - stats.start_time
        
        # Заголовок
        lines.append(self._c(G, "┌" + "─" * (W - 2) + "┐"))
        title = self._get_title(stats.mode)
        lines.append(self._c(G, "│") + f"{title:^{W-2}}" + self._c(G, "│"))
        time_str = f"{stats.start_time.strftime('%Y-%m-%d  %H:%M')} - {end_time.strftime('%H:%M')}"
        lines.append(self._c(G, "│") + f"{time_str:^{W-2}}" + self._c(G, "│"))
        lines.append(self._c(G, "└" + "─" * (W - 2) + "┘"))
        lines.append("")
        
        # Таблица регионов
        lines.append(self._c(G, "┌" + "─" * (W - 2) + "┐"))
        lines.append(self._c(G, "│") + f"  {'RESULTS BY REGION':<{W-4}}" + self._c(G, "│"))
        lines.append(self._c(G, "├" + "─" * (W - 2) + "┤"))
        
        # Заголовок таблицы
        if stats.mode == 'judge':
            header = f"  {'REGION':<14}  {'CASES':>10}  {'FOUND':>10}  {'NOT FOUND':>10}  {'ERRORS':>10}  {'TIME':>10}  {'STATUS':>8}"
        elif stats.mode == 'events':
            header = f"  {'REGION':<14}  {'CASES':>10}  {'UPDATED':>10}  {'NEW EVENTS':>10}  {'ERRORS':>10}  {'TIME':>10}  {'STATUS':>8}"
        elif stats.mode == 'docs':
            header = f"  {'REGION':<14}  {'CASES':>10}  {'DOCS':>10}  {'SIZE MB':>10}  {'ERRORS':>10}  {'TIME':>10}  {'STATUS':>8}"
        else:
            header = f"  {'REGION':<14}  {'CASES':>10}  {'SUCCESS':>10}  {'ERRORS':>10}  {'TIME':>10}  {'STATUS':>8}"
        
        lines.append(self._c(G, "│") + f"{header:<{W-2}}" + self._c(G, "│"))
        lines.append(self._c(G, "├" + "─" * (W - 2) + "┤"))
        
        # Данные регионов
        totals = {
            'cases': 0, 'found': 0, 'not_found': 0, 'updated': 0,
            'events': 0, 'docs': 0, 'size': 0.0, 'errors': 0
        }
        
        for region_key, region_data in stats.regions.items():
            name = self._short_name(region_data.get('name', region_key))
            cases = region_data.get('processed', 0)
            errors = region_data.get('errors', 0)
            time_str = region_data.get('time', '-')
            
            totals['cases'] += cases
            totals['errors'] += errors
            
            status = "ERROR" if errors > 0 else "OK"
            status_c = R if errors > 0 else G
            
            cases_str = str(cases) if cases > 0 else "-"
            errors_str = str(errors) if errors > 0 else "-"
            
            if stats.mode == 'judge':
                found = region_data.get('judges_found', 0)
                not_found = region_data.get('judges_not_found', 0)
                totals['found'] += found
                totals['not_found'] += not_found
                found_str = str(found) if found > 0 else "-"
                not_found_str = str(not_found) if not_found > 0 else "-"
                row = f"  {name:<14}  {cases_str:>10}  {found_str:>10}  {not_found_str:>10}  {errors_str:>10}  {time_str:>10}  {status:>8}"
            elif stats.mode == 'events':
                updated = region_data.get('cases_updated', 0)
                events = region_data.get('events_added', 0)
                totals['updated'] += updated
                totals['events'] += events
                updated_str = str(updated) if updated > 0 else "-"
                events_str = str(events) if events > 0 else "-"
                row = f"  {name:<14}  {cases_str:>10}  {updated_str:>10}  {events_str:>10}  {errors_str:>10}  {time_str:>10}  {status:>8}"
            elif stats.mode == 'docs':
                docs = region_data.get('docs_downloaded', 0)
                size = region_data.get('docs_size_mb', 0.0)
                totals['docs'] += docs
                totals['size'] += size
                docs_str = str(docs) if docs > 0 else "-"
                size_str = f"{size:.1f}" if size > 0 else "-"
                row = f"  {name:<14}  {cases_str:>10}  {docs_str:>10}  {size_str:>10}  {errors_str:>10}  {time_str:>10}  {status:>8}"
            else:
                row = f"  {name:<14}  {cases_str:>10}  {errors_str:>10}  {time_str:>10}  {status:>8}"
            
            lines.append(self._c(G, "│") + self._c(status_c, f"{row:<{W-2}}") + self._c(G, "│"))
        
        # Итоговая строка
        lines.append(self._c(G, "├" + "─" * (W - 2) + "┤"))
        
        if stats.mode == 'judge':
            total_row = f"  {'TOTAL':<14}  {totals['cases']:>10}  {totals['found']:>10}  {totals['not_found']:>10}  {totals['errors']:>10}  {'':>10}  {'':>8}"
        elif stats.mode == 'events':
            total_row = f"  {'TOTAL':<14}  {totals['cases']:>10}  {totals['updated']:>10}  {totals['events']:>10}  {totals['errors']:>10}  {'':>10}  {'':>8}"
        elif stats.mode == 'docs':
            total_row = f"  {'TOTAL':<14}  {totals['cases']:>10}  {totals['docs']:>10}  {totals['size']:>10.1f}  {totals['errors']:>10}  {'':>10}  {'':>8}"
        else:
            total_row = f"  {'TOTAL':<14}  {totals['cases']:>10}  {totals['errors']:>10}  {'':>10}  {'':>8}"
        
        lines.append(self._c(G, "│") + self._c(BG, f"{total_row:<{W-2}}") + self._c(G, "│"))
        lines.append(self._c(G, "└" + "─" * (W - 2) + "┘"))
        lines.append("")
        
        # Summary
        lines.append(self._c(G, "┌" + "─" * (W - 2) + "┐"))
        lines.append(self._c(G, "│") + f"  {'SUMMARY':<{W-4}}" + self._c(G, "│"))
        lines.append(self._c(G, "├" + "─" * (W - 2) + "┤"))
        
        hours, remainder = divmod(int(duration.total_seconds()), 3600)
        minutes, seconds = divmod(remainder, 60)
        if hours > 0:
            duration_str = f"{hours}h {minutes:02d}m {seconds:02d}s"
        else:
            duration_str = f"{minutes}m {seconds:02d}s"
        
        speed = totals['cases'] / (duration.total_seconds() / 60) if duration.total_seconds() > 0 else 0
        
        summary_lines = [f"    Duration           {duration_str}"]
        
        if stats.mode == 'judge':
            pct_found = (totals['found'] / totals['cases'] * 100) if totals['cases'] > 0 else 0
            summary_lines.extend([
                f"    Cases checked      {totals['cases']}",
                f"    Judges found       {totals['found']} ({pct_found:.1f}%)",
                f"    Still no judge     {totals['not_found']}",
                f"    Errors             {totals['errors']}",
                f"    Avg speed          {speed:.1f} cases/min",
            ])
        elif stats.mode == 'events':
            pct_updated = (totals['updated'] / totals['cases'] * 100) if totals['cases'] > 0 else 0
            avg_events = totals['events'] / totals['updated'] if totals['updated'] > 0 else 0
            summary_lines.extend([
                f"    Cases checked      {totals['cases']}",
                f"    Cases updated      {totals['updated']} ({pct_updated:.1f}%)",
                f"    New events added   {totals['events']}",
                f"    Avg events/case    {avg_events:.1f}",
                f"    Errors             {totals['errors']}",
                f"    Avg speed          {speed:.1f} cases/min",
            ])
        elif stats.mode == 'docs':
            avg_docs = totals['docs'] / totals['cases'] if totals['cases'] > 0 else 0
            summary_lines.extend([
                f"    Cases checked      {totals['cases']}",
                f"    Documents found    {totals['docs']}",
                f"    Total size         {totals['size']:.1f} MB",
                f"    Avg docs/case      {avg_docs:.1f}",
                f"    Errors             {totals['errors']}",
                f"    Avg speed          {speed:.1f} cases/min",
            ])
        
        for line in summary_lines:
            lines.append(self._c(G, "│") + f"{line:<{W-2}}" + self._c(G, "│"))
        
        lines.append(self._c(G, "└" + "─" * (W - 2) + "┘"))
        
        return "\n".join(lines)


class UpdateReporter:
    """Репортер для Update режимов"""
    
    def __init__(self, mode: str):
        self.mode = mode
        self.formatter = UpdateReportFormatter()
        self.report_logger = logging.getLogger('report')
        self.stats = UpdateSessionStats(mode=mode, start_time=datetime.now())
    
    def set_total_cases(self, total: int):
        self.stats.total_cases = total
    
    def add_region_stats(self, region_key: str, region_name: str, data: Dict):
        self.stats.regions[region_key] = {
            'name': region_name,
            **data
        }
        
        # Обновляем общие счётчики
        self.stats.processed += data.get('processed', 0)
        self.stats.errors += data.get('errors', 0)
        
        if self.mode == 'judge':
            self.stats.judges_found += data.get('judges_found', 0)
            self.stats.judges_not_found += data.get('judges_not_found', 0)
        elif self.mode == 'events':
            self.stats.cases_updated += data.get('cases_updated', 0)
            self.stats.events_added += data.get('events_added', 0)
        elif self.mode == 'docs':
            self.stats.docs_downloaded += data.get('docs_downloaded', 0)
            self.stats.docs_total_size_mb += data.get('docs_size_mb', 0.0)
    
    def print_report(self):
        self.stats.end_time = datetime.now()
        report = self.formatter.format_report(self.stats)
        self.report_logger.info(report)