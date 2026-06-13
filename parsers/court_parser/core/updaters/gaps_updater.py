"""
Проверка и закрытие пропусков в нумерации дел
"""
from typing import Dict, List, Any, Set, Tuple
from datetime import datetime
from collections import defaultdict

from core.updaters.base_updater import BaseUpdater
from core.region_worker import RegionWorker
from utils.constants import CaseStatus


class GapsUpdater(BaseUpdater):
    """
    Updater для проверки пропущенных номеров дел
    
    Команда: --mode gaps
    
    Логика:
    1. Для каждого региона/суда/года получает существующие номера из БД
    2. Находит пропуски в последовательности
    3. Пытается спарсить пропущенные номера
    4. Обновляет метку времени проверки
    """
    
    MODE = 'gaps'
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
        # Конфигурация gaps
        self.gaps_config = self.settings.parsing_settings
        self.max_gaps_per_session = self.gaps_config.get('max_gaps_per_session', 200)
        self.gaps_check_interval_days = self.gaps_config.get('gaps_check_interval_days', 30)
        
        # Статистика
        self.total_gaps_found = 0
        self.total_gaps_closed = 0
    
    def get_config(self) -> Dict[str, Any]:
        """Получить конфигурацию"""
        return self.gaps_config
    
    async def get_cases_to_process(self) -> List[str]:
        """
        Этот метод не используется напрямую для gaps.
        Вместо этого используем get_gaps_to_process()
        """
        return []
    
    async def process_case(self, worker: RegionWorker, case_number: str) -> Dict[str, Any]:
        """Обработка одного пропущенного номера"""
        result = {
            'case_number': case_number,
            'success': False,
            'found': False,
            'saved': False
        }
        
        try:
            # Определяем регион и суд по номеру
            case_info = self.text_processor.find_region_and_court_by_case_number(
                case_number, self.settings.regions
            )
            
            if not case_info:
                result['error'] = 'region_not_found'
                return result
            
            # Поиск дела
            search_result = await worker.search_and_save(
                db_manager=self.db_manager,
                court_key=case_info['court_key'],
                sequence_number=int(case_info['sequence']),
                year=case_info['year']
            )
            
            result['success'] = True
            result['found'] = search_result.get('saved', False)
            result['saved'] = search_result.get('saved', False)
            
            if result['saved']:
                self.total_gaps_closed += 1
                self.logger.info(f"✅ Пропуск закрыт: {case_number}")
            else:
                self.logger.debug(f"Пропуск не найден на сайте: {case_number}")
        
        except Exception as e:
            self.logger.error(f"Ошибка обработки пропуска {case_number}: {e}")
            result['error'] = str(e)
        
        return result
    
    async def get_gaps_for_court(
        self,
        region_key: str,
        court_key: str,
        year: str
    ) -> List[int]:
        """
        Получить список пропущенных номеров для суда.

        Ищет дырки ДВУХ типов:
        1. Внутренние — между min и max существующих (классические пропуски)
        2. Хвостовые — несколько номеров ЗА max (на случай если сетевой сбой
        прервал парсинг в хвосте, и дела после max остались несобранными)
        """
        existing = await self.db_manager.get_existing_case_numbers(
            region_key, court_key, year, self.settings
        )

        if not existing:
            return []

        max_seq = max(existing)
        min_seq = min(existing)

        # 1. Внутренние дырки
        full_range = set(range(min_seq, max_seq + 1))
        gaps = full_range - existing

        # 2. Хвостовая проверка: пробуем несколько номеров за max.
        #    Если сбой оборвал хвост — дела найдутся. Если их реально нет —
        #    process_case вернёт no_results, и они просто не сохранятся.
        tail_probe = self.gaps_config.get('gaps_tail_probe', 5)
        for i in range(1, tail_probe + 1):
            gaps.add(max_seq + i)

        return sorted(gaps)
    
    async def run(self) -> Dict[str, Any]:
        """
        Запуск проверки пропусков
        
        Переопределяем базовый run() для специфичной логики gaps
        """
        years = self.settings.get_parsing_years()  # ← список годов
        court_types = self.gaps_config.get('court_types', ['smas', 'appellate'])
        target_regions = self.settings.get_target_regions()

        # Собираем все пропуски по регионам
        all_gaps: Dict[str, List[str]] = defaultdict(list)
        gaps_by_court: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))

        # Сохраняем для какого года какой суд проверялся (для обновления метаданных)
        # ключ: (region_key, court_key) → year
        checked_pairs: List[Tuple[str, str, str]] = []

        self.logger.info("=" * 60)
        self.logger.info(f"GAPS CHECK: Поиск пропусков по годам {years}")
        self.logger.info("=" * 60)

        # Фаза 1: Сбор пропусков по ВСЕМ годам
        for year in years:
            for region_key in target_regions:
                region_config = self.settings.get_region(region_key)
                available_courts = list(region_config.get('courts', {}).keys())

                courts_to_check = [c for c in court_types if c in available_courts]
                if not courts_to_check and available_courts:
                    courts_to_check = available_courts

                for court_key in courts_to_check:
                    should_check = await self.db_manager.should_check_gaps(
                        region_key, court_key, year, self.gaps_check_interval_days
                    )

                    if not should_check:
                        self.logger.debug(
                            f"Пропускаем {region_key}/{court_key}/{year} - "
                            f"проверялось менее {self.gaps_check_interval_days} дней назад"
                        )
                        continue

                    gaps = await self.get_gaps_for_court(region_key, court_key, year)

                    checked_pairs.append((region_key, court_key, year))

                    if gaps:
                        region_cfg = self.settings.get_region(region_key)
                        court_cfg = self.settings.get_court(region_key, court_key)

                        for seq in gaps:
                            case_number = self.text_processor.generate_case_number(
                                region_cfg, court_cfg, year, seq
                            )
                            all_gaps[region_key].append(case_number)

                        gaps_by_court[region_key][court_key] += len(gaps)
                        self.total_gaps_found += len(gaps)

                        self.logger.info(
                            f"📋 {region_key}/{court_key}/{year}: найдено {len(gaps)} пропусков"
                        )
        
        # Проверяем есть ли пропуски
        if not all_gaps:
            self.logger.info("✅ Пропусков не найдено!")
            return {'total_gaps': 0, 'closed': 0}
        
        self.logger.info("-" * 60)
        self.logger.info(f"Всего пропусков: {self.total_gaps_found}")
        
        # Ограничение на сессию
        total_to_process = sum(len(g) for g in all_gaps.values())
        if total_to_process > self.max_gaps_per_session:
            self.logger.warning(
                f"⚠️ Ограничение: обработаем только {self.max_gaps_per_session} "
                f"из {total_to_process} пропусков"
            )
            all_gaps = self._limit_gaps(all_gaps, self.max_gaps_per_session)
        
        self.logger.info("-" * 60)
        
        # Фаза 2: Инициализация UI
        from utils.terminal_ui import init_ui, Mode
        
        regions_display = {
            key: self.settings.get_region(key)['name']
            for key in all_gaps.keys()
        }
        
        # Для gaps не нужны court_types в UI
        ui = init_ui(Mode.PARSE, regions_display, court_types=[])
        
        # Устанавливаем total для каждого региона
        for region_key, gaps_list in all_gaps.items():
            region = ui.regions.get(region_key)
            if region:
                region.total_cases = len(gaps_list)
        
        await ui.start()
        
        # Фаза 3: Обработка пропусков
        import asyncio
        semaphore = asyncio.Semaphore(self.max_parallel)
        
        async def process_region_gaps(region_key: str, gap_numbers: List[str]):
            async with semaphore:
                worker = RegionWorker(self.settings, region_key)
                
                try:
                    if not await worker.initialize():
                        self.logger.error(f"Не удалось инициализировать воркер {region_key}")
                        ui.region_error(region_key, "Init failed")
                        return
                    
                    ui.region_start(region_key)
                    
                    processed = 0
                    closed = 0
                    
                    for case_number in gap_numbers:
                        result = await self.process_case(worker, case_number)
                        processed += 1
                        
                        if result.get('saved'):
                            closed += 1
                        
                        # Обновляем UI
                        ui.update_progress(
                            region_key, 
                            processed=processed, 
                            found=closed
                        )
                        
                        await asyncio.sleep(self.delay)
                    
                    ui.region_done(region_key)
                    
                    # Обновляем дату проверки пропусков по всем проверенным (суд, год)
                    region_checked = [
                        (ck, yr) for (rk, ck, yr) in checked_pairs
                        if rk == region_key
                    ]
                    for court_key, yr in region_checked:
                        existing = await self.db_manager.get_existing_case_numbers(
                            region_key, court_key, yr, self.settings
                        )
                        max_seq = max(existing) if existing else 0
                        await self.db_manager.update_gaps_check_date(
                            region_key, court_key, yr, max_seq
                        )
                    
                except Exception as e:
                    self.logger.error(f"Ошибка в регионе {region_key}: {e}")
                    ui.region_error(region_key, str(e))
                
                finally:
                    await worker.cleanup()
        
        # Запускаем обработку
        tasks = [
            process_region_gaps(region_key, gaps_list)
            for region_key, gaps_list in all_gaps.items()
        ]
        
        await asyncio.gather(*tasks, return_exceptions=True)
        
        await ui.finish()
        
        # Финальный отчёт
        self._print_report()
        
        return {
            'total_gaps': self.total_gaps_found,
            'closed': self.total_gaps_closed,
            'remaining': self.total_gaps_found - self.total_gaps_closed
        }
    
    def _limit_gaps(
        self, 
        all_gaps: Dict[str, List[str]], 
        limit: int
    ) -> Dict[str, List[str]]:
        """
        Ограничить количество пропусков с приоритетом по регионам
        
        Распределяем лимит пропорционально количеству пропусков
        """
        total = sum(len(g) for g in all_gaps.values())
        if total <= limit:
            return all_gaps
        
        result = {}
        remaining = limit
        
        # Сортируем регионы по количеству пропусков (больше = приоритетнее)
        sorted_regions = sorted(
            all_gaps.items(), 
            key=lambda x: len(x[1]), 
            reverse=True
        )
        
        for region_key, gaps in sorted_regions:
            # Пропорциональное распределение
            region_share = int(limit * len(gaps) / total)
            region_share = min(region_share, remaining, len(gaps))
            
            if region_share > 0:
                result[region_key] = gaps[:region_share]
                remaining -= region_share
        
        return result
    
    def _print_report(self):
        """Вывод финального отчёта"""
        self.logger.info("")
        self.logger.info("=" * 60)
        self.logger.info("GAPS CHECK COMPLETE")
        self.logger.info("=" * 60)
        self.logger.info(f"Всего пропусков найдено: {self.total_gaps_found}")
        self.logger.info(f"Пропусков закрыто:       {self.total_gaps_closed}")
        self.logger.info(f"Осталось пропусков:      {self.total_gaps_found - self.total_gaps_closed}")
        
        if self.total_gaps_found > 0:
            pct = 100 * self.total_gaps_closed / self.total_gaps_found
            self.logger.info(f"Процент закрытия:        {pct:.1f}%")
        
        self.logger.info("=" * 60)