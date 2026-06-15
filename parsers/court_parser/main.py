"""
Точка входа парсера
"""
import sys
import os
import asyncio
from datetime import datetime

# ★ ФОРСИРУЕМ UTF-8 НА WINDOWS
if sys.platform == "win32":
    os.environ["PYTHONIOENCODING"] = "utf-8"
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except:
        pass

from core.parser import CourtParser
from core.region_worker import RegionWorker
from config.settings import Settings
from database.db_manager import DatabaseManager
from utils.logger import init_logging, get_logger
from utils.terminal_ui import init_ui, get_ui, Mode, RegionStatus, CourtStatus


def _reset_ui():
    """Сброс глобального UI для корректного вывода логов"""
    try:
        import utils.terminal_ui as terminal_ui
        if terminal_ui._ui_instance is not None:
            # Принудительно останавливаем UI
            terminal_ui._ui_instance._running = False
            terminal_ui._ui_instance = None
    except Exception:
        pass

async def parse_all_regions_from_config() -> dict:
    """Парсинг всех регионов"""
    
    settings = Settings()
    ps = settings.parsing_settings
    
    years = settings.get_parsing_years()   # ← список годов
    court_types = ps.get('court_types', ['smas', 'appellate'])
    start_from = ps.get('start_from', 1)
    max_number = ps.get('max_number', 9999)
    max_consecutive_empty = ps.get('max_consecutive_empty', 5)
    delay_between_requests = ps.get('delay_between_requests', 2)
    max_parallel_regions = ps.get('max_parallel_regions', 3)
    
    # Получаем регионы
    all_regions = settings.get_target_regions()
    limit_regions = settings.get_limit_regions()
    regions_to_process = all_regions[:limit_regions] if limit_regions else all_regions
    
    # Собираем информацию о регионах и их судах
    regions_display = {}
    region_courts = {}
    
    for key in regions_to_process:
        region_config = settings.get_region(key)
        regions_display[key] = region_config['name']
        
        # Получаем доступные суды для этого региона
        available_courts = list(region_config.get('courts', {}).keys())
        
        # Фильтруем только те, которые в court_types (или все если это особый регион)
        if available_courts:
            # Проверяем есть ли стандартные суды (smas, appellate)
            standard_courts = [c for c in court_types if c in available_courts]
            
            if standard_courts:
                # Обычный регион — используем стандартные суды
                region_courts[key] = standard_courts
            else:
                # Особый регион (Republic) — используем все его суды
                region_courts[key] = available_courts
        else:
            region_courts[key] = court_types
    
    # Инициализация UI с информацией о судах для каждого региона
    ui = init_ui(Mode.PARSE, regions_display, court_types, region_courts)
    
    # Подключение к БД
    db_manager = DatabaseManager(settings.database)
    await db_manager.connect()
    
    # Семафор для параллельности
    semaphore = asyncio.Semaphore(max_parallel_regions)
    
    # Статистика для отчёта
    report_data = {
        'no_judge': 0,
        'no_parties': 0,
    }
    
    logger = get_logger('main')
    
    try:
        await ui.start()
        
        async def process_region(region_key: str):
            async with semaphore:
                region_court_types = region_courts.get(region_key, court_types)

                # ★ Цикл по годам: текущий + хвост прошлого (в Q1)
                for year in years:
                    await process_region_with_ui(
                        region_key=region_key,
                        settings=settings,
                        db_manager=db_manager,
                        ui=ui,
                        court_types=region_court_types,
                        year=year,
                        start_from=start_from,
                        max_number=max_number,
                        max_consecutive_empty=max_consecutive_empty,
                        delay=delay_between_requests,
                        report_data=report_data,
                        logger=logger
                    )
        
        tasks = [process_region(r) for r in regions_to_process]
        await asyncio.gather(*tasks, return_exceptions=True)
        
        await ui.finish()
        ui.print_final_report(report_data)
        
    except KeyboardInterrupt:
        await ui.finish()
        print("\n⚠ Прервано пользователем")
    
    finally:
        await db_manager.disconnect()
    
    return {}

async def process_region_with_ui(
    region_key: str,
    settings: Settings,
    db_manager,
    ui,
    court_types: list,
    year: str,
    start_from: int,
    max_number: int,
    max_consecutive_empty: int,
    delay: float,
    report_data: dict,
    logger
):
    """Обработка региона"""
    
    region_config = settings.get_region(region_key)
    worker = RegionWorker(settings, region_key)
    
    try:
        if not await worker.initialize():
            logger.error(f"Failed to initialize worker for {region_key}")
            ui.region_error(region_key, "Failed to initialize")
            return
        
        ui.region_start(region_key)
        logger.info(f"Started processing region: {region_key}")
        
        region_no_judge = 0
        
        for court_key in court_types:
            court_config = region_config['courts'].get(court_key)
            if not court_config:
                continue
            
            ui.court_start(region_key, court_key)
            logger.info(f"Started court: {region_key}/{court_key}")
            
            try:
                court_stats = await parse_court(
                    worker=worker,
                    db_manager=db_manager,
                    settings=settings,
                    region_key=region_key,
                    court_key=court_key,
                    year=year,
                    start_from=start_from,
                    max_number=max_number,
                    max_consecutive_empty=max_consecutive_empty,
                    delay=delay,
                    ui=ui,
                    logger=logger
                )
                
                ui.court_done(region_key, court_key, court_stats['saved'])
                logger.info(f"Completed court: {region_key}/{court_key}, saved: {court_stats['saved']}")
                
                region_no_judge += court_stats.get('no_judge', 0)
                
            except Exception as e:
                logger.error(f"Error in court {region_key}/{court_key}: {e}")
                ui.court_error(region_key, court_key, str(e))
        
        ui.region_done(region_key)
        logger.info(f"Completed region: {region_key}")
        
        report_data['no_judge'] += region_no_judge
        
    except Exception as e:
        logger.error(f"Error in region {region_key}: {e}", exc_info=True)
        ui.region_error(region_key, str(e))
    
    finally:
        await worker.cleanup()

async def parse_court(
    worker,
    db_manager,
    settings,
    region_key: str,
    court_key: str,
    year: str,
    start_from: int,
    max_number: int,
    max_consecutive_empty: int,
    delay: float,
    ui,
    logger
) -> dict:
    """Парсинг суда"""
    
    stats = {
        'saved': 0,
        'queries': 0,
        'no_judge': 0,
        'consecutive_empty': 0,
    }
    
    # ★ Номера, по которым была ТЕХНИЧЕСКАЯ ошибка (не "дело отсутствует")
    failed_numbers: list = []
    
    def _is_technical_error(err) -> bool:
        """
        True  → запрос не подтвердил отсутствие дела (сеть/авторизация/circuit breaker/БД)
        False → сайт корректно ответил 'нет дела' (no_results / target_not_found)
                или номер некорректен (region_not_found)
        """
        if not err:
            return False
        if err in ('no_results', 'target_not_found', 'region_not_found'):
            return False  # подтверждённое отсутствие или баг номера — НЕ переспрашиваем
        return True       # сеть, авторизация, circuit breaker, save_failed
    
    # Получаем существующие номера
    existing = await db_manager.get_existing_case_numbers(
        region_key, court_key, year, settings
    )
    last_in_db = max(existing) if existing else 0
    
    current_number = last_in_db + 1 if last_in_db > 0 else start_from
    
    logger.debug(f"Starting from number {current_number} (last in DB: {last_in_db})")
    
    while current_number <= max_number:
        if stats['consecutive_empty'] >= max_consecutive_empty:
            logger.info(f"Reached {max_consecutive_empty} consecutive empty results, stopping")
            break
        
        result = await worker.search_and_save(
            db_manager=db_manager,
            court_key=court_key,
            sequence_number=current_number,
            year=year
        )
        
        stats['queries'] += 1
        ui.increment_queries(region_key)
        
        if result['success'] and result.get('saved'):
            saved_count = result.get('saved_count', 1)
            stats['saved'] += saved_count
            stats['consecutive_empty'] = 0
            ui.increment_saved(region_key, court_key, saved_count)
            
            case_numbers = result.get('case_numbers', [result.get('case_number')])
            for case_num in case_numbers:
                has_judge = result.get('has_judge', True)
                if not has_judge:
                    stats['no_judge'] += 1
                logger.info(
                    f"Saved: {case_num}",
                    extra={'region': region_key, 'court': court_key, 'case_number': case_num}
                )
        
        elif result.get('error') == 'no_results':
            # Сайт подтвердил: дела нет → реальная пустота
            stats['consecutive_empty'] += 1
            logger.debug(
                f"No results for #{current_number} (consecutive: {stats['consecutive_empty']})",
                extra={'region': region_key, 'court': court_key}
            )
        
        elif result.get('error') == 'target_not_found':
            # Сайт ответил, но целевого нет → тоже подтверждённая пустота
            stats['consecutive_empty'] += 1
            logger.debug(
                f"Target not found for #{current_number}",
                extra={'region': region_key, 'court': court_key}
            )
        
        elif _is_technical_error(result.get('error')):
            # ★ ТЕХНИЧЕСКАЯ ошибка: НЕ трогаем consecutive_empty, ЗАПОМИНАЕМ номер
            failed_numbers.append(current_number)
            logger.warning(
                f"Technical error for #{current_number}: {result.get('error')} "
                f"(will retry at end of court)",
                extra={'region': region_key, 'court': court_key}
            )
        
        current_number += 1
        await asyncio.sleep(delay)
    
    # =========================================================================
    # ★ ФИНАЛЬНЫЙ ПЕРЕСПРОС сбойных номеров (Уровень 2)
    # Сеть могла восстановиться за время прохода суда
    # =========================================================================
    if failed_numbers:
        logger.info(
            f"Retrying {len(failed_numbers)} failed numbers for {region_key}/{court_key}/{year}"
        )
        still_failed = []
        
        for num in failed_numbers:
            result = await worker.search_and_save(
                db_manager=db_manager,
                court_key=court_key,
                sequence_number=num,
                year=year
            )
            stats['queries'] += 1
            ui.increment_queries(region_key)
            
            if result['success'] and result.get('saved'):
                saved_count = result.get('saved_count', 1)
                stats['saved'] += saved_count
                ui.increment_saved(region_key, court_key, saved_count)
                logger.info(f"Retry success: #{num} saved")
            elif _is_technical_error(result.get('error')):
                still_failed.append(num)
            # no_results / target_not_found на ретрае → дело реально отсутствует, ОК
            
            await asyncio.sleep(delay)
        
        if still_failed:
            # Уровень 3: оставляем для GAPS следующей сессии.
            # Сбрасываем дату проверки этого суда → GAPS обязан перепроверить.
            logger.warning(
                f"{len(still_failed)} numbers still failed after retry "
                f"for {region_key}/{court_key}/{year}: {still_failed}. "
                f"Resetting gaps date for re-check next session."
            )
            await db_manager.reset_gaps_check_date(region_key, court_key, year)
    
    return stats


# === UPDATE РЕЖИМЫ ===

async def run_update_judge():
    """Режим обновления судей"""
    settings = Settings()
    db_manager = DatabaseManager(settings.database)
    await db_manager.connect()
    logger = get_logger('main')
    
    try:
        judge_config = settings.update_settings.get('judge', {})
        cases = await db_manager.get_smas_cases_without_judge(
            settings,
            interval_days=judge_config.get('check_interval_days', 1),
            final_event_types=judge_config.get('final_event_types', []),
            max_stale_days=judge_config.get('max_stale_days'),
        )
        
        if not cases:
            logger.info("Нет дел для обновления")
            return
        
        from utils.text_processor import TextProcessor
        tp = TextProcessor()
        
        grouped = {}
        for case_number in cases:
            info = tp.find_region_and_court_by_case_number(case_number, settings.regions)
            if info:
                grouped.setdefault(info['region_key'], []).append(case_number)
        
        regions_display = {k: settings.get_region(k)['name'] for k in grouped.keys()}
        ui = init_ui(Mode.JUDGE, regions_display, court_types=[])
        
        for region_key, region_cases in grouped.items():
            region = ui.regions.get(region_key)
            if region:
                region.total_cases = len(region_cases)
        
        await ui.start()
        
        semaphore = asyncio.Semaphore(3)
        
        async def process_region(region_key: str, region_cases: list):
            async with semaphore:
                worker = RegionWorker(settings, region_key)
                try:
                    if not await worker.initialize():
                        ui.region_error(region_key, "Init failed")
                        return
                    
                    ui.region_start(region_key)
                    
                    processed = 0
                    found = 0
                    
                    for case_number in region_cases:
                        _, cases_found = await worker.search_case_by_number(case_number)
                        
                        processed += 1
                        
                        target = next(
                            (c for c in cases_found if tp.is_matching_case_number(c.case_number, case_number)),
                            None
                        )
                        
                        if target and target.judge:
                            await db_manager.update_case(target)
                            found += 1
                            logger.info(f"Judge found for {case_number}: {target.judge}")
                        
                        await db_manager.mark_case_as_updated(case_number)
                        
                        ui.update_progress(region_key, processed=processed, found=found)
                        
                        await asyncio.sleep(2)
                    
                    ui.region_done(region_key)
                    
                except Exception as e:
                    logger.error(f"Error in {region_key}: {e}")
                    ui.region_error(region_key, str(e))
                finally:
                    await worker.cleanup()
        
        tasks = [process_region(k, v) for k, v in grouped.items()]
        await asyncio.gather(*tasks, return_exceptions=True)
        
        await ui.finish()
        ui.print_final_report()
        
    finally:
        await db_manager.disconnect()


async def run_update_events():
    """Режим обновления событий"""
    settings = Settings()
    db_manager = DatabaseManager(settings.database)
    await db_manager.connect()
    logger = get_logger('main')
    
    try:
        config = settings.update_settings.get('case_events', {})
        filters = config.get('filters', {})

        cases = await db_manager.get_cases_for_update({
            'defendant_keywords': filters.get('party_keywords', []),
            'exclude_event_types': filters.get('exclude_event_types', []),
            'update_interval_days': config.get('check_interval_days', 2),
            # НОВОЕ:
            'final_event_types': config.get('final_event_types', []),
            'final_check_period_days': config.get('final_check_period_days', 30),
            'max_stale_days': config.get('max_stale_days'),
        })
        
        if not cases:
            logger.info("Нет дел для обновления")
            return
        
        from utils.text_processor import TextProcessor
        tp = TextProcessor()
        
        grouped = {}
        for case_number in cases:
            info = tp.find_region_and_court_by_case_number(case_number, settings.regions)
            if info:
                grouped.setdefault(info['region_key'], []).append(case_number)
        
        regions_display = {k: settings.get_region(k)['name'] for k in grouped.keys()}
        ui = init_ui(Mode.EVENTS, regions_display, court_types=[])
        
        for region_key, region_cases in grouped.items():
            region = ui.regions.get(region_key)
            if region:
                region.total_cases = len(region_cases)
        
        await ui.start()
        
        semaphore = asyncio.Semaphore(3)
        
        async def process_region(region_key: str, region_cases: list):
            async with semaphore:
                worker = RegionWorker(settings, region_key)
                try:
                    if not await worker.initialize():
                        ui.region_error(region_key, "Init failed")
                        return
                    
                    ui.region_start(region_key)
                    
                    processed = 0
                    events_total = 0
                    
                    for case_number in region_cases:
                        _, cases_found = await worker.search_case_by_number(case_number)
                        
                        processed += 1
                        
                        target = next(
                            (c for c in cases_found if tp.is_matching_case_number(c.case_number, case_number)),
                            None
                        )
                        
                        events_added = 0
                        if target:
                            result = await db_manager.update_case(target)
                            events_added = result.get('events_added', 0)
                            events_total += events_added
                            if events_added > 0:
                                logger.info(f"Added {events_added} events for {case_number}")
                        
                        await db_manager.mark_case_as_updated(case_number)
                        
                        ui.update_progress(region_key, processed=processed, events=events_total)
                        
                        await asyncio.sleep(2)
                    
                    ui.region_done(region_key)
                    
                except Exception as e:
                    logger.error(f"Error in {region_key}: {e}")
                    ui.region_error(region_key, str(e))
                finally:
                    await worker.cleanup()
        
        tasks = [process_region(k, v) for k, v in grouped.items()]
        await asyncio.gather(*tasks, return_exceptions=True)
        
        await ui.finish()
        ui.print_final_report()
        
    finally:
        await db_manager.disconnect()


async def run_update_docs():
    """Режим скачивания документов"""
    settings = Settings()
    db_manager = DatabaseManager(settings.database)
    await db_manager.connect()
    logger = get_logger('main')
    
    try:
        config = settings.update_settings.get('docs', {})
        filters = config.get('filters', {})
        final_post_check_delay_days = config.get('final_post_check_delay_days', 10)
        final_event_types = config.get('final_event_types', [])
        max_attempts = config.get('documents_max_attempts', 5)

        cases = await db_manager.get_cases_for_documents(
            filters={
                'party_keywords': filters.get('party_keywords', []),
                'party_role': filters.get('party_role'),
                'court_types': filters.get('court_types'),
                'regions': filters.get('regions'),
                'year': filters.get('year'),
                'check_interval_days': config.get('check_interval_days', 5),
                'final_post_check_delay_days': final_post_check_delay_days,
                'order': filters.get('order', 'oldest'),
            },
            limit=config.get('max_per_session'),
            final_event_types=final_event_types,
            max_attempts=max_attempts
        )
        
        if not cases:
            logger.info("Нет дел для обработки")
            return
        
        from utils.text_processor import TextProcessor
        from search.document_handler import DocumentHandler
        
        tp = TextProcessor()
        doc_handler = DocumentHandler(
            settings.base_url,
            config.get('storage_dir', './documents'),
            settings.regions
        )
        
        grouped = {}
        for case in cases:
            info = tp.find_region_and_court_by_case_number(case['case_number'], settings.regions)
            if info:
                grouped.setdefault(info['region_key'], []).append(case)
        
        regions_display = {k: settings.get_region(k)['name'] for k in grouped.keys()}
        ui = init_ui(Mode.DOCS, regions_display, court_types=[])
        
        for region_key, region_cases in grouped.items():
            region = ui.regions.get(region_key)
            if region:
                region.total_cases = len(region_cases)
        
        await ui.start()
        
        semaphore = asyncio.Semaphore(3)
        
        async def process_region(region_key: str, region_cases: list):
            async with semaphore:
                worker = RegionWorker(settings, region_key)
                try:
                    if not await worker.initialize():
                        ui.region_error(region_key, "Init failed")
                        return
                    
                    ui.region_start(region_key)
                    
                    processed = 0
                    docs_total = 0
                    
                    for case in region_cases:
                        case_number = case['case_number']
                        case_id = case['id']
                        
                        results_html, cases_found = await worker.search_case_by_number(case_number)
                        processed += 1
                        
                        if not results_html or not cases_found:
                            await db_manager.finalize_document_check(
                                case_id=case_id,
                                final_event_types=final_event_types,
                                final_post_check_delay_days=final_post_check_delay_days,
                                made_progress=False,
                                max_attempts=max_attempts
                            )
                            continue
                        
                        target = next(
                            (c for c in cases_found if tp.is_matching_case_number(c.case_number, case_number)),
                            None
                        )
                        
                        if not target or target.result_index is None:
                            await db_manager.finalize_document_check(
                                case_id=case_id,
                                final_event_types=final_event_types,
                                final_post_check_delay_days=final_post_check_delay_days,
                                made_progress=False,
                                max_attempts=max_attempts
                            )
                            continue
                        
                        # Обновляем события дела (если найдены новые статусы/события)
                        await db_manager.update_case(target)
                        
                        existing_keys = await db_manager.get_document_keys(case_id)

                        fetch = await doc_handler.fetch_all_documents(
                            session=worker.session,
                            results_html=results_html,
                            case_number=case_number,
                            case_index=target.result_index,
                            existing_keys=existing_keys,
                            delay=config.get('download_delay', 0)
                        )

                        downloaded = fetch['downloaded']
                        if downloaded:
                            await db_manager.save_documents(case_id, downloaded)
                            docs_total += len(downloaded)
                            logger.info(f"Downloaded {len(downloaded)} docs for {case_number}")

                        # Вызов централизованного планировщика жизненного цикла документа
                        await db_manager.finalize_document_check(
                            case_id=case_id,
                            final_event_types=final_event_types,
                            final_post_check_delay_days=final_post_check_delay_days,
                            made_progress=fetch['made_progress'],
                            max_attempts=max_attempts
                        )
                        
                        ui.update_progress(region_key, processed=processed, docs=docs_total)
                        
                        await asyncio.sleep(2)
                    
                    ui.region_done(region_key)
                    
                except Exception as e:
                    logger.error(f"Error in {region_key}: {e}")
                    ui.region_error(region_key, str(e))
                finally:
                    await worker.cleanup()
        
        tasks = [process_region(k, v) for k, v in grouped.items()]
        await asyncio.gather(*tasks, return_exceptions=True)
        
        await ui.finish()
        ui.print_final_report()
        
    finally:
        await db_manager.disconnect()


async def run_gaps_check():
    """Режим проверки пропусков"""
    settings = Settings()
    db_manager = DatabaseManager(settings.database)
    await db_manager.connect()
    
    try:
        from core.updaters.gaps_updater import GapsUpdater
        
        gaps_updater = GapsUpdater(settings, db_manager)
        result = await gaps_updater.run()
        
        return result
        
    finally:
        await db_manager.disconnect()
    

async def run_pipeline(): 
    """
    Режим полного пайплайна: gaps → parse → events → docs
    """
    logger = get_logger('pipeline')
    
    logger.info("=" * 60)
    logger.info("PIPELINE START: gaps → parse → events → docs")
    logger.info("=" * 60)
    
    pipeline_start = datetime.now()

    # ★ Фиксируем годы один раз на весь pipeline (защита от запуска в полночь смены года)
    _settings_init = Settings()
    frozen_years = _settings_init.get_parsing_years()
    logger.info(f"Pipeline years: {frozen_years}")

    results = {
        'gaps': {'found': 0, 'closed': 0},
        'parse': {'saved': 0},
        'judge': {'processed': 0, 'found': 0},
        'events': {'processed': 0, 'events_added': 0},
        'docs': {'processed': 0, 'docs_downloaded': 0},
    }
    
    # =========================================================================
    # ЭТАП 0: GAPS
    # =========================================================================
    _reset_ui()
    logger.info("")
    logger.info("-" * 60)
    logger.info("STAGE 0: CHECKING GAPS")
    logger.info("-" * 60)
    
    try:
        settings = Settings()
        db_manager = DatabaseManager(settings.database)
        await db_manager.connect()
        
        try:
            from core.updaters.gaps_updater import GapsUpdater
            gaps_updater = GapsUpdater(settings, db_manager)
            gaps_result = await gaps_updater.run()
            
            results['gaps']['found'] = gaps_result.get('total_gaps', 0)
            results['gaps']['closed'] = gaps_result.get('closed', 0)
        finally:
            await db_manager.disconnect()
    except Exception as e:
        logger.error(f"Gaps failed: {e}", exc_info=True)
    finally:
        _reset_ui()
    
    # =========================================================================
    # ЭТАП 1: PARSE
    # =========================================================================
    _reset_ui()
    logger.info("")
    logger.info("-" * 60)
    logger.info("STAGE 1: PARSING NEW CASES")
    logger.info("-" * 60)
    
    try:
        await parse_all_regions_from_config()
        
        ui = get_ui()
        if ui:
            results['parse']['saved'] = ui.stats.total_saved
    except Exception as e:
        logger.error(f"Parse failed: {e}", exc_info=True)
    finally:
        _reset_ui()
    
    # =========================================================================
    # ПОДСЧЁТ ДЕЛ ДЛЯ СЛЕДУЮЩИХ ЭТАПОВ
    # =========================================================================
    settings = Settings()
    db_manager = DatabaseManager(settings.database)
    await db_manager.connect()
    
    try:
        # Подсчёт дел для events
        events_config = settings.update_settings.get('case_events', {})
        events_filters = events_config.get('filters', {})
        events_cases = await db_manager.get_cases_for_update({
            'defendant_keywords': events_filters.get('party_keywords', []),
            'exclude_event_types': events_filters.get('exclude_event_types', []),
            'update_interval_days': events_config.get('check_interval_days', 2),
            # НОВОЕ:
            'final_event_types': events_config.get('final_event_types', []),
            'final_check_period_days': events_config.get('final_check_period_days', 30),
            'max_stale_days': events_config.get('max_stale_days'),
        })
        
        # Подсчёт дел для docs
        docs_config = settings.update_settings.get('docs', {})
        docs_filters = docs_config.get('filters', {})
        docs_cases = await db_manager.get_cases_for_documents(
            filters={
                'party_keywords': docs_filters.get('party_keywords', []),
                'party_role': docs_filters.get('party_role'),
                'court_types': docs_filters.get('court_types'),
                'regions': docs_filters.get('regions'),
                'year': docs_filters.get('year'),
                'check_interval_days': docs_config.get('check_interval_days', 5),
                'final_post_check_delay_days': docs_config.get('final_post_check_delay_days', 10),
                'order': docs_filters.get('order', 'oldest'),
            },
            limit=docs_config.get('max_per_session'),
            final_event_types=docs_config.get('final_event_types', []),
            max_attempts=docs_config.get('documents_max_attempts', 5)
        )
        
        logger.info("")
        logger.info("=" * 60)
        logger.info(f"PENDING: Events={len(events_cases)} cases, Docs={len(docs_cases)} cases")
        logger.info("=" * 60)
        
    finally:
        await db_manager.disconnect()
    
    # =========================================================================
    # ЭТАП 2: JUDGE
    # =========================================================================
    _reset_ui()
    logger.info("")
    logger.info("-" * 60)
    logger.info("STAGE 2: UPDATING JUDGES")
    logger.info("-" * 60)

    try:
        settings = Settings()
        db_manager = DatabaseManager(settings.database)
        await db_manager.connect()
        
        try:
            from core.updaters.judge_updater import JudgeUpdater
            judge_updater = JudgeUpdater(settings, db_manager)
            judge_result = await judge_updater.run()
            
            results['judge'] = {
                'processed': judge_result.get('processed', 0),
                'found': judge_updater.stats.get('judges_found', 0)
            }
        finally:
            await db_manager.disconnect()
    except Exception as e:
        logger.error(f"Judge failed: {e}", exc_info=True)
    finally:
        _reset_ui()


    # =========================================================================
    # ЭТАП 3: EVENTS
    # =========================================================================
    _reset_ui()
    logger.info("")
    logger.info("-" * 60)
    logger.info("STAGE 3: UPDATING EVENTS")
    logger.info("-" * 60)
    
    try:
        settings = Settings()
        db_manager = DatabaseManager(settings.database)
        await db_manager.connect()
        
        try:
            from core.updaters.events_updater import EventsUpdater
            events_updater = EventsUpdater(settings, db_manager)
            events_result = await events_updater.run()
            
            results['events']['processed'] = events_result.get('processed', 0)
            results['events']['events_added'] = events_updater.stats.get('events_added', 0)
        finally:
            await db_manager.disconnect()
    except Exception as e:
        logger.error(f"Events failed: {e}", exc_info=True)
    finally:
        _reset_ui()
    
    # =========================================================================
    # ЭТАП 4: DOCS
    # =========================================================================
    _reset_ui()
    logger.info("")
    logger.info("-" * 60)
    logger.info("STAGE 4: DOWNLOADING DOCUMENTS")
    logger.info("-" * 60)
    
    try:
        settings = Settings()
        db_manager = DatabaseManager(settings.database)
        await db_manager.connect()
        
        try:
            from core.updaters.docs_updater import DocsUpdater
            docs_updater = DocsUpdater(settings, db_manager)
            docs_result = await docs_updater.run()
            
            results['docs']['processed'] = docs_result.get('processed', 0)
            results['docs']['docs_downloaded'] = docs_updater.stats.get('docs_downloaded', 0)
        finally:
            await db_manager.disconnect()
    except Exception as e:
        logger.error(f"Docs failed: {e}", exc_info=True)
    finally:
        _reset_ui()
    
    # =========================================================================
    # ФИНАЛЬНЫЙ ОТЧЁТ
    # =========================================================================
    pipeline_elapsed = datetime.now() - pipeline_start
    total_minutes, total_seconds = divmod(int(pipeline_elapsed.total_seconds()), 60)
    
    logger.info("")
    logger.info("=" * 60)
    logger.info("PIPELINE COMPLETE")
    logger.info("=" * 60)
    logger.info(f"Total time: {total_minutes}:{total_seconds:02d}")
    logger.info(f"Gaps:   {results['gaps']['closed']}/{results['gaps']['found']} closed")
    logger.info(f"Parse:  {results['parse']['saved']} cases")
    logger.info(f"Judge:  {results['judge']['found']}/{results['judge']['processed']} found")
    logger.info(f"Events: {results['events']['events_added']} added")
    logger.info(f"Docs:   {results['docs']['docs_downloaded']} downloaded")
    logger.info("=" * 60)
    
    return results


def main():
    """Главная функция"""
    init_logging(log_dir="logs", level="DEBUG")
    
    mode = 'parse'
    submode = None
    
    if '--mode' in sys.argv:
        idx = sys.argv.index('--mode')
        if idx + 1 < len(sys.argv):
            mode = sys.argv[idx + 1]
        if idx + 2 < len(sys.argv) and not sys.argv[idx + 2].startswith('-'):
            submode = sys.argv[idx + 2]
    
    logger = get_logger('main')
    
    try:
        if mode == 'parse':
            asyncio.run(parse_all_regions_from_config())
        
        elif mode == 'gaps':
            asyncio.run(run_gaps_check())
        
        elif mode == 'pipeline':
            asyncio.run(run_pipeline())
        
        elif mode == 'update':
            if submode == 'judge':
                asyncio.run(run_update_judge())
            elif submode == 'case_events':
                asyncio.run(run_update_events())
            elif submode == 'docs':
                asyncio.run(run_update_docs())
            else:
                logger.error("Usage: --mode update [judge|case_events|docs]")
                sys.exit(1)
        else:
            logger.error("Usage: --mode [parse|gaps|pipeline|update]")
            sys.exit(1)
            
    except KeyboardInterrupt:
        logger.warning("Interrupted by user")
        sys.exit(0)


if __name__ == '__main__':
    main()