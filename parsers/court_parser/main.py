"""
Точка входа парсера
"""
import sys
import asyncio
import traceback
from typing import List, Optional, Dict

from core.parser import CourtParser
from core.region_worker import RegionWorker
from config.settings import Settings
from database.db_manager import DatabaseManager
from search.document_handler import DocumentHandler
from utils.logger import setup_logger, get_logger, init_logging
from utils.progress import ProgressDisplay
from datetime import datetime
from utils.stats_reporter import StatsReporter


async def parse_all_regions_from_config() -> dict:
    """Парсинг всех регионов с изолированными воркерами"""
    logger = get_logger('main')
    
    settings = Settings()
    ps = settings.parsing_settings
    
    year = ps.get('year', '2025')
    court_types = ps.get('court_types', ['smas'])
    start_from = ps.get('start_from', 1)
    max_number = ps.get('max_number', 9999)
    max_consecutive_empty = ps.get('max_consecutive_empty', 200)
    delay_between_requests = ps.get('delay_between_requests', 2)
    max_parallel_regions = ps.get('max_parallel_regions', 1)
    
    region_retry_max_attempts = ps.get('region_retry_max_attempts', 3)
    region_retry_delay = ps.get('region_retry_delay_seconds', 5)
    
    limit_regions = settings.get_limit_regions()
    limit_cases_per_region = settings.get_limit_cases_per_region()
    
    all_regions = settings.get_target_regions()
    
    if limit_regions:
        regions_to_process = all_regions[:limit_regions]
    else:
        regions_to_process = all_regions
    
    # Статистика сессии
    session_stats = {
        'start_time': datetime.now(),
        'end_time': None,
        'year': year,
        'regions_total': len(regions_to_process),
        'regions_processed': 0,
        'regions_failed': 0,
        'total_queries': 0,
        'total_cases_saved': 0,
        'gaps_filled': 0,
        'regions': {}
    }
    
    stats_lock = asyncio.Lock()
    semaphore = asyncio.Semaphore(max_parallel_regions)
    
    regions_display = {
        key: settings.get_region (key)['name'] 
        for key in regions_to_process
    }
    progress = ProgressDisplay(regions_display, court_types)
    
    db_manager = DatabaseManager(settings.database)
    await db_manager.connect()
    
    # Инициализация репортера статистики
    stats_reporter = StatsReporter(db_manager, settings)
    
    try:
        # === НАЧАЛЬНЫЙ ОТЧЁТ ===
        plan = {
            'mode': 'parse',
            'year': year,
            'court_types': court_types,
            'target_regions': regions_to_process,
            'max_consecutive_empty': max_consecutive_empty,
        }
        await stats_reporter.print_start_report(plan)
        
        print()  # Пустая строка перед прогрессом
        await progress.start()
        
        async def process_region_with_retry(region_key: str):
            async with semaphore:
                region_config = settings.get_region(region_key)
                region_session = {}
                
                for attempt in range(1, region_retry_max_attempts + 1):
                    worker = None
                    try:
                        worker = RegionWorker(settings, region_key)
                        
                        if not await worker.initialize():
                            raise Exception("Не удалось инициализировать воркер")
                        
                        logger.info(f"Регион {region_config['name']}: старт (попытка {attempt})")
                        
                        region_stats = await process_region_all_courts_with_worker(
                            worker=worker,
                            db_manager=db_manager,
                            settings=settings,
                            region_key=region_key,
                            court_types=court_types,
                            year=year,
                            start_from=start_from,
                            max_number=max_number,
                            max_consecutive_empty=max_consecutive_empty,
                            delay_between_requests=delay_between_requests,
                            limit_cases=limit_cases_per_region,
                            progress=progress,
                            logger=logger,
                            region_session=region_session
                        )
                        
                        await progress.set_region_done(region_key)
                        
                        async with stats_lock:
                            session_stats['regions_processed'] += 1
                            session_stats['total_queries'] += region_stats['total_queries']
                            session_stats['total_cases_saved'] += region_stats['total_cases_saved']
                            session_stats['regions'][region_key] = region_session
                        
                        logger.info(
                            f"Регион {region_config['name']}: завершён | "
                            f"запросов: {region_stats['total_queries']} | "
                            f"сохранено: {region_stats['total_cases_saved']}"
                        )
                        
                        return region_stats
                        
                    except Exception as e:
                        logger.error(f"Регион {region_config['name']}: ошибка - {e}")
                        
                        # Помечаем ошибку в статистике
                        for court_key in court_types:
                            if court_key not in region_session:
                                region_session[court_key] = {
                                    'queries': 0,
                                    'saved': 0,
                                    'time': '',
                                    'stop_reason': 'error',
                                    'consecutive_empty': 0
                                }
                            else:
                                region_session[court_key]['stop_reason'] = 'error'
                        
                        if attempt < region_retry_max_attempts:
                            await asyncio.sleep(region_retry_delay)
                        else:
                            await progress.set_region_error(region_key)
                            async with stats_lock:
                                session_stats['regions_failed'] += 1
                                session_stats['regions'][region_key] = region_session
                            return None
                            
                    finally:
                        if worker:
                            await worker.cleanup()
        
        tasks = [process_region_with_retry(r) for r in regions_to_process]
        await asyncio.gather(*tasks, return_exceptions=True)
        
        await progress.finish()
        
        session_stats['end_time'] = datetime.now()
        
        # === ФИНАЛЬНЫЙ ОТЧЁТ ===
        print()  # Пустая строка после прогресса
        await stats_reporter.print_end_report(session_stats)
        
    except KeyboardInterrupt:
        session_stats['end_time'] = datetime.now()
        logger.warning("Прервано пользователем")
        
        # Помечаем все активные регионы как остановленные вручную
        for region_key in regions_to_process:
            if region_key in session_stats['regions']:
                for court_key in session_stats['regions'][region_key]:
                    if not session_stats['regions'][region_key][court_key].get('stop_reason'):
                        session_stats['regions'][region_key][court_key]['stop_reason'] = 'manual'
        
        await stats_reporter.print_end_report(session_stats)
        
    finally:
        await db_manager.disconnect()
    
    return session_stats


async def process_region_all_courts_with_worker(
    worker: RegionWorker,
    db_manager,
    settings: Settings,
    region_key: str,
    court_types: List[str],
    year: str,
    start_from: int,
    max_number: int,
    max_consecutive_empty: int,
    delay_between_requests: float,
    limit_cases: Optional[int],
    progress: ProgressDisplay,
    logger,
    region_session: Dict  # Новый параметр для сбора статистики
) -> dict:
    """Обработка всех судов региона через воркер"""
    region_config = settings.get_region(region_key)
    
    region_stats = {
        'region_key': region_key,
        'courts_processed': 0,
        'total_queries': 0,
        'total_cases_saved': 0,
        'courts_stats': {}
    }
    
    for court_key in court_types:
        court_config = region_config['courts'].get(court_key)
        if not court_config:
            logger.warning(f"Суд {court_key} не найден в регионе {region_key}")
            continue
        
        logger.info(f"Регион {region_key}, суд {court_key}: старт")
        
        await progress.update(
            region_key, 
            court=court_key, 
            saved=0, 
            queries=0, 
            consecutive_empty=0
        )
        
        court_start_time = datetime.now()
        
        try:
            court_stats = await parse_court_with_worker(
                worker=worker,
                db_manager=db_manager,
                settings=settings,
                region_key=region_key,
                court_key=court_key,
                year=year,
                start_from=start_from,
                max_number=max_number,
                max_consecutive_empty=max_consecutive_empty,
                delay_between_requests=delay_between_requests,
                limit_cases=limit_cases,
                progress=progress,
                logger=logger
            )
            
            await progress.set_court_done(region_key, court_key)
            
            court_end_time = datetime.now()
            court_duration = court_end_time - court_start_time
            minutes, seconds = divmod(int(court_duration.total_seconds()), 60)
            
            # Определяем причину остановки
            if court_stats['consecutive_empty'] >= max_consecutive_empty:
                stop_reason = 'empty_limit'
            elif limit_cases and court_stats['queries_made'] >= limit_cases:
                stop_reason = 'query_limit'
            else:
                stop_reason = 'completed'
            
            # Сохраняем статистику суда для отчёта
            region_session[court_key] = {
                'queries': court_stats['queries_made'],
                'saved': court_stats['cases_saved'],
                'time': f"{minutes}:{seconds:02d}",
                'stop_reason': stop_reason,
                'consecutive_empty': court_stats['consecutive_empty']
            }
            
            region_stats['courts_processed'] += 1
            region_stats['total_queries'] += court_stats['queries_made']
            region_stats['total_cases_saved'] += court_stats['cases_saved']
            region_stats['courts_stats'][court_key] = court_stats
            
            logger.info(
                f"Регион {region_key}, суд {court_key}: завершён | "
                f"запросов: {court_stats['queries_made']} | "
                f"сохранено: {court_stats['cases_saved']}"
            )
            
        except Exception as e:
            logger.error(f"Ошибка суда {court_key} в регионе {region_key}: {e}")
            
            court_end_time = datetime.now()
            court_duration = court_end_time - court_start_time
            minutes, seconds = divmod(int(court_duration.total_seconds()), 60)
            
            region_session[court_key] = {
                'queries': 0,
                'saved': 0,
                'time': f"{minutes}:{seconds:02d}",
                'stop_reason': 'error',
                'consecutive_empty': 0
            }
            continue
    
    return region_stats


async def parse_court_with_worker(
    worker: RegionWorker,
    db_manager,
    settings: Settings,
    region_key: str,
    court_key: str,
    year: str,
    start_from: int,
    max_number: int,
    max_consecutive_empty: int,
    delay_between_requests: float,
    limit_cases: Optional[int],
    progress: ProgressDisplay,
    logger
) -> dict:
    """Парсинг одного суда через воркер"""
    
    stats = {
        'missing_found': 0,
        'missing_filled': 0,
        'missing_not_found': 0,
        'new_queries': 0,
        'new_saved': 0,
        'consecutive_empty': 0
    }
    
    existing = await db_manager.get_existing_case_numbers(
        region_key, court_key, year, settings
    )
    
    last_in_db = await db_manager.get_last_sequence_number(
        region_key, court_key, year, settings
    )
    
    logger.info(f"{region_key}/{court_key}: существует {len(existing)}, последний #{last_in_db}")
    
    total_saved = 0
    total_queries = 0
    
    if last_in_db > 0:
        full_range = set(range(start_from, last_in_db + 1))
        missing = sorted(full_range - existing)
        stats['missing_found'] = len(missing)
        
        if missing:
            logger.info(f"{region_key}/{court_key}: пропусков {len(missing)}")
            
            for seq_num in missing:
                if limit_cases and total_queries >= limit_cases:
                    break
                
                result = await worker.search_and_save(
                    db_manager=db_manager,
                    court_key=court_key,
                    sequence_number=seq_num,
                    year=year
                )
                
                total_queries += 1
                
                if result['success'] and result.get('saved'):
                    stats['missing_filled'] += 1
                    total_saved += 1
                else:
                    stats['missing_not_found'] += 1
                
                await progress.update(
                    region_key,
                    court=court_key,
                    saved=total_saved,
                    queries=total_queries,
                    consecutive_empty=stats['consecutive_empty']
                )
                
                await asyncio.sleep(delay_between_requests)
    
    actual_start = last_in_db + 1 if last_in_db > 0 else start_from
    
    if actual_start <= max_number:
        logger.info(f"{region_key}/{court_key}: новые дела с #{actual_start}")
        
        current_number = actual_start
        
        while current_number <= max_number:
            if limit_cases and total_queries >= limit_cases:
                logger.info(f"{region_key}/{court_key}: лимит запросов {limit_cases}")
                break
            
            if stats['consecutive_empty'] >= max_consecutive_empty:
                logger.info(f"{region_key}/{court_key}: лимит пустых {max_consecutive_empty}")
                break
            
            result = await worker.search_and_save(
                db_manager=db_manager,
                court_key=court_key,
                sequence_number=current_number,
                year=year
            )
            
            stats['new_queries'] += 1
            total_queries += 1
            
            if result['success'] and result.get('saved'):
                stats['new_saved'] += 1
                total_saved += 1
                stats['consecutive_empty'] = 0
            elif result.get('error') == 'no_results':
                stats['consecutive_empty'] += 1
            elif result.get('error') == 'target_not_found' and court_key != 'smas':
                stats['consecutive_empty'] += 1
            
            await progress.update(
                region_key,
                court=court_key,
                saved=total_saved,
                queries=total_queries,
                consecutive_empty=stats['consecutive_empty']
            )
            
            current_number += 1
            await asyncio.sleep(delay_between_requests)
    
    return {
        'queries_made': total_queries,
        'cases_saved': total_saved,
        'consecutive_empty': stats['consecutive_empty'],
        'missing_filled': stats['missing_filled'],
        'new_saved': stats['new_saved']
    }


async def update_cases_history():
    """Режим обновления: события + документы"""
    logger = get_logger('main')
    
    settings = Settings()
    update_config = settings.config.get('update_settings', {})
    docs_config = settings.config.get('documents_settings', {})
    
    if not update_config.get('enabled', True):
        logger.warning("Update Mode отключен в настройках")
        return
    
    interval_days = update_config.get('update_interval_days', 2)
    filters = update_config.get('filters', {})
    
    storage_dir = docs_config.get('storage_dir', './documents')
    download_delay = docs_config.get('download_delay', 2.0)
    
    logger.info("=" * 70)
    logger.info("РЕЖИМ ОБНОВЛЕНИЯ: СОБЫТИЯ + ДОКУМЕНТЫ")
    logger.info("=" * 70)
    logger.info(f"Интервал: {interval_days} дней")
    if filters.get('defendant_keywords'):
        logger.info(f"Фильтр по ответчику: {filters['defendant_keywords']}")
    if filters.get('exclude_event_types'):
        logger.info(f"Исключить события: {filters['exclude_event_types']}")
    
    stats = {
        'cases_updated': 0,
        'events_added': 0,
        'documents_downloaded': 0,
        'errors': 0,
        'skipped': 0
    }
    
    async with CourtParser() as parser:
        doc_handler = DocumentHandler(
            base_url=settings.base_url,
            storage_dir=storage_dir,
            regions_config=settings.regions
        )
        
        logger.info("Этап 1: Дела СМАС без судьи")
        
        smas_cases = await parser.db_manager.get_smas_cases_without_judge(settings, interval_days)
        logger.info(f"Найдено дел СМАС без судьи: {len(smas_cases)}")
        
        for case_number in smas_cases:
            result = await _process_single_case(
                parser, doc_handler, case_number, download_delay, logger
            )
            _update_stats(stats, result)
        
        logger.info("Этап 2: Дела по ключевым словам")
        
        keyword_cases = await parser.db_manager.get_cases_for_update({
            'defendant_keywords': filters.get('defendant_keywords', []),
            'exclude_event_types': filters.get('exclude_event_types', []),
            'update_interval_days': interval_days
        })
        logger.info(f"Найдено дел по ключевым словам: {len(keyword_cases)}")
        
        for case_number in keyword_cases:
            result = await _process_single_case(
                parser, doc_handler, case_number, download_delay, logger
            )
            _update_stats(stats, result)
    
    logger.info("=" * 70)
    logger.info("ИТОГИ ОБНОВЛЕНИЯ:")
    logger.info(f"  Обновлено дел: {stats['cases_updated']}")
    logger.info(f"  Добавлено событий: {stats['events_added']}")
    logger.info(f"  Скачано документов: {stats['documents_downloaded']}")
    logger.info(f"  Пропущено: {stats['skipped']}")
    logger.info(f"  Ошибок: {stats['errors']}")
    logger.info("=" * 70)


async def _process_single_case(parser, doc_handler, case_number: str, delay: float, logger) -> dict:
    """Обработка одного дела: обновление событий + документы"""
    result = {'updated': False, 'events_added': 0, 'documents': 0, 'error': False, 'skipped': False}
    
    try:
        logger.info(f"Обработка дела: {case_number}")
        
        results_html, cases = await parser.search_case_by_number(case_number)
        
        if not results_html or not cases:
            logger.warning(f"Дело не найдено на сайте: {case_number}")
            result['skipped'] = True
            return result
        
        target_case = next((c for c in cases if c.case_number == case_number), None)
        
        if not target_case or target_case.result_index is None:
            logger.warning(f"Индекс не определён: {case_number}")
            result['skipped'] = True
            return result
        
        save_result = await parser.db_manager.update_case(target_case)
        
        if save_result.get('events_added', 0) > 0:
            result['events_added'] = save_result['events_added'] 
            result['updated'] = True
            logger.info(f"Добавлено событий: {result['events_added']} для {case_number}")
        
        case_id = save_result.get('case_id') or await parser.db_manager.get_case_id(case_number)
        
        if case_id:
            session = await parser.session_manager.get_session()
            existing_keys = await parser.db_manager.get_document_keys(case_id)
            
            downloaded = await doc_handler.fetch_all_documents(
                session=session,
                results_html=results_html,
                case_number=case_number,
                case_index=target_case.result_index,
                existing_keys=existing_keys,
                delay=delay
            )
            
            if downloaded:
                await parser.db_manager.save_documents(case_id, downloaded)
                result['documents'] = len(downloaded)
                result['updated'] = True
                logger.info(f"Скачано документов: {len(downloaded)} для {case_number}")
            
            await parser.db_manager.mark_documents_downloaded(case_id)
        
        await asyncio.sleep(delay)
        
    except Exception as e:
        logger.error(f"Ошибка обработки дела {case_number}: {e}")
        result['error'] = True
    
    return result


def _update_stats(stats: dict, result: dict):
    """Обновить общую статистику"""
    if result.get('error'):
        stats['errors'] += 1
    elif result.get('skipped'):
        stats['skipped'] += 1
    elif result.get('updated'):
        stats['cases_updated'] += 1
        stats['events_added'] += result.get('events_added', 0)
        stats['documents_downloaded'] += result.get('documents', 0)
    else:
        stats['skipped'] += 1


def main():
    """Главная функция"""
    # Инициализация всех логгеров ОДИН раз при старте
    init_logging(log_dir="logs", level="DEBUG")
    logger = get_logger('main')
    
    logger.info("=" * 70)
    logger.info("ПАРСЕР СУДЕБНЫХ ДЕЛ КАЗАХСТАНА v2.1")
    logger.info("=" * 70)
    
    mode = 'parse'
    
    if '--mode' in sys.argv:
        idx = sys.argv.index('--mode')
        if idx + 1 < len(sys.argv):
            mode = sys.argv[idx + 1]
    
    try:
        if mode == 'parse':
            asyncio.run(parse_all_regions_from_config())
        
        elif mode == 'update':
            asyncio.run(update_cases_history())
        
        else:
            logger.error(f"Неизвестный режим: {mode}")
            sys.exit(1)
            
    except KeyboardInterrupt:
        logger.warning("Прервано пользователем")
        sys.exit(0)
    except Exception as e:
        logger.critical(f"Критическая ошибка: {e}")
        logger.debug(traceback.format_exc())
        sys.exit(1)


if __name__ == '__main__':
    main()