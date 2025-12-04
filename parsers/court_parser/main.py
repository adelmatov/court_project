"""
Точка входа парсера
"""
import sys
import asyncio
import traceback
from typing import List, Optional

from core.parser import CourtParser
from config.settings import Settings
from search.document_handler import DocumentHandler
from utils.logger import setup_logger


async def parse_all_regions_from_config() -> dict:
    """Парсинг всех регионов согласно настройкам из config.json"""
    logger = setup_logger('main', level='INFO')
    
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
    
    logger.info("=" * 70)
    logger.info(f"МАССОВЫЙ ПАРСИНГ: {', '.join(court_types)} ({year})")
    logger.info("=" * 70)
    logger.info(f"Настройки из config.json:")
    logger.info(f"  Год: {year}")
    logger.info(f"  Типы судов: {', '.join(court_types)}")
    logger.info(f"  Диапазон номеров: {start_from}-{max_number}")
    logger.info(f"  Лимит пустых подряд: {max_consecutive_empty}")
    logger.info(f"  Задержка между запросами: {delay_between_requests} сек")
    logger.info(f"  Параллельных регионов: {max_parallel_regions}")
    logger.info(f"  Retry на регион: {region_retry_max_attempts} попыток")
    
    if limit_regions:
        logger.info(f"  🔒 ЛИМИТ РЕГИОНОВ: {limit_regions}")
    if limit_cases_per_region:
        logger.info(f"  🔒 ЛИМИТ ЗАПРОСОВ НА РЕГИОН: {limit_cases_per_region}")
    
    logger.info("=" * 70)
    
    all_regions = settings.get_target_regions()
    
    if limit_regions:
        regions_to_process = all_regions[:limit_regions]
        logger.info(f"Обрабатываю {len(regions_to_process)} из {len(all_regions)} регионов")
    else:
        regions_to_process = all_regions
        logger.info(f"Обрабатываю все {len(regions_to_process)} регионов")
    
    total_stats = {
        'regions_processed': 0,
        'regions_failed': 0,
        'total_queries': 0,
        'total_cases_saved': 0
    }
    stats_lock = asyncio.Lock()
    
    semaphore = asyncio.Semaphore(max_parallel_regions)
    
    async with CourtParser() as parser:
        
        async def process_region_with_retry(region_key: str):
            async with semaphore:
                region_config = settings.get_region(region_key)
                
                for attempt in range(1, region_retry_max_attempts + 1):
                    try:
                        logger.info(f"\n{'='*70}")
                        if attempt > 1:
                            logger.info(f"🔄 Регион: {region_config['name']} (попытка {attempt}/{region_retry_max_attempts})")
                        else:
                            logger.info(f"Регион: {region_config['name']}")
                        logger.info(f"{'='*70}")
                        
                        region_stats = await process_region_all_courts(
                            parser=parser,
                            settings=settings,
                            region_key=region_key,
                            court_types=court_types,
                            year=year,
                            start_from=start_from,
                            max_number=max_number,
                            max_consecutive_empty=max_consecutive_empty,
                            delay_between_requests=delay_between_requests,
                            limit_cases=limit_cases_per_region
                        )
                        
                        async with stats_lock:
                            total_stats['regions_processed'] += 1
                            total_stats['total_queries'] += region_stats['total_queries']
                            total_stats['total_cases_saved'] += region_stats['total_cases_saved']
                        
                        return region_stats
                    
                    except Exception as e:
                        if attempt < region_retry_max_attempts:
                            logger.warning(f"⚠️ Регион {region_config['name']}: ошибка (попытка {attempt})")
                            logger.warning(f"   {e}")
                            await parser.session_manager.create_session()
                            await asyncio.sleep(region_retry_delay)
                        else:
                            logger.error(f"❌ Регион {region_config['name']} failed")
                            logger.error(traceback.format_exc())
                            async with stats_lock:
                                total_stats['regions_failed'] += 1
                            return None
        
        tasks = [process_region_with_retry(r) for r in regions_to_process]
        await asyncio.gather(*tasks, return_exceptions=True)
    
    logger.info("\n" + "=" * 70)
    logger.info("ОБЩАЯ СТАТИСТИКА:")
    logger.info(f"  Обработано регионов: {total_stats['regions_processed']}")
    if total_stats['regions_failed'] > 0:
        logger.info(f"  Регионов с ошибками: {total_stats['regions_failed']}")
    logger.info(f"  Всего запросов: {total_stats['total_queries']}")
    logger.info(f"  Всего сохранено: {total_stats['total_cases_saved']}")
    logger.info("=" * 70)
    
    return total_stats


async def process_region_all_courts(
    parser,
    settings,
    region_key: str,
    court_types: List[str],
    year: str,
    start_from: int,
    max_number: int,
    max_consecutive_empty: int,
    delay_between_requests: float,
    limit_cases: Optional[int] = None
) -> dict:
    """Обработка всех судов региона"""
    logger = setup_logger('main', level='INFO')
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
            logger.warning(f"⚠️ Суд {court_key} не найден в регионе {region_key}")
            continue
            
        logger.info(f"\n📍 Суд: {court_config['name']}")
        
        try:
            court_stats = await parse_court(
                parser=parser,
                settings=settings,
                region_key=region_key,
                court_key=court_key,
                year=year,
                start_from=start_from,
                max_number=max_number,
                max_consecutive_empty=max_consecutive_empty,
                delay_between_requests=delay_between_requests,
                limit_cases=limit_cases
            )
            
            region_stats['courts_processed'] += 1
            region_stats['total_queries'] += court_stats['queries_made']
            region_stats['total_cases_saved'] += court_stats['cases_saved']
            region_stats['courts_stats'][court_key] = court_stats
            
        except Exception as e:
            logger.error(f"❌ Ошибка суда {court_key}: {e}")
            logger.error(traceback.format_exc())
            continue
    
    logger.info(f"\n{'-'*70}")
    logger.info(f"ИТОГИ РЕГИОНА {region_config['name']}:")
    logger.info(f"  Судов: {region_stats['courts_processed']}/{len(court_types)}")
    logger.info(f"  Запросов: {region_stats['total_queries']}")
    logger.info(f"  Сохранено: {region_stats['total_cases_saved']}")
    logger.info(f"{'-'*70}")
    
    return region_stats


async def parse_court(
    parser,
    settings,
    region_key: str,
    court_key: str,
    year: str,
    start_from: int,
    max_number: int,
    max_consecutive_empty: int,
    delay_between_requests: float,
    limit_cases: Optional[int] = None
) -> dict:
    """Парсинг одного суда"""
    logger = setup_logger('main', level='INFO')
    
    stats = {
        'missing_found': 0,
        'missing_filled': 0,
        'missing_not_found': 0,
        'new_queries': 0,
        'new_saved': 0,
        'consecutive_empty': 0
    }
    
    existing = await parser.db_manager.get_existing_case_numbers(
        region_key, court_key, year, settings
    )
    
    last_in_db = await parser.db_manager.get_last_sequence_number(
        region_key, court_key, year, settings
    )
    
    logger.info(f"📊 Анализ БД:")
    logger.info(f"   Существующих номеров: {len(existing)}")
    logger.info(f"   Последний номер: {last_in_db}")
    
    # ШАГ 1: Заполнение пропусков
    if last_in_db > 0:
        full_range = set(range(start_from, last_in_db + 1))
        missing = sorted(full_range - existing)
        stats['missing_found'] = len(missing)
        
        if missing:
            logger.info(f"\n{'─' * 70}")
            logger.info(f"ШАГ 1: Заполнение пропусков")
            logger.info(f"{'─' * 70}")
            logger.info(f"📋 Пропущено номеров: {len(missing)}")
            
            for i, seq_num in enumerate(missing, 1):
                if limit_cases:
                    total_queries = stats['missing_filled'] + stats['missing_not_found'] + stats['new_queries']
                    if total_queries >= limit_cases:
                        logger.info(f"🔒 Лимит запросов ({limit_cases})")
                        break
                
                result = await parser.search_and_save(
                    region_key=region_key,
                    court_key=court_key,
                    sequence_number=seq_num,
                    year=year
                )
                
                if result['success'] and result.get('saved'):
                    stats['missing_filled'] += 1
                else:
                    stats['missing_not_found'] += 1
                
                if i % 10 == 0 or i == len(missing):
                    logger.info(
                        f"   [{i}/{len(missing)}] "
                        f"Заполнено: {stats['missing_filled']}, "
                        f"Не найдено: {stats['missing_not_found']}"
                    )
                
                await asyncio.sleep(delay_between_requests)
    
    # ШАГ 2: Сбор новых дел
    actual_start = last_in_db + 1 if last_in_db > 0 else start_from
    
    if actual_start <= max_number:
        logger.info(f"\n{'─' * 70}")
        logger.info(f"ШАГ 2: Сбор новых дел")
        logger.info(f"{'─' * 70}")
        logger.info(f"▶️  Старт с номера: {actual_start}")
        
        current_number = actual_start
        
        while current_number <= max_number:
            if limit_cases:
                total_queries = (stats['missing_filled'] + stats['missing_not_found'] + 
                               stats['new_queries'])
                if total_queries >= limit_cases:
                    logger.info(f"🔒 Лимит запросов ({limit_cases})")
                    break
            
            if stats['consecutive_empty'] >= max_consecutive_empty:
                logger.info(f"🛑 Лимит пустых подряд ({max_consecutive_empty}), стоп")
                break
            
            result = await parser.search_and_save(
                region_key=region_key,
                court_key=court_key,
                sequence_number=current_number,
                year=year
            )
            
            stats['new_queries'] += 1
            
            if result['success'] and result.get('saved'):
                stats['new_saved'] += 1
                stats['consecutive_empty'] = 0
            elif result.get('error') == 'no_results':
                stats['consecutive_empty'] += 1
            elif result.get('error') == 'target_not_found' and court_key != 'smas':
                stats['consecutive_empty'] += 1
            
            if stats['new_queries'] % 10 == 0:
                logger.info(
                    f"   #{current_number} | "
                    f"Запросов: {stats['new_queries']} | "
                    f"Сохранено: {stats['new_saved']} | "
                    f"Пустых подряд: {stats['consecutive_empty']}"
                )
            
            current_number += 1
            await asyncio.sleep(delay_between_requests)
    
    total_saved = stats['missing_filled'] + stats['new_saved']
    total_queries = (stats['missing_filled'] + stats['missing_not_found'] + 
                    stats['new_queries'])
    
    return {
        'queries_made': total_queries,
        'cases_saved': total_saved,
        'consecutive_empty': stats['consecutive_empty'],
        'missing_filled': stats['missing_filled'],
        'new_saved': stats['new_saved']
    }


async def update_cases_history():
    """
    Режим обновления: события + документы
    """
    logger = setup_logger('main', level='INFO')
    
    settings = Settings()
    update_config = settings.config.get('update_settings', {})
    docs_config = settings.config.get('documents_settings', {})
    
    if not update_config.get('enabled', True):
        logger.warning("⚠️ Update Mode отключен в настройках")
        return
    
    interval_days = update_config.get('update_interval_days', 2)
    filters = update_config.get('filters', {})
    
    storage_dir = docs_config.get('storage_dir', './documents')
    download_delay = docs_config.get('download_delay', 2.0)
    
    logger.info("\n" + "=" * 70)
    logger.info("РЕЖИМ ОБНОВЛЕНИЯ: СОБЫТИЯ + ДОКУМЕНТЫ")
    logger.info("=" * 70)
    logger.info(f"Интервал: {interval_days} дней")
    if filters.get('defendant_keywords'):
        logger.info(f"Фильтр по ответчику: {filters['defendant_keywords']}")
    if filters.get('exclude_event_types'):
        logger.info(f"Исключить события: {filters['exclude_event_types']}")
    logger.info("=" * 70)
    
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
        
        # Этап 1: Дела СМАС без судьи
        logger.info("\n📋 Этап 1: Дела СМАС без судьи...")
        smas_cases = await parser.db_manager.get_smas_cases_without_judge(settings, interval_days)
        logger.info(f"   Найдено: {len(smas_cases)}")
        
        for case_number in smas_cases:
            result = await _process_single_case(
                parser, doc_handler, case_number, download_delay, logger
            )
            _update_stats(stats, result)
        
        # Этап 2: Дела по ключевым словам
        logger.info(f"\n📋 Этап 2: Дела по ключевым словам...")
        keyword_cases = await parser.db_manager.get_cases_for_update({
            'defendant_keywords': filters.get('defendant_keywords', []),
            'exclude_event_types': filters.get('exclude_event_types', []),
            'update_interval_days': interval_days
        })
        logger.info(f"   Найдено: {len(keyword_cases)}")
        
        for case_number in keyword_cases:
            result = await _process_single_case(
                parser, doc_handler, case_number, download_delay, logger
            )
            _update_stats(stats, result)
    
    # Итоги
    logger.info("\n" + "=" * 70)
    logger.info("ИТОГИ ОБНОВЛЕНИЯ:")
    logger.info(f"  Обновлено дел: {stats['cases_updated']}")
    logger.info(f"  Добавлено событий: {stats['events_added']}")
    logger.info(f"  Скачано документов: {stats['documents_downloaded']}")
    logger.info(f"  Пропущено: {stats['skipped']}")
    logger.info(f"  Ошибок: {stats['errors']}")
    logger.info("=" * 70)


async def _process_single_case(parser, doc_handler, case_number: str, delay: float, logger) -> dict:
    """Обработка одного дела: обновление событий + документы"""
    result = {'updated': False, 'events_added': 0, 'documents': 0, 'error': False}
    
    try:
        logger.info(f"   🔄 {case_number}")
        
        # 1. Поиск на сайте
        results_html, cases = await parser.search_case_by_number(case_number)
        
        if not results_html or not cases:
            logger.warning(f"      ⚠️ Не найдено на сайте")
            result['skipped'] = True
            return result
        
        # 2. Найти целевое дело
        target_case = next((c for c in cases if c.case_number == case_number), None)
        
        if not target_case or target_case.result_index is None:
            logger.warning(f"      ⚠️ Индекс не определён")
            result['skipped'] = True
            return result
        
        # 3. Обновление событий в БД
        save_result = await parser.db_manager.update_case(target_case)
        
        if save_result.get('events_added', 0) > 0:
            result['events_added'] = save_result['events_added']
            result['updated'] = True
            logger.info(f"      ✅ +{result['events_added']} событий")
        
        # 4. Скачивание документов
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
                logger.info(f"      📎 +{len(downloaded)} документов")
            
            await parser.db_manager.mark_documents_downloaded(case_id)
        
        await asyncio.sleep(delay)
        
    except Exception as e:
        logger.error(f"      ❌ {case_number}: {e}")
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
    logger = setup_logger('main', level='INFO')
    
    logger.info("\n" + "=" * 70)
    logger.info("ПАРСЕР СУДЕБНЫХ ДЕЛ КАЗАХСТАНА v2.1")
    logger.info("=" * 70)
    
    # Парсинг аргументов
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
            logger.error(f"❌ Неизвестный режим: {mode}")
            logger.info("Доступные режимы:")
            logger.info("  --mode parse   (по умолчанию)")
            logger.info("  --mode update  (события + документы)")
            sys.exit(1)
            
    except KeyboardInterrupt:
        logger.warning("\n⚠️ Прервано пользователем")
        sys.exit(0)
    except Exception as e:
        logger.critical(f"\n💥 Ошибка: {e}")
        logger.debug(traceback.format_exc())
        sys.exit(1)


if __name__ == '__main__':
    main()