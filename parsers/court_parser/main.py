"""
Точка входа парсера
"""
import asyncio
import sys
from typing import Optional, List
import traceback

from core.parser import CourtParser
from config.settings import Settings
from utils.logger import setup_logger
from utils.text_processor import TextProcessor


async def parse_all_regions_from_config() -> dict:
    """Парсинг всех регионов согласно настройкам из config.json"""
    logger = setup_logger('main', level='INFO')
    
    # Загрузка настроек
    settings = Settings()
    ps = settings.parsing_settings
    
    year = ps.get('year', '2025')
    court_types = ps.get('court_types', ['smas'])
    start_from = ps.get('start_from', 1)
    max_number = ps.get('max_number', 9999)
    max_consecutive_empty = ps.get('max_consecutive_empty', 200)
    delay_between_requests = ps.get('delay_between_requests', 2)
    max_parallel_regions = ps.get('max_parallel_regions', 1)
    
    # Настройки retry на уровне региона
    region_retry_max_attempts = ps.get('region_retry_max_attempts', 3)
    region_retry_delay = ps.get('region_retry_delay_seconds', 5)
    
    # ЛИМИТЫ ДЛЯ ТЕСТИРОВАНИЯ
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
    
    # Получение списка регионов
    all_regions = settings.get_target_regions()
    
    # Применение лимита регионов
    if limit_regions:
        regions_to_process = all_regions[:limit_regions]
        logger.info(f"Обрабатываю {len(regions_to_process)} из {len(all_regions)} регионов")
    else:
        regions_to_process = all_regions
        logger.info(f"Обрабатываю все {len(regions_to_process)} регионов")
    
    # Общая статистика
    total_stats = {
        'regions_processed': 0,
        'regions_failed': 0,
        'total_queries': 0,
        'total_cases_saved': 0
    }
    stats_lock = asyncio.Lock()
    
    # Семафор для контроля параллельности
    semaphore = asyncio.Semaphore(max_parallel_regions)
    
    # Создаём парсер один раз
    async with CourtParser() as parser:
        
        async def process_region_with_retry(region_key: str):
            """Обработка региона с retry"""
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
                        
                        # Парсинг всех судов региона
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
                        
                        # Успех → обновляем статистику
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
        
        # Запускаем все регионы
        tasks = [process_region_with_retry(r) for r in regions_to_process]
        await asyncio.gather(*tasks, return_exceptions=True)
    
    # Итоговая статистика
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
    
    # Итоги региона
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
    """
    Парсинг одного суда
    
    Логика:
    1. Заполнение пропусков в уже пройденном диапазоне
    2. Сбор новых дел от последнего номера
    """
    logger = setup_logger('main', level='INFO')
    court_config = settings.get_court(region_key, court_key)
    
    stats = {
        'missing_found': 0,
        'missing_filled': 0,
        'missing_not_found': 0,
        'new_queries': 0,
        'new_saved': 0,
        'consecutive_empty': 0
    }
    
    # ════════════════════════════════════════════════════════════════════
    # АНАЛИЗ БД
    # ════════════════════════════════════════════════════════════════════
    
    existing = await parser.db_manager.get_existing_case_numbers(
        region_key, court_key, year, settings
    )
    
    last_in_db = await parser.db_manager.get_last_sequence_number(
        region_key, court_key, year, settings
    )
    
    logger.info(f"📊 Анализ БД:")
    logger.info(f"   Существующих номеров: {len(existing)}")
    logger.info(f"   Последний номер: {last_in_db}")
    
    # ════════════════════════════════════════════════════════════════════
    # ШАГ 1: Заполнение пропусков
    # ════════════════════════════════════════════════════════════════════
    
    if last_in_db > 0:
        # Вычисляем пропущенные номера
        full_range = set(range(start_from, last_in_db + 1))
        missing = sorted(full_range - existing)
        stats['missing_found'] = len(missing)
        
        if missing:
            logger.info(f"\n{'─' * 70}")
            logger.info(f"ШАГ 1: Заполнение пропусков")
            logger.info(f"{'─' * 70}")
            logger.info(f"📋 Пропущено номеров: {len(missing)}")
            
            if len(missing) <= 20:
                logger.info(f"   Номера: {missing}")
            else:
                logger.info(f"   Первые 10: {missing[:10]}")
                logger.info(f"   Последние 10: {missing[-10:]}")
            
            for i, seq_num in enumerate(missing, 1):
                # Проверка лимита
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
                    status = "✅"
                else:
                    stats['missing_not_found'] += 1
                    status = "❌"
                
                # Прогресс каждые 10 или в конце
                if i % 10 == 0 or i == len(missing):
                    logger.info(
                        f"   [{i}/{len(missing)}] "
                        f"Заполнено: {stats['missing_filled']}, "
                        f"Не найдено: {stats['missing_not_found']}"
                    )
                
                await asyncio.sleep(delay_between_requests)
            
            logger.info(f"\n   Итого пропусков: заполнено {stats['missing_filled']}, "
                       f"не найдено {stats['missing_not_found']}")
        else:
            logger.info(f"   Пропусков нет ✓")
    
    # ════════════════════════════════════════════════════════════════════
    # ШАГ 2: Сбор новых дел
    # ════════════════════════════════════════════════════════════════════
    
    actual_start = last_in_db + 1 if last_in_db > 0 else start_from
    
    if actual_start > max_number:
        logger.info(f"✅ Все номера до {max_number} уже обработаны")
    else:
        logger.info(f"\n{'─' * 70}")
        logger.info(f"ШАГ 2: Сбор новых дел")
        logger.info(f"{'─' * 70}")
        logger.info(f"▶️  Старт с номера: {actual_start}")
        
        current_number = actual_start
        
        while current_number <= max_number:
            # Проверка лимита
            if limit_cases:
                total_queries = (stats['missing_filled'] + stats['missing_not_found'] + 
                               stats['new_queries'])
                if total_queries >= limit_cases:
                    logger.info(f"🔒 Лимит запросов ({limit_cases})")
                    break
            
            # Проверка consecutive_empty
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
            
            # Прогресс каждые 10 запросов
            if stats['new_queries'] % 10 == 0:
                logger.info(
                    f"   #{current_number} | "
                    f"Запросов: {stats['new_queries']} | "
                    f"Сохранено: {stats['new_saved']} | "
                    f"Пустых подряд: {stats['consecutive_empty']}"
                )
            
            current_number += 1
            await asyncio.sleep(delay_between_requests)
    
    # ════════════════════════════════════════════════════════════════════
    # ИТОГИ
    # ════════════════════════════════════════════════════════════════════
    
    total_saved = stats['missing_filled'] + stats['new_saved']
    total_queries = (stats['missing_filled'] + stats['missing_not_found'] + 
                    stats['new_queries'])
    
    logger.info(f"\n{'═' * 70}")
    logger.info(f"ИТОГИ {court_config['name']}:")
    logger.info(f"  Пропусков найдено: {stats['missing_found']}")
    logger.info(f"  Пропусков заполнено: {stats['missing_filled']}")
    logger.info(f"  Новых дел собрано: {stats['new_saved']}")
    logger.info(f"  Всего сохранено: {total_saved}")
    logger.info(f"  Всего запросов: {total_queries}")
    logger.info(f"{'═' * 70}")
    
    # Возвращаем статистику в формате, совместимом с вызывающим кодом
    return {
        'queries_made': total_queries,
        'cases_saved': total_saved,
        'consecutive_empty': stats['consecutive_empty'],
        'missing_found': stats['missing_found'],
        'missing_filled': stats['missing_filled']
    }


async def update_cases_history():
    """
    Режим обновления истории дел
    
    Этап 1: Дела СМАС без судьи (приоритет)
    Этап 2: Дела по ключевым словам
    """
    logger = setup_logger('main', level='INFO')
    
    settings = Settings()
    update_config = settings.update_settings
    
    if not update_config.get('enabled'):
        logger.warning("⚠️ Update Mode отключен")
        return
    
    interval_days = update_config.get('update_interval_days', 2)
    
    logger.info("\n" + "=" * 70)
    logger.info("РЕЖИМ ОБНОВЛЕНИЯ")
    logger.info("=" * 70)
    
    stats = {
        'stage1_checked': 0,
        'stage1_updated': 0,
        'stage1_errors': 0,
        'stage2_checked': 0,
        'stage2_updated': 0,
        'stage2_errors': 0
    }
    
    async with CourtParser() as parser:
        text_processor = TextProcessor()
        
        # ════════════════════════════════════════════════════════════════
        # ЭТАП 1: Дела СМАС без судьи
        # ════════════════════════════════════════════════════════════════
        
        logger.info("\n" + "-" * 70)
        logger.info("ЭТАП 1: Дела СМАС без судьи")
        logger.info("-" * 70)
        
        smas_cases = await parser.db_manager.get_smas_cases_without_judge(
            settings=settings,
            interval_days=interval_days
        )
        
        if smas_cases:
            logger.info(f"📋 Дел СМАС без судьи: {len(smas_cases)}")
            
            for i, case_number in enumerate(smas_cases, 1):
                try:
                    case_info = text_processor.find_region_and_court_by_case_number(
                        case_number, settings.regions
                    )
                    
                    if not case_info:
                        logger.warning(f"⚠️ Не удалось определить регион: {case_number}")
                        stats['stage1_errors'] += 1
                        continue
                    
                    logger.info(f"[{i}/{len(smas_cases)}] {case_number}")
                    
                    result = await parser.search_and_save(
                        region_key=case_info['region_key'],
                        court_key=case_info['court_key'],
                        sequence_number=int(case_info['sequence']),
                        year=case_info['year']
                    )
                    
                    stats['stage1_checked'] += 1
                    
                    if result['success']:
                        await parser.db_manager.mark_case_as_updated(case_number)
                        if result.get('saved'):
                            stats['stage1_updated'] += 1
                            logger.info(f"   ✅ Судья обновлён")
                    else:
                        stats['stage1_errors'] += 1
                        logger.warning(f"   ⚠️ {result.get('error', 'unknown')}")
                    
                    await asyncio.sleep(2)
                    
                except Exception as e:
                    stats['stage1_errors'] += 1
                    logger.error(f"❌ {case_number}: {e}")
        else:
            logger.info("✅ Нет дел СМАС без судьи")
        
        logger.info(f"\nИтоги этапа 1: проверено {stats['stage1_checked']}, "
                   f"обновлено {stats['stage1_updated']}, ошибок {stats['stage1_errors']}")
        
        # ════════════════════════════════════════════════════════════════
        # ЭТАП 2: Дела по ключевым словам
        # ════════════════════════════════════════════════════════════════
        
        logger.info("\n" + "-" * 70)
        logger.info("ЭТАП 2: Дела по ключевым словам")
        logger.info("-" * 70)
        
        keyword_cases = await parser.db_manager.get_cases_for_update({
            'defendant_keywords': update_config['filters']['defendant_keywords'],
            'exclude_event_types': update_config['filters']['exclude_event_types'],
            'update_interval_days': interval_days
        })
        
        if keyword_cases:
            logger.info(f"📋 Дел по ключевым словам: {len(keyword_cases)}")
            
            for i, case_number in enumerate(keyword_cases, 1):
                try:
                    case_info = text_processor.find_region_and_court_by_case_number(
                        case_number, settings.regions
                    )
                    
                    if not case_info:
                        logger.warning(f"⚠️ Не удалось определить регион: {case_number}")
                        stats['stage2_errors'] += 1
                        continue
                    
                    logger.info(f"[{i}/{len(keyword_cases)}] {case_number}")
                    
                    result = await parser.search_and_save(
                        region_key=case_info['region_key'],
                        court_key=case_info['court_key'],
                        sequence_number=int(case_info['sequence']),
                        year=case_info['year']
                    )
                    
                    stats['stage2_checked'] += 1
                    
                    if result['success']:
                        await parser.db_manager.mark_case_as_updated(case_number)
                        if result.get('saved'):
                            stats['stage2_updated'] += 1
                    else:
                        stats['stage2_errors'] += 1
                    
                    await asyncio.sleep(2)
                    
                except Exception as e:
                    stats['stage2_errors'] += 1
                    logger.error(f"❌ {case_number}: {e}")
        else:
            logger.info("✅ Нет дел для обновления по ключевым словам")
        
        logger.info(f"\nИтоги этапа 2: проверено {stats['stage2_checked']}, "
                   f"обновлено {stats['stage2_updated']}, ошибок {stats['stage2_errors']}")
    
    # ════════════════════════════════════════════════════════════════════
    # ОБЩИЕ ИТОГИ
    # ════════════════════════════════════════════════════════════════════
    
    total_checked = stats['stage1_checked'] + stats['stage2_checked']
    total_updated = stats['stage1_updated'] + stats['stage2_updated']
    total_errors = stats['stage1_errors'] + stats['stage2_errors']
    
    logger.info("\n" + "=" * 70)
    logger.info("ОБЩИЕ ИТОГИ UPDATE MODE:")
    logger.info(f"  Этап 1 (СМАС без судьи): {stats['stage1_checked']} проверено, "
               f"{stats['stage1_updated']} обновлено")
    logger.info(f"  Этап 2 (ключевые слова): {stats['stage2_checked']} проверено, "
               f"{stats['stage2_updated']} обновлено")
    logger.info(f"  ВСЕГО: {total_checked} проверено, {total_updated} обновлено, "
               f"{total_errors} ошибок")
    logger.info("=" * 70)


async def main():
    """Главная функция"""
    logger = setup_logger('main', level='INFO')
    
    logger.info("\n" + "=" * 70)
    logger.info("ПАРСЕР СУДЕБНЫХ ДЕЛ КАЗАХСТАНА v2.0")
    logger.info("=" * 70)
    
    try:
        if '--mode' in sys.argv:
            idx = sys.argv.index('--mode')
            if idx + 1 < len(sys.argv) and sys.argv[idx + 1] == 'update':
                await update_cases_history()
                return 0
        
        await parse_all_regions_from_config()
        logger.info("\n✅ Завершено")
        return 0
    
    except KeyboardInterrupt:
        logger.warning("\n🛑 Прервано")
        return 1
    
    except Exception as e:
        logger.critical(f"\n💥 Ошибка: {e}")
        logger.critical(traceback.format_exc())
        return 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))