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
from utils import TextProcessor


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
    max_consecutive_failures = ps.get('max_consecutive_failures', 50)
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
    logger.info(f"  Макс. неудач подряд: {max_consecutive_failures}")
    logger.info(f"  Задержка между запросами: {delay_between_requests} сек")
    logger.info(f"  Параллельных регионов: {max_parallel_regions}")
    logger.info(f"  Retry на регион: {region_retry_max_attempts} попыток, задержка {region_retry_delay} сек")
    
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
    
    # Общая статистика (thread-safe)
    total_stats = {
        'regions_processed': 0,
        'regions_failed': 0,
        'total_queries': 0,
        'total_target_cases': 0,
        'total_related_cases': 0,
        'total_cases_saved': 0
    }
    stats_lock = asyncio.Lock()
    
    # Семафор для контроля параллельности
    semaphore = asyncio.Semaphore(max_parallel_regions)
    
    async def process_region_with_retry_and_semaphore(region_key: str):
        """
        Обработка региона с retry и пересозданием сессии
        
        Семафор держится на всё время retry (не занимает дополнительный слот)
        """
        async with semaphore:
            region_config = settings.get_region(region_key)
            
            for attempt in range(1, region_retry_max_attempts + 1):
                try:
                    logger.info(f"\n{'='*70}")
                    if attempt > 1:
                        logger.info(f"🔄 Регион: {region_config['name']} (повторная попытка {attempt}/{region_retry_max_attempts})")
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
                        max_consecutive_failures=max_consecutive_failures,
                        delay_between_requests=delay_between_requests,
                        limit_cases=limit_cases_per_region
                    )
                    
                    # Успех → обновляем статистику
                    async with stats_lock:
                        total_stats['regions_processed'] += 1
                        total_stats['total_queries'] += region_stats['total_queries']
                        total_stats['total_target_cases'] += region_stats['total_target_cases']
                        total_stats['total_related_cases'] += region_stats['total_related_cases']
                        total_stats['total_cases_saved'] += region_stats['total_cases_saved']
                    
                    return region_stats
                
                except Exception as e:
                    if attempt < region_retry_max_attempts:
                        logger.warning(
                            f"⚠️ Регион {region_config['name']} завершился с ошибкой "
                            f"(попытка {attempt}/{region_retry_max_attempts})"
                        )
                        logger.warning(f"   Ошибка: {e}")
                        logger.info(f"   Пересоздаю HTTP сессию и повторяю через {region_retry_delay} сек...")
                        
                        # Пересоздание сессии
                        await parser.session_manager.create_session()
                        await asyncio.sleep(region_retry_delay)
                    else:
                        # Последняя попытка failed
                        logger.error(
                            f"❌ Регион {region_config['name']} failed после "
                            f"{region_retry_max_attempts} попыток"
                        )
                        logger.error(f"   Финальная ошибка: {e}")
                        logger.error(traceback.format_exc())
                        
                        async with stats_lock:
                            total_stats['regions_failed'] += 1
                        
                        return None
    
    # Создаём парсер один раз
    async with CourtParser() as parser:
        # Создаём задачи для всех регионов
        tasks = [
            process_region_with_retry_and_semaphore(region_key)
            for region_key in regions_to_process
        ]
        
        # Запускаем все задачи (семафор ограничит параллельность)
        results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Общая статистика
    logger.info("\n" + "=" * 70)
    logger.info("ОБЩАЯ СТАТИСТИКА:")
    logger.info(f"  Обработано регионов: {total_stats['regions_processed']}")
    if total_stats['regions_failed'] > 0:
        logger.info(f"  Регионов с ошибками: {total_stats['regions_failed']}")
    logger.info(f"  Всего запросов к серверу: {total_stats['total_queries']}")
    logger.info(f"  Найдено целевых дел: {total_stats['total_target_cases']}")
    logger.info(f"  Найдено связанных дел: {total_stats['total_related_cases']}")
    logger.info(f"  Всего сохранено дел: {total_stats['total_cases_saved']}")
    
    if total_stats['total_queries'] > 0:
        avg_per_query = total_stats['total_cases_saved'] / total_stats['total_queries']
        logger.info(f"  Среднее дел на запрос: {avg_per_query:.1f}")
    
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
    max_consecutive_failures: int,
    delay_between_requests: float,
    limit_cases: Optional[int] = None
) -> dict:
    """
    Обработка всех судов региона ПОСЛЕДОВАТЕЛЬНО
    
    Args:
        parser: экземпляр CourtParser
        settings: экземпляр Settings
        region_key: ключ региона ('astana', 'almaty', ...)
        court_types: список типов судов (['smas', 'appellate'])
        year: год ('2025')
        start_from: начальный номер дела (1)
        max_number: конечный номер дела (9999)
        max_consecutive_failures: лимит неудач подряд (50)
        delay_between_requests: задержка между запросами (2.0)
        limit_cases: лимит дел для тестирования (None = без лимита)
    
    Returns:
        {
            'region_key': 'astana',
            'courts_processed': 2,
            'total_queries': 100,
            'total_target_cases': 10,
            'total_related_cases': 90,
            'total_cases_saved': 100,
            'courts_stats': {
                'smas': {...},
                'appellate': {...}
            }
        }
    """
    logger = setup_logger('main', level='INFO')
    region_config = settings.get_region(region_key)
    
    region_stats = {
        'region_key': region_key,
        'courts_processed': 0,
        'total_queries': 0,
        'total_target_cases': 0,
        'total_related_cases': 0,
        'total_cases_saved': 0,
        'courts_stats': {}
    }
    
    # ПОСЛЕДОВАТЕЛЬНАЯ обработка судов
    for court_key in court_types:
        court_config = region_config['courts'][court_key]
        logger.info(f"\n📍 Суд: {court_config['name']}")
        
        try:
            # Парсинг одного суда
            court_stats = await parse_court(
                parser=parser,
                settings=settings,
                region_key=region_key,
                court_key=court_key,
                year=year,
                start_from=start_from,
                max_number=max_number,
                max_consecutive_failures=max_consecutive_failures,
                delay_between_requests=delay_between_requests,
                limit_cases=limit_cases
            )
            
            # Обновление статистики региона
            region_stats['courts_processed'] += 1
            region_stats['total_queries'] += court_stats['queries_made']
            region_stats['total_target_cases'] += court_stats['target_cases_found']
            region_stats['total_related_cases'] += court_stats['related_cases_found']
            region_stats['total_cases_saved'] += court_stats['total_cases_saved']
            region_stats['courts_stats'][court_key] = court_stats
            
        except Exception as e:
            logger.error(f"❌ Ошибка парсинга суда {court_key}: {e}")
            logger.error(traceback.format_exc())
            continue
    
    # Итоги региона
    logger.info(f"\n{'-'*70}")
    logger.info(f"ИТОГИ РЕГИОНА {region_config['name']}:")
    logger.info(f"  Обработано судов: {region_stats['courts_processed']}/{len(court_types)}")
    logger.info(f"  Запросов к серверу: {region_stats['total_queries']}")
    logger.info(f"  Найдено целевых дел: {region_stats['total_target_cases']}")
    logger.info(f"  Найдено связанных дел: {region_stats['total_related_cases']}")
    logger.info(f"  Всего сохранено дел: {region_stats['total_cases_saved']}")
    
    if region_stats['total_queries'] > 0:
        target_rate = (region_stats['total_target_cases'] / region_stats['total_queries'] * 100)
        logger.info(f"  Процент целевых дел: {target_rate:.1f}%")
    
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
    max_consecutive_failures: int,
    delay_between_requests: float,
    limit_cases: Optional[int] = None
) -> dict:
    """
    Парсинг одного суда (последовательно по делам)
    
    Args:
        parser: экземпляр CourtParser
        settings: экземпляр Settings
        region_key: ключ региона ('astana')
        court_key: ключ суда ('smas')
        year: год ('2025')
        start_from: начальный номер дела (1)
        max_number: конечный номер дела (9999)
        max_consecutive_failures: лимит неудач подряд (50)
        delay_between_requests: задержка между запросами (2.0)
        limit_cases: лимит дел для тестирования (None = без лимита)
    
    Returns:
        {
            'queries_made': 100,
            'target_cases_found': 10,
            'related_cases_found': 90,
            'total_cases_saved': 100,
            'consecutive_failures': 0
        }
    """
    logger = setup_logger('main', level='INFO')
    
    stats = {
        'queries_made': 0,
        'target_cases_found': 0,
        'related_cases_found': 0,
        'total_cases_saved': 0,
        'consecutive_failures': 0
    }
    
    current_number = start_from
    
    while current_number <= max_number:
        # Проверка лимита дел
        if limit_cases and stats['queries_made'] >= limit_cases:
            logger.info(f"🔒 Достигнут лимит дел ({limit_cases}), завершаю суд")
            break
        
        # Проверка лимита неудач
        if stats['consecutive_failures'] >= max_consecutive_failures:
            logger.info(f"Достигнут лимит неудач ({max_consecutive_failures}), завершаю суд")
            break
        
        # Поиск дела
        result = await parser.search_and_save(
            region_key=region_key,
            court_key=court_key,
            case_number=str(current_number),
            year=year
        )
        
        stats['queries_made'] += 1
        
        if result['success']:
            # Успех
            stats['total_cases_saved'] += result['total_saved']
            
            if result['target_found']:
                stats['target_cases_found'] += 1
            
            stats['related_cases_found'] += result['related_saved']
            stats['consecutive_failures'] = 0
        else:
            # Неудача
            stats['consecutive_failures'] += 1
        
        # Периодическая статистика
        if stats['queries_made'] % 10 == 0:
            logger.info(
                f"📊 Прогресс: запросов {stats['queries_made']}, "
                f"найдено целевых {stats['target_cases_found']}, "
                f"всего сохранено {stats['total_cases_saved']}"
            )
        
        current_number += 1
        
        # Задержка между запросами
        await asyncio.sleep(delay_between_requests)
    
    return stats


async def update_cases_history():
    """
    Режим обновления истории дел
    """
    logger = setup_logger('main', level='INFO')
    
    # Загрузка настроек
    settings = Settings()
    update_config = settings.update_settings
    
    if not update_config.get('enabled'):
        logger.warning("⚠️ Update Mode отключен в config.json")
        return
    
    logger.info("\n" + "=" * 70)
    logger.info("РЕЖИМ ОБНОВЛЕНИЯ ИСТОРИИ ДЕЛ")
    logger.info("=" * 70)
    logger.info(f"Интервал обновления: {update_config['update_interval_days']} дней")
    logger.info(f"Фильтр по ответчику: {update_config['filters']['defendant_keywords']}")
    logger.info(f"Исключить события: {update_config['filters']['exclude_event_types']}")
    logger.info("=" * 70)
    
    stats = {
        'checked': 0,
        'updated': 0,
        'no_changes': 0,
        'errors': 0
    }
    
    # ИЗМЕНЕНО: создаём парсер С ФЛАГОМ Update Mode
    async with CourtParser(update_mode=True) as parser:
        # Получение списка дел для обновления
        cases_to_update = await parser.db_manager.get_cases_for_update({
            'defendant_keywords': update_config['filters']['defendant_keywords'],
            'exclude_event_types': update_config['filters']['exclude_event_types'],
            'update_interval_days': update_config['update_interval_days']
        })
        
        if not cases_to_update:
            logger.info("✅ Нет дел для обновления")
            return
        
        logger.info(f"\n📋 Найдено дел для обновления: {len(cases_to_update)}")
        logger.info(f"Начинаю проверку...\n")
        
        text_processor = TextProcessor()
        
        for i, case_number in enumerate(cases_to_update, 1):
            try:
                # Распарсить полный номер дела
                case_info = text_processor.find_region_and_court_by_case_number(
                    case_number, 
                    settings.regions
                )
                
                if not case_info:
                    logger.error(f"❌ Не удалось распарсить номер: {case_number}")
                    stats['errors'] += 1
                    continue
                
                # Вызов БЕЗ параметра update_mode (автоматически используется self.update_mode)
                result = await parser.search_and_save(
                    region_key=case_info['region_key'],
                    court_key=case_info['court_key'],
                    case_number=case_info['sequence'],
                    year=case_info['year']
                )
                
                stats['checked'] += 1
                
                # КРИТИЧЕСКАЯ ЛОГИКА
                if result['success']:
                    # УСПЕХ: пометить как обновлённое
                    await parser.db_manager.mark_case_as_updated(case_number)
                    
                    if result['total_saved'] > 0:
                        stats['updated'] += 1
                        logger.info(f"✅ [{i}/{len(cases_to_update)}] {case_number}: +{result['total_saved']} событий")
                    else:
                        stats['no_changes'] += 1
                        logger.debug(f"⚪ [{i}/{len(cases_to_update)}] {case_number}: без изменений")
                else:
                    # НЕУДАЧА: НЕ помечать (last_updated_at остаётся старым)
                    stats['errors'] += 1
                    logger.warning(f"⚠️ [{i}/{len(cases_to_update)}] {case_number}: ошибка")
                
                # Периодическая статистика
                if stats['checked'] % 10 == 0:
                    logger.info(
                        f"\n📊 Прогресс: {stats['checked']}/{len(cases_to_update)} "
                        f"(обновлено: {stats['updated']}, без изменений: {stats['no_changes']}, ошибок: {stats['errors']})\n"
                    )
                
                # Задержка между запросами
                await asyncio.sleep(2)
                
            except Exception as e:
                # ИСКЛЮЧЕНИЕ: НЕ помечать
                stats['errors'] += 1
                logger.error(f"❌ [{i}/{len(cases_to_update)}] Ошибка обновления {case_number}: {e}")
                continue
    
    # Итоговая статистика
    logger.info("\n" + "=" * 70)
    logger.info("ИТОГИ ОБНОВЛЕНИЯ:")
    logger.info(f"  Проверено дел: {stats['checked']}")
    logger.info(f"  Обновлено (новые события): {stats['updated']}")
    logger.info(f"  Без изменений: {stats['no_changes']}")
    logger.info(f"  Ошибок: {stats['errors']}")
    
    if stats['errors'] > 0:
        logger.warning(
            f"\n⚠️ {stats['errors']} дел не обновились и будут проверены при следующем запуске"
        )
    
    logger.info("=" * 70)


async def main():
    """
    Главная функция - запуск парсинга согласно config.json
    """
    logger = setup_logger('main', level='INFO')
    
    logger.info("\n" + "=" * 70)
    logger.info("ПАРСЕР СУДЕБНЫХ ДЕЛ КАЗАХСТАНА")
    logger.info("=" * 70)
    logger.info("Версия: 2.0.0")
    logger.info("Режим: Боевой (настройки из config.json)")
    logger.info("=" * 70)
    
    try:
        # Проверка режима запуска
        if '--mode' in sys.argv:
            mode_index = sys.argv.index('--mode')
            if mode_index + 1 < len(sys.argv):
                mode = sys.argv[mode_index + 1]
                
                if mode == 'update':
                    # РЕЖИМ ОБНОВЛЕНИЯ
                    await update_cases_history()
                    logger.info("\n✅ Обновление завершено")
                    return 0
                else:
                    logger.error(f"❌ Неизвестный режим: {mode}")
                    logger.info("Доступные режимы: update")
                    return 1
        
        # По умолчанию: Full Scan Mode
        stats = await parse_all_regions_from_config()
        
        logger.info("\n✅ Парсер завершил работу успешно")
        return 0
    
    except KeyboardInterrupt:
        logger.warning("\n🛑 Прервано пользователем")
        return 1
    
    except Exception as e:
        logger.critical(f"\n💥 Критическая ошибка: {e}")
        logger.critical(traceback.format_exc())
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
