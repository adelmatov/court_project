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
    court_type = ps.get('court_type', 'smas')
    start_from = ps.get('start_from', 1)
    max_number = ps.get('max_number', 9999)
    max_consecutive_failures = ps.get('max_consecutive_failures', 50)
    delay_between_requests = ps.get('delay_between_requests', 2)
    delay_between_regions = ps.get('delay_between_regions', 5)
    
    # ЛИМИТЫ ДЛЯ ТЕСТИРОВАНИЯ
    limit_regions = settings.get_limit_regions()
    limit_cases_per_region = settings.get_limit_cases_per_region()
    
    logger.info("=" * 70)
    logger.info(f"МАССОВЫЙ ПАРСИНГ: {court_type} ({year})")
    logger.info("=" * 70)
    logger.info(f"Настройки из config.json:")
    logger.info(f"  Год: {year}")
    logger.info(f"  Тип суда: {court_type}")
    logger.info(f"  Диапазон номеров: {start_from}-{max_number}")
    logger.info(f"  Макс. неудач подряд: {max_consecutive_failures}")
    logger.info(f"  Задержка между запросами: {delay_between_requests} сек")
    logger.info(f"  Задержка между регионами: {delay_between_regions} сек")
    
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
    
    total_stats = {
        'regions_processed': 0,
        'total_queries': 0,
        'total_target_cases': 0,
        'total_related_cases': 0,
        'total_cases_saved': 0
    }
    
    # ИЗМЕНЕНО: создаём парсер БЕЗ флага (Full Scan Mode)
    async with CourtParser() as parser:  # ← update_mode=False (default)
        for region_key in regions_to_process:
            logger.info(f"\n{'='*70}")
            logger.info(f"Регион: {settings.get_region(region_key)['name']}")
            logger.info(f"{'='*70}")
            
            try:
                # Парсинг региона
                stats = await parse_region_with_limits(
                    parser=parser,
                    region_key=region_key,
                    court_key=court_type,
                    year=year,
                    start_from=start_from,
                    max_number=max_number,
                    max_consecutive_failures=max_consecutive_failures,
                    delay_between_requests=delay_between_requests,
                    limit_cases=limit_cases_per_region
                )
                
                total_stats['regions_processed'] += 1
                total_stats['total_queries'] += stats['queries_made']
                total_stats['total_target_cases'] += stats['target_cases_found']
                total_stats['total_related_cases'] += stats['related_cases_found']
                total_stats['total_cases_saved'] += stats['total_cases_saved']
                
            except Exception as e:
                logger.error(f"Ошибка парсинга региона {region_key}: {e}")
                continue
            
            # Задержка между регионами
            if total_stats['regions_processed'] < len(regions_to_process):
                await asyncio.sleep(delay_between_regions)
    
    # Общая статистика
    logger.info("\n" + "=" * 70)
    logger.info("ОБЩАЯ СТАТИСТИКА:")
    logger.info(f"  Обработано регионов: {total_stats['regions_processed']}")
    logger.info(f"  Всего запросов к серверу: {total_stats['total_queries']}")
    logger.info(f"  Найдено целевых дел: {total_stats['total_target_cases']}")
    logger.info(f"  Найдено связанных дел: {total_stats['total_related_cases']}")
    logger.info(f"  Всего сохранено дел: {total_stats['total_cases_saved']}")
    
    if total_stats['total_queries'] > 0:
        avg_per_query = total_stats['total_cases_saved'] / total_stats['total_queries']
        logger.info(f"  Среднее дел на запрос: {avg_per_query:.1f}")
    
    logger.info("=" * 70)
    
    return total_stats


async def parse_region_with_limits(parser, region_key: str, court_key: str,
                                   year: str, start_from: int, max_number: int,
                                   max_consecutive_failures: int,
                                   delay_between_requests: float,
                                   limit_cases: Optional[int] = None) -> dict:
    """
    Парсинг региона с учетом лимита дел
    
    Args:
        limit_cases: максимальное количество дел для проверки (для тестирования)
    """
    logger = setup_logger('main', level='INFO')
    
    stats = {
        'queries_made': 0,              # Количество запросов к серверу
        'target_cases_found': 0,        # Найдено целевых дел
        'related_cases_found': 0,       # Найдено связанных дел
        'total_cases_saved': 0,         # Всего дел сохранено
        'consecutive_failures': 0
    }
    
    current_number = start_from
    
    while current_number <= max_number:
        # Проверка лимита дел
        if limit_cases and stats['queries_made'] >= limit_cases:
            logger.info(f"🔒 Достигнут лимит дел ({limit_cases}), завершаю регион")
            break
        
        # Проверка лимита неудач
        if stats['consecutive_failures'] >= max_consecutive_failures:
            logger.info(f"Достигнут лимит неудач ({max_consecutive_failures}), завершаю регион")
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
    
    # Итоговая статистика региона
    logger.info("-" * 70)
    logger.info(f"ИТОГИ РЕГИОНА:")
    logger.info(f"  Запросов к серверу: {stats['queries_made']}")
    logger.info(f"  Найдено целевых дел: {stats['target_cases_found']}")
    logger.info(f"  Найдено связанных дел: {stats['related_cases_found']}")
    logger.info(f"  Всего сохранено дел: {stats['total_cases_saved']}")
    
    if stats['queries_made'] > 0:
        target_rate = (stats['target_cases_found'] / stats['queries_made'] * 100)
        total_rate = (stats['total_cases_saved'] / stats['queries_made'] * 100)
        logger.info(f"  Процент целевых дел: {target_rate:.1f}%")
        logger.info(f"  Среднее дел на запрос: {stats['total_cases_saved'] / stats['queries_made']:.1f}")
    
    logger.info("-" * 70)
    
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
    async with CourtParser(update_mode=True) as parser:  # ← ФЛАГ!
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
                
                # ИЗМЕНЕНО: вызов БЕЗ параметра update_mode (автоматически используется self.update_mode)
                result = await parser.search_and_save(
                    region_key=case_info['region_key'],
                    court_key=case_info['court_key'],
                    case_number=case_info['sequence'],
                    year=case_info['year']
                    # ← НЕТ параметра update_mode!
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