"""
Обновление событий (истории) дел
"""
from typing import Dict, List, Any

from core.updaters.base_updater import BaseUpdater
from core.region_worker import RegionWorker


class EventsUpdater(BaseUpdater):
    """
    Updater для обновления событий дел
    
    Команда: --mode update case_events
    
    Логика:
    1. Находит дела по фильтру (ключевые слова в ответчике)
    2. Исключает дела с финальными событиями
    3. Ищет каждое дело на сайте
    4. Добавляет новые события в БД
    """
    MODE = 'events'

    def get_config(self) -> Dict[str, Any]:
        """Получить конфигурацию для events updater"""
        return self.settings.config.get('update_settings', {}).get('case_events', {})
    
    async def get_cases_to_process(self) -> List[str]:
        """Получить дела для обновления событий"""
        config = self.get_config()
        
        if not config.get('enabled', True):
            self.logger.info("EventsUpdater отключен в конфиге")
            return []
        
        filters = config.get('filters', {})
        interval_days = config.get('check_interval_days', 2)
        
        cases = await self.db_manager.get_cases_for_update({
            'defendant_keywords': filters.get('party_keywords', []),
            'exclude_event_types': filters.get('exclude_event_types', []),
            'update_interval_days': interval_days
        })
        
        self.logger.info(
            f"Найдено дел для обновления событий: {len(cases)} "
            f"(фильтр: {filters.get('party_keywords', [])}, интервал {interval_days} дней)"
        )
        
        return cases
    
    async def process_case(self, worker: RegionWorker, case_number: str) -> Dict[str, Any]:
        """Обработать одно дело — найти и добавить новые события"""
        result = {
            'case_number': case_number,
            'success': False,
            'events_added': 0
        }
        
        try:
            # Поиск дела на сайте
            _, cases = await worker.search_case_by_number(case_number)
            
            if not cases:
                self.logger.debug(f"Дело не найдено на сайте: {case_number}")
                return result
            
            # Ищем целевое дело
            target = next(
                (c for c in cases 
                 if self.text_processor.is_matching_case_number(c.case_number, case_number)),
                None
            )
            
            if not target:
                self.logger.debug(f"Целевое дело не найдено: {case_number}")
                return result
            
            # Обновляем дело (события + судья если появился)
            update_result = await self.db_manager.update_case(target)
            await self.db_manager.mark_case_as_updated(case_number)
            
            result['success'] = True
            result['events_added'] = update_result.get('events_added', 0)
            
            if result['events_added'] > 0:
                self.logger.info(
                    f"✅ Добавлено событий: {result['events_added']} для {case_number}"
                )
            else:
                self.logger.debug(f"Новых событий нет: {case_number}")
        
        except Exception as e:
            self.logger.error(f"Ошибка обработки {case_number}: {e}")
            result['error'] = str(e)
        
        return result