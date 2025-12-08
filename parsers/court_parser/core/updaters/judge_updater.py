"""
Обновление судей в делах СМАС
"""
from typing import Dict, List, Any

from core.updaters.base_updater import BaseUpdater
from core.region_worker import RegionWorker


class JudgeUpdater(BaseUpdater):
    """
    Updater для обновления информации о судьях
    
    Команда: --mode update judge
    
    Логика:
    1. Находит дела СМАС без судьи
    2. Ищет каждое дело на сайте
    3. Если судья появился — обновляет в БД
    """
    MODE = 'judge'

    def get_config(self) -> Dict[str, Any]:
        """Получить конфигурацию для judge updater"""
        return self.settings.config.get('update_settings', {}).get('judge', {})
    
    async def get_cases_to_process(self) -> List[str]:
        """Получить дела СМАС без судьи"""
        config = self.get_config()
        
        if not config.get('enabled', True):
            self.logger.info("JudgeUpdater отключен в конфиге")
            return []
        
        interval_days = config.get('check_interval_days', 1)
        target_courts = config.get('target_courts', ['smas'])
        
        # Получаем дела без судьи для указанных типов судов
        cases = await self.db_manager.get_smas_cases_without_judge(
            self.settings,
            interval_days=interval_days
        )
        
        self.logger.info(
            f"Найдено дел без судьи: {len(cases)} "
            f"(интервал {interval_days} дней, суды: {target_courts})"
        )
        
        return cases
    
    async def process_case(self, worker: RegionWorker, case_number: str) -> Dict[str, Any]:
        """Обработать одно дело — найти и обновить судью"""
        result = {
            'case_number': case_number,
            'success': False,
            'judge_found': False,
            'judge_name': None
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
            
            # Проверяем наличие судьи
            if target.judge:
                # Обновляем судью в БД
                await self.db_manager.update_case(target)
                await self.db_manager.mark_case_as_updated(case_number)
                
                result['success'] = True
                result['judge_found'] = True
                result['judge_name'] = target.judge
                
                self.logger.info(f"✅ Судья найден: {case_number} → {target.judge}")
            else:
                # Судья всё ещё не назначен
                await self.db_manager.mark_case_as_updated(case_number)
                result['success'] = True
                self.logger.debug(f"Судья не назначен: {case_number}")
        
        except Exception as e:
            self.logger.error(f"Ошибка обработки {case_number}: {e}")
            result['error'] = str(e)
        
        return result