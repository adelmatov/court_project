"""
Обновление судей в делах СМАС
"""
from typing import Dict, List, Any

from core.updaters.base_updater import BaseUpdater
from core.region_worker import RegionWorker
from utils.terminal_ui import Mode


class JudgeUpdater(BaseUpdater):
    """Updater для обновления информации о судьях"""
    
    MODE = Mode.JUDGE

    def get_config(self) -> Dict[str, Any]:
        return self.settings.config.get('update_settings', {}).get('judge', {})
    
    async def get_cases_to_process(self) -> List[str]:
        config = self.get_config()
        
        if not config.get('enabled', True):
            self.logger.info("JudgeUpdater отключен в конфиге")
            return []
        
        interval_days = config.get('check_interval_days', 1)
        
        cases = await self.db_manager.get_smas_cases_without_judge(
            self.settings,
            interval_days=interval_days
        )
        
        self.logger.info(f"Дел без судьи: {len(cases)}")
        return cases
    
    async def process_case(self, worker: RegionWorker, case_number: str) -> Dict[str, Any]:
        result = {
            'case_number': case_number,
            'success': False,
            'judge_found': False,
        }
        
        try:
            _, cases = await worker.search_case_by_number(case_number)
            
            if not cases:
                return result
            
            target = next(
                (c for c in cases 
                 if self.text_processor.is_matching_case_number(c.case_number, case_number)),
                None
            )
            
            if not target:
                return result
            
            if target.judge:
                await self.db_manager.update_case(target)
                result['judge_found'] = True
                self.logger.info(f"Судья найден: {case_number} → {target.judge}")
            
            await self.db_manager.mark_case_as_updated(case_number)
            result['success'] = True
        
        except Exception as e:
            self.logger.error(f"Ошибка: {case_number}: {e}")
            result['error'] = str(e)
        
        return result